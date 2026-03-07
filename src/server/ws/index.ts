import { WebSocket, Server } from 'ws';
import EventEmitter from 'events';
import { Cluster, GenericCloseCodes, ServerService } from '../../types';
import ms from 'ms';
import { authenticate } from '../../utils/AuthHandler';
import { randomUUID } from 'crypto';
import {
  WSServerCloseCode,
  ClientOpCodes,
  ServerOpCodes,
  parsePayload,
  assertOpCode,
  InvalidOpCodeError,
  InvalidPayloadError,
  OpCodeAssertionError,
} from './Payloads';
import type { ServerStructures, ClientStructures } from './Payloads';
const schema = require('../../config/schema.json');

// === Interfaces and Types ===

interface CCCInstance {
  startedBy: number;
  to: number | 'all';
  sendingData: string;
  returnData?: string | string[];
  id: string;
}

interface WSCluster extends WebSocket {
  lastHeartbeat: number;
  heartbeatTimeoutID?: NodeJS.Timeout;
  user: string;
  sendPayload<T extends ServerStructures>(payload: T): void;
}

// Constructs a WSCluster object from a WebSocket and a username.
function WSCluster(socket: WebSocket, user: string): WSCluster {
  let skt = Object.create(socket);

  let cluster: WSCluster = Object.assign(skt, {
    sendPayload: function (
      this: WSCluster,
      payload: ServerStructures,
      ...args: any
    ) {
      this.send(JSON.stringify(payload), ...args);
    },
    user,
  });

  return cluster;
}

// === Helper Functions ===

function genericToWSCloseCode(code: GenericCloseCodes): WSServerCloseCode {
  let closeCode: WSServerCloseCode;
  switch (code) {
    case GenericCloseCodes.ServerRestarting:
      closeCode = WSServerCloseCode.ServiceRestart;
      break;
    case GenericCloseCodes.ServerError:
      closeCode = WSServerCloseCode.ServerError;
      break;
    case GenericCloseCodes.InvalidData:
      closeCode = WSServerCloseCode.DecodeError;
      break;
    default:
      closeCode = WSServerCloseCode.UnknownError;
  }
  return closeCode;
}

// === WebSocket Server Implementation ===

class WSService extends EventEmitter implements ServerService {
  public name = 'ws';

  private server: Server;
  heartbeatTimeout: number = ms('100s');
  private sockets: Map<number, WSCluster> = new Map<number, WSCluster>();
  private clusterStats = {
    maxClusters: -1,
  };
  private cccInstances: Map<string, CCCInstance> = new Map<
    string,
    CCCInstance
  >();

  public getCluster(id: number): Cluster | null {
    const cluster = this.sockets.get(id);
    if (!cluster) return null;
    return { id, lastHeartbeat: cluster.lastHeartbeat, user: cluster.user };
  }

  public getAllClusters(): { [key: number]: Cluster } {
    let data: { [key: number]: Cluster } = {};
    this.sockets.forEach((value, key, self) => {
      data[key] = {
        id: key,
        lastHeartbeat: value.lastHeartbeat,
        user: value.user,
      };
    });
    return data;
  }

  public disconnectCluster(id: number, code: GenericCloseCodes): void {
    this.closeSocket(id, genericToWSCloseCode(code));
  }

  public serverClosing() {
    this.sockets.forEach((c, k) => {
      this.closeSocket(k, WSServerCloseCode.ServiceRestart);
    });
  }

  public dataPushed() {
    this.sockets.forEach((c) => {
      c.sendPayload<ServerStructures.DataPushed>({
        op: ServerOpCodes.DataPushed,
        d: undefined,
      });
    });
  }

  public constructor() {
    console.debug('[WSService] Initializing WebSocket service...');
    super();

    this.server = new Server({ path: '/manager', port: 8080 });

    this.server.on('connection', (socket) => this.onConnection(socket));
  }

  private closeSocket(id: number, code: WSServerCloseCode) {
    if (!this.sockets.has(id)) return;
    this.sockets.get(id)!.close(code);
    this.sockets.get(id)!.removeAllListeners();
    this.emit('disconnected', id, code);
    this.sockets.delete(id);
    this.sockets.forEach((socket) => {
      socket.sendPayload<ServerStructures.ClusterStatus>({
        op: ServerOpCodes.ClusterStatus,
        d: {
          count: this.clusterStats.maxClusters,
          connected: Array.from(this.sockets.keys()),
        },
      });
    });
  }

  private notifyNewCluster() {
    console.debug(
      '[notifyNewCluster] Notifying all clusters of new cluster connection...',
    );
    this.sockets.forEach((socket) => {
      socket.sendPayload<ServerStructures.ClusterStatus>({
        op: ServerOpCodes.ClusterStatus,
        d: {
          count: this.clusterStats.maxClusters,
          connected: Array.from(this.sockets.keys()),
        },
      });
    });
  }

  private onConnection(socket: WebSocket) {
    console.debug('[onConnection] New cluster connecting...');
    // Ask the client to identify itself and provide the schema and heartbeat timeout.
    socket.send(
      JSON.stringify({
        op: ServerOpCodes.Identify,
        d: { heartbeatTimeout: this.heartbeatTimeout, schema },
      }),
    );
    socket.once('message', (data) => {
      let payload = parsePayload(data.toString());
      console.debug(
        '[onConnection] Recieved first message from cluster, attempting to authenticate...',
        payload,
      );
      try {
        // Assert that the payload is an Identify payload.
        assertOpCode<ClientStructures.Identity>(
          payload,
          ClientOpCodes.Identity,
        );
      } catch (e) {
        if (e instanceof OpCodeAssertionError) {
          // The payload was not an Identify payload, so close the connection.
          socket.close(WSServerCloseCode.NotAuthenticated);
          return;
        } else {
          // An unknown error occurred while validating the payload, so close the connection.
          console.error(
            '[onConnection] An error occurred while validating the Identify payload!',
            e,
          );
          socket.close(WSServerCloseCode.ServerError);
          return;
        }
      }
      console.debug('[onConnection] Payload is Identify, validating...');
      // Authenticate the client using the provided token.
      let user = authenticate(payload.d.token);
      if (!user) {
        console.debug(
          '[onConnection] Authentication failed, closing connection.',
        );
        // Authentication failed, so close the connection.
        socket.close(WSServerCloseCode.AuthenticationFailed);
        return;
      }
      console.debug(
        '[onConnection] Authentication successful, proceeding with connection...',
      );

      // Are there already clusters connected?
      if (this.sockets.size < 1) {
        console.debug;
        // There are no clusters connected, so we can set the max cluster count to whatever the client specified.
        this.clusterStats.maxClusters = payload.d.clusters;
        // There are already clusters connected, is the new cluster's max cluster count the same as the existing clusters' max cluster count?
      } else if (this.clusterStats.maxClusters !== payload.d.clusters) {
        console.warn(
          '[onConnection] New cluster has a different max cluster count than existing clusters! Closing connection.',
        );
        // The new cluster's max cluster count is different from the existing clusters' max cluster count, so close the connection.
        socket.close(WSServerCloseCode.InvalidClusterCount);
        return;
      }
      // Is the new cluster trying to connect with an invalid cluster ID?
      if (payload.d.cluster >= this.clusterStats.maxClusters) {
        console.warn(
          '[onConnection] New cluster has an invalid cluster ID that is out of range! Closing connection.',
        );
        // The new cluster is trying to connect with an invalid cluster ID, so close the connection.
        socket.close(WSServerCloseCode.InvalidCluster);
        return;
      }
      // Is there already a cluster connected with the new cluster's cluster ID?
      if (this.sockets.has(payload.d.cluster)) {
        console.warn(
          '[onConnection] New cluster is trying to connect with a cluster ID that is already in use! Closing connection.',
        );
        // There is already a cluster connected with the new cluster's cluster ID, so close the connection.
        socket.close(WSServerCloseCode.AlreadyAuthenticated);
        return;
      }

      console.debug(
        '[onConnection] Cluster passed all validation checks, adding to connected clusters.',
      );

      // The client is authenticated and valid, so add it to the list of connected clusters.
      this.sockets.set(payload.d.cluster, WSCluster(socket, user));

      // Set up a function to handle the cluster disconnecting or encountering an error.
      const closeOrError = (code: number | Error) => {
        if (typeof code !== 'number') {
          console.error(
            '[onConnection] An error occurred with cluster',
            payload.d.cluster,
            code,
          );
        }
        // If the cluster is already disconnected, do nothing.
        if (!this.sockets.has(payload.d.cluster)) return;
        // Emit a 'disconnected' event with the cluster ID and the close code or error.
        this.emit('disconnected', payload.d.cluster, code);
        // Clear the cluster's heartbeat timeout and remove it from the list of connected clusters.
        clearTimeout(this.sockets.get(payload.d.cluster)!.heartbeatTimeoutID!);
        this.sockets.delete(payload.d.cluster);
      };

      console.debug('[onConnection] Setting up event listeners for cluster...');
      // Listen for the cluster disconnecting or encountering an error.
      this.sockets
        .get(payload.d.cluster)!
        .once('close', closeOrError)
        .once('error', closeOrError)
        // Listen for messages from the cluster and handle them with the handleClientPayload function.
        .on('message', (data) =>
          this.handleClientPayload(payload.d.cluster, data.toString()),
        );

      console.debug('[onConnection] Setting up heartbeat for cluster...');
      // Set up the heartbeat timeout for the cluster.
      this.handleHeartbeat(payload.d.cluster);

      // Emit a 'ClusterStatus' event to all connected clusters with the current cluster count and the list of connected cluster IDs.
      this.notifyNewCluster();

      console.debug(
        '[onConnection] Emitting authenticated event to service...',
      );
      // Emit an 'authenticated' event with the cluster ID, max cluster count, and username.
      this.emit(
        'authenticated',
        payload.d.cluster,
        this.clusterStats.maxClusters,
        user,
      );
    });
  }

  private handleClientPayload(id: number, data: string) {
    let payload: ClientStructures;
    try {
      payload = parsePayload(data);
    } catch (err) {
      if (err instanceof InvalidPayloadError) {
        console.warn(
          `[handleClientPayload] Received invalid payload from cluster ${id}, closing connection.`,
        );
        this.closeSocket(id, WSServerCloseCode.DecodeError);
        return;
      } else if (err instanceof InvalidOpCodeError) {
        console.warn(
          `[handleClientPayload] Received payload with invalid op code from cluster ${id}, closing connection.`,
        );
        this.closeSocket(id, WSServerCloseCode.InvalidOpcode);
        return;
      } else {
        console.error(
          `[handleClientPayload] An error occurred while parsing payload from cluster ${id}!`,
          err,
        );
        this.closeSocket(id, WSServerCloseCode.ServerError);
        return;
      }
    }
    console.debug(
      `[handleClientPayload] Parsed payload from cluster ${id}:`,
      payload,
    );
    const cluster = this.sockets.get(id)!;
    switch (payload.op) {
      // The client should never send an Identify payload after the initial authentication.
      case ClientOpCodes.Identity:
        console.debug(
          `[handleClientPayload] Cluster ${id} attempted to re-authenticate, closing connection.`,
        );
        this.closeSocket(id, WSServerCloseCode.AlreadyAuthenticated);
        break;
      // Handle heartbeat messages from the cluster.
      case ClientOpCodes.Heartbeat:
        console.debug(`[handleClientPayload] Cluster ${id} sent a heartbeat.`);
        this.handleHeartbeat(id);
        break;
      // Process received data.
      case ClientOpCodes.SendData:
        // Define a callback function to handle the response from the service after emitting the 'data' event.
        const dataCallback = (success: boolean, code?: GenericCloseCodes) => {
          // If the service successfully processed the data, send a DataACK payload back to the cluster with success: true.
          if (success)
            cluster.sendPayload<ServerStructures.DataACK>({
              op: ServerOpCodes.DataACK,
              d: { success: true },
            });
          // The service was not able to process the data, check the provided close code.
          else {
            // We weren't ready to process the data, but the client is still fine, so send a DataACK payload back to the cluster with success: false.
            if (code === GenericCloseCodes.NotReadyForData)
              cluster.sendPayload<ServerStructures.DataACK>({
                op: ServerOpCodes.DataACK,
                d: { success: false },
              });
            else
              this.closeSocket(
                id,
                code
                  ? genericToWSCloseCode(code)
                  : WSServerCloseCode.ServerError,
              );
          }
        };
        this.emit('data', id, payload.d, dataCallback);
        break;
      case ClientOpCodes.CCCBegin:
        // Create and store a new CCC instance for this request.
        let instance = this.beginCCC(id, payload.d.data, payload.d.to);
        this.cccInstances.set(instance.id, instance);
        // Send a CCCBegin payload back to the cluster with the CCC instance ID.
        cluster.sendPayload<ServerStructures.CCCBegin>({
          op: ServerOpCodes.CCCBegin,
          d: { id: instance.id },
        });
        break;
      case ClientOpCodes.CCCReturn:
        let cccInstance = this.cccInstances.get(payload.d.id);
        // If we can't find the CCC instance for this response, ignore it.
        if (!cccInstance) {
          console.warn(
            `[handleClientPayload] Received CCCReturn payload with invalid instance ID from cluster ${id}, ignoring.`,
          );
          return;
        }
        // If the CCC instance is a broadcast, store the response in the returnData array and check if we have received responses from all clusters.
        if (cccInstance.to === 'all') {
          (cccInstance.returnData as string[])[id] = payload.d.data;
          if (
            (cccInstance.returnData as string[]).filter((a) => !!a).length ===
            this.sockets.size
          ) {
            // We have received responses from all clusters, so send a CCCReturn payload back to the cluster that initiated the CCC with the collected responses.
            this.sockets.get(cccInstance.startedBy)!.sendPayload({
              op: ServerOpCodes.CCCReturn,
              d: {
                data: cccInstance.returnData,
                id: cccInstance.id,
                from: cccInstance.to,
              },
            } as ServerStructures.CCCReturn);
            // Delete the CCC instance now that it is complete.
            this.cccInstances.delete(cccInstance.id);
          }
        }
        // If the CCC instance is not a broadcast, send a CCCReturn payload back to the cluster that initiated the CCC with the response and delete the CCC instance.
        else {
          (cccInstance.returnData as string) = payload.d.data;
          this.sockets.get(cccInstance.startedBy)!.sendPayload({
            op: ServerOpCodes.CCCReturn,
            d: {
              data: cccInstance.returnData,
              id: cccInstance.id,
              from: cccInstance.to,
            },
          } as ServerStructures.CCCReturn);
          this.cccInstances.delete(cccInstance.id);
        }
        break;
      default:
        console.warn(
          `[handleClientPayload] Received payload with unrecognized op code from cluster ${id}, closing connection.`,
        );
        this.closeSocket(id, WSServerCloseCode.InvalidOpcode);
        break;
    }
  }

  private handleHeartbeat(id: number) {
    const cluster = this.sockets.get(id);
    if (!cluster) return;
    // Clear the existing heartbeat timeout.
    clearTimeout(cluster.heartbeatTimeoutID!);
    // Send a heartbeat response back to the cluster.
    console.debug(
      `[handleHeartbeat] Sending heartbeat response to cluster ${id}.`,
    );
    cluster.sendPayload<ServerStructures.Heartbeat>({
      op: ServerOpCodes.Heartbeat,
      d: undefined,
    });
    // Update the last heartbeat time for the cluster and set a new heartbeat timeout.
    cluster.lastHeartbeat = Date.now();
    cluster.heartbeatTimeoutID = setTimeout(() => {
      console.debug(
        `[handleHeartbeat] Cluster ${id} timed out due to missed heartbeat.`,
      );
      this.closeSocket(id, WSServerCloseCode.HeartbeatTimeout);
    }, this.heartbeatTimeout);
  }

  private beginCCC(
    from: number,
    data: string,
    to: number | 'all',
  ): CCCInstance {
    let instance: CCCInstance = {
      startedBy: from,
      sendingData: data,
      returnData: to === 'all' ? [] : undefined,
      to,
      id: randomUUID(),
    };
    if (to === 'all') {
      this.sockets.forEach((c) => {
        c.sendPayload<ServerStructures.CCCPropagate>({
          op: ServerOpCodes.CCCPropagate,
          d: { data, id: instance.id },
        });
      });
    } else {
      this.sockets.get(to)!.sendPayload<ServerStructures.CCCPropagate>({
        op: ServerOpCodes.CCCPropagate,
        d: { data, id: instance.id },
      });
    }
    return instance;
  }
}

const service = new WSService();

export default service;
