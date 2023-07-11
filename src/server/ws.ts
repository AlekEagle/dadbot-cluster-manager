import ws from 'ws';
import EventEmitter from 'events';
import { Cluster, GenericCloseCodes, ServerService } from '../types';
import ms from 'ms';
import { authenticate } from '../utils/AuthHandler';
import { randomUUID } from 'crypto';
const schema = require('../../config/schema.json');

const __wsServer = new ws.Server({ path: '/manager', port: 8080 });

class WSService extends EventEmitter implements ServerService {
  public name = 'ws';
  public getCluster(id: number): Cluster {
    const clstr = sockets.get(id);
    return { id, lastHeartbeat: clstr.lastHeartbeat, user: clstr.user };
  }
  public getAllClusters(): { [key: number]: Cluster } {
    let data: { [key: number]: Cluster } = {};
    sockets.forEach((value, key, self) => {
      data[key] = {
        id: key,
        lastHeartbeat: value.lastHeartbeat,
        user: value.user
      };
    });
    return data;
  }
  public disconnectCluster(id: number, code: GenericCloseCodes) {
    closeSocket(id, genericToWSCloseCode(code));
  }
  public serverClosing() {
    sockets.forEach((c, k) => {
      closeSocket(k, WSServerCloseCode.ServiceRestart);
    });
  }
  public dataPushed() {
    sockets.forEach(c => {
      c.sendPayload({ op: ServerOpCodes.DataPushed });
    });
  }
}
const __emitter = new WSService();
const heartbeatTimeout: number = ms('100s');

export enum ServerOpCodes {
  Heartbeat,
  Identify,
  DataACK,
  CCCPropagate,
  CCCReturn,
  CCCConfirm,
  ClusterStatus,
  DataPushed
}

export enum WSServerCloseCode {
  Normal = 1000,
  NoStatus = 1005,
  Abnormal = 1006,
  ServerError = 1011,
  ServiceRestart = 1012,
  BadGateway = 1014,
  UnknownError = 4000,
  InvalidOpcode = 4001,
  DecodeError = 4002,
  NotAuthenticated = 4003,
  AuthenticationFailed = 4004,
  AlreadyAuthenticated = 4005,
  HeartbeatTimeout = 4006,
  Ratelimited = 4008,
  InvalidCluster = 4010,
  InvalidClusterCount = 4011,
  invalidCCCID = 4012
}

export enum WSClientCloseCode {
  Normal = 1000,
  GoingAway = 1001,
  NoStatus = 1005,
  Abnormal = 1006
}

export enum ClientOpCodes {
  Heartbeat,
  Identity,
  SendData,
  CCCBegin,
  CCCReturn
}

export interface PayloadStructure<T> {
  op: ServerOpCodes | ClientOpCodes;
  d?: T;
}

export namespace ServerStructures {
  export interface Heartbeat {}
  export interface Identify {
    heartbeatTimeout: number;
    schema: any;
  }
  export interface DataACK {
    success: boolean;
  }
  export interface CCCPropagate {
    id: string;
    data: string;
  }
  export interface CCCReturn {
    id: string;
    from: number | 'all';
    data: string;
  }
  export interface CCCConfirm {
    id: string;
  }
  export interface ClusterStatus {
    count: number;
    connected: number[];
  }
  export interface DataPushed {}
}

export namespace ClientStructures {
  export interface Heartbeat {}
  export interface Identity {
    token: string;
    clusters: number;
    cluster: number;
  }
  export interface SendData {
    type: 0 | 1 | 2;
    data: any;
  }
  export interface CCCBegin {
    to: number | 'all';
    data: string;
  }
  export interface CCCReturn {
    id: string;
    data: string;
  }
}

interface CCCInstance {
  startedBy: number;
  to: number | 'all';
  sendingData: string;
  returnData?: string | string[];
  id: string;
}

interface __WSClusterConnection extends ws {
  lastHeartbeat?: number;
  heartbeatTimeoutID?: NodeJS.Timeout;
  user: string;
  sendPayload(payload: PayloadStructure<any>): void;
}

function closeSocket(id: number, code: WSServerCloseCode) {
  if (!sockets.has(id)) return;
  sockets.get(id).close(code);
  sockets.get(id).removeAllListeners();
  __emitter.emit('disconnected', id, code);
  sockets.delete(id);
  sockets.forEach(socket => {
    socket.sendPayload({
      op: ServerOpCodes.ClusterStatus,
      d: {
        count: clusterStats.maxClusters,
        connected: Array.from(sockets.keys())
      }
    } as PayloadStructure<ServerStructures.ClusterStatus>);
  });
}

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

function parsePayload(rawPayload: string): PayloadStructure<any> {
  let payload;
  try {
    payload = JSON.parse(rawPayload);
  } catch (e) {
    console.error(e);
    throw new Error('rawPayload not valid JSON');
  }
  if (typeof payload.op !== 'number') {
    throw new Error('rawPayload missing op code');
  }
  return payload as PayloadStructure<any>;
}

const sockets: Map<number, __WSClusterConnection> = new Map<
  number,
  __WSClusterConnection
>();
const clusterStats = {
  maxClusters: -1
};

const CCCInstances: Map<string, CCCInstance> = new Map<string, CCCInstance>();

function ClusterFromSocket(socket: ws, usr: string): __WSClusterConnection {
  let skt = Object.create(socket);

  let clstr: __WSClusterConnection = Object.assign(skt, {
    sendPayload: function (payload: PayloadStructure<any>, ...args: any) {
      this.send(JSON.stringify(payload), ...args);
    },
    user: usr
  });

  return clstr;
}

__wsServer.on('connection', socket => {
  socket.send(
    JSON.stringify({
      op: ServerOpCodes.Identify,
      d: { heartbeatTimeout, schema }
    })
  );
  socket.once('message', data => {
    let payload = parsePayload(data.toString());
    if (!payload.d || !(payload.d as ClientStructures.Identity).token) {
      socket.close(WSServerCloseCode.InvalidOpcode);
      return;
    }
    let usr: string;
    try {
      usr = authenticate((payload.d as ClientStructures.Identity).token);
    } catch (e) {
      socket.close(WSServerCloseCode.AuthenticationFailed);
    }
    if (!usr) {
      socket.close(WSServerCloseCode.AuthenticationFailed);
      return;
    } else {
      if (sockets.size < 1)
        clusterStats.maxClusters = (
          payload.d as ClientStructures.Identity
        ).clusters;
      else if (
        clusterStats.maxClusters !==
        (payload.d as ClientStructures.Identity).clusters
      ) {
        socket.close(WSServerCloseCode.InvalidClusterCount);
        return;
      }
      if (
        (payload.d as ClientStructures.Identity).cluster >=
        clusterStats.maxClusters
      ) {
        socket.close(WSServerCloseCode.InvalidCluster);
        return;
      }
      if (sockets.has((payload.d as ClientStructures.Identity).cluster)) {
        socket.close(WSServerCloseCode.AlreadyAuthenticated);
        return;
      }

      sockets.set(
        (payload.d as ClientStructures.Identity).cluster,
        ClusterFromSocket(socket, usr)
      );

      function closeOrError(code: number | Error) {
        if (!sockets.has((payload.d as ClientStructures.Identity).cluster))
          return;
        __emitter.emit(
          'disconnected',
          (payload.d as ClientStructures.Identity).cluster,
          code
        );
        clearTimeout(
          sockets.get((payload.d as ClientStructures.Identity).cluster)
            .heartbeatTimeoutID
        );
        sockets.delete((payload.d as ClientStructures.Identity).cluster);
      }
      sockets
        .get((payload.d as ClientStructures.Identity).cluster)
        .once('close', closeOrError);
      sockets
        .get((payload.d as ClientStructures.Identity).cluster)
        .once('error', closeOrError);

      sockets
        .get((payload.d as ClientStructures.Identity).cluster)
        .on('message', data =>
          handleClientPayload(
            (payload.d as ClientStructures.Identity).cluster,
            parsePayload(data.toString())
          )
        );

      handleHeartbeat((payload.d as ClientStructures.Identity).cluster);

      sockets.forEach((socket, key) => {
        socket.sendPayload({
          op: ServerOpCodes.ClusterStatus,
          d: {
            count: clusterStats.maxClusters,
            connected: Array.from(sockets.keys())
          }
        } as PayloadStructure<ServerStructures.ClusterStatus>);
      });

      __emitter.emit(
        'authenticated',
        (payload.d as ClientStructures.Identity).cluster,
        (payload.d as ClientStructures.Identity).clusters,
        usr
      );
    }
  });
});

function handleClientPayload(id: number, data: PayloadStructure<any>) {
  console.debug(id, data);
  const clstr = sockets.get(id);
  switch (data.op) {
    case ClientOpCodes.Identity:
      closeSocket(id, WSServerCloseCode.AlreadyAuthenticated);
      break;
    case ClientOpCodes.Heartbeat:
      handleHeartbeat(id);
      break;
    case ClientOpCodes.SendData:
      let sDData = data as PayloadStructure<ClientStructures.SendData>;
      function dataCB(success: boolean, code?: GenericCloseCodes) {
        if (success)
          clstr.sendPayload({
            op: ServerOpCodes.DataACK,
            d: { success: true }
          } as PayloadStructure<ServerStructures.DataACK>);
        else {
          if (code !== GenericCloseCodes.NotReadyForData)
            closeSocket(
              id,
              code ? genericToWSCloseCode(code) : WSServerCloseCode.ServerError
            );
          else
            clstr.sendPayload({
              op: ServerOpCodes.DataACK,
              d: { success: true }
            } as PayloadStructure<ServerStructures.DataACK>);
        }
      }
      __emitter.emit('data', id, sDData.d, dataCB);
      break;
    case ClientOpCodes.CCCBegin:
      let cccBeginData = data as PayloadStructure<ClientStructures.CCCBegin>;
      let instance = beginCCC(id, cccBeginData.d.data, cccBeginData.d.to);
      CCCInstances.set(instance.id, instance);
      sockets.get(id).sendPayload({
        op: ServerOpCodes.CCCConfirm,
        d: { id: instance.id }
      } as PayloadStructure<ServerStructures.CCCConfirm>);
      break;
    case ClientOpCodes.CCCReturn:
      let cccReturnData = data as PayloadStructure<ClientStructures.CCCReturn>;
      let ins = CCCInstances.get(cccReturnData.d.id);
      if (ins.to === 'all') {
        (ins.returnData as string[])[id] = cccReturnData.d.data;
        if (
          (ins.returnData as string[]).filter(a => !!a).length === sockets.size
        ) {
          sockets.get(ins.startedBy).sendPayload({
            op: ServerOpCodes.CCCReturn,
            d: {
              data: ins.returnData,
              id: ins.id,
              from: ins.to
            }
          } as PayloadStructure<ServerStructures.CCCReturn>);
          CCCInstances.delete(ins.id);
        }
      } else {
        (ins.returnData as string) = cccReturnData.d.data;
        sockets.get(ins.startedBy).sendPayload({
          op: ServerOpCodes.CCCReturn,
          d: { data: ins.returnData, id: ins.id, from: ins.to }
        } as PayloadStructure<ServerStructures.CCCReturn>);
        CCCInstances.delete(ins.id);
      }
      break;
    default:
      closeSocket(id, WSServerCloseCode.InvalidOpcode);
      break;
  }
}

function beginCCC(from: number, data: string, to: number | 'all'): CCCInstance {
  let instance: CCCInstance = {
    startedBy: from,
    sendingData: data,
    returnData: to === 'all' ? [] : null,
    to,
    id: randomUUID()
  };
  if (to === 'all') {
    sockets.forEach(c => {
      c.sendPayload({
        op: ServerOpCodes.CCCPropagate,
        d: { data, id: instance.id }
      } as PayloadStructure<ServerStructures.CCCPropagate>);
    });
  } else {
    sockets.get(to).sendPayload({
      op: ServerOpCodes.CCCPropagate,
      d: { data, id: instance.id }
    } as PayloadStructure<ServerStructures.CCCPropagate>);
  }
  return instance;
}

function handleHeartbeat(id: number) {
  const cluster = sockets.get(id);
  if (!cluster) return;
  clearTimeout(cluster.heartbeatTimeoutID);
  cluster.sendPayload({ op: ServerOpCodes.Heartbeat });
  cluster.lastHeartbeat = Date.now();
  cluster.heartbeatTimeoutID = setTimeout(() => {
    closeSocket(id, WSServerCloseCode.HeartbeatTimeout);
  }, heartbeatTimeout);
}

export default __emitter;
