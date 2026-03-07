import { DataTypes } from '../../types';

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
  invalidCCCID = 4012,
}

export enum WSClientCloseCode {
  Normal = 1000,
  GoingAway = 1001,
  NoStatus = 1005,
  Abnormal = 1006,
}

export enum ServerOpCodes {
  Heartbeat,
  Identify,
  DataACK,
  CCCPropagate,
  CCCReturn,
  CCCBegin,
  ClusterStatus,
  DataPushed,
}

export enum ClientOpCodes {
  Heartbeat,
  Identity,
  SendData,
  CCCBegin,
  CCCReturn,
}

export namespace ServerStructures {
  export interface Heartbeat {
    op: ServerOpCodes.Heartbeat;
    d: undefined;
  }
  export interface Identify {
    op: ServerOpCodes.Identify;
    d: {
      heartbeatTimeout: number;
      schema: any;
    };
  }
  export interface DataACK {
    op: ServerOpCodes.DataACK;
    d: {
      success: boolean;
    };
  }
  export interface CCCPropagate {
    op: ServerOpCodes.CCCPropagate;
    d: {
      id: string;
      data: string;
    };
  }
  export interface CCCReturn {
    op: ServerOpCodes.CCCReturn;
    d: {
      id: string;
      from: number | 'all';
      data: string;
    };
  }
  export interface CCCBegin {
    op: ServerOpCodes.CCCBegin;
    d: {
      id: string;
    };
  }
  export interface ClusterStatus {
    op: ServerOpCodes.ClusterStatus;
    d: {
      count: number;
      connected: number[];
    };
  }
  export interface DataPushed {
    op: ServerOpCodes.DataPushed;
    d: undefined;
  }
}

export type ServerStructures =
  | ServerStructures.Heartbeat
  | ServerStructures.Identify
  | ServerStructures.DataACK
  | ServerStructures.CCCPropagate
  | ServerStructures.CCCReturn
  | ServerStructures.CCCBegin
  | ServerStructures.ClusterStatus
  | ServerStructures.DataPushed;

export namespace ClientStructures {
  export interface Heartbeat {
    op: ClientOpCodes.Heartbeat;
    d: undefined;
  }
  export interface Identity {
    op: ClientOpCodes.Identity;
    d: {
      token: string;
      clusters: number;
      cluster: number;
    };
  }
  export interface SendData {
    op: ClientOpCodes.SendData;
    d: {
      type: DataTypes;
      data: any;
    };
  }
  export interface CCCBegin {
    op: ClientOpCodes.CCCBegin;
    d: {
      to: number | 'all';
      data: string;
    };
  }
  export interface CCCReturn {
    op: ClientOpCodes.CCCReturn;
    d: {
      id: string;
      data: string;
    };
  }
}

export type ClientStructures =
  | ClientStructures.Heartbeat
  | ClientStructures.Identity
  | ClientStructures.SendData
  | ClientStructures.CCCBegin
  | ClientStructures.CCCReturn;

export class InvalidPayloadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidPayloadError';
  }
}

export class InvalidOpCodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidOpCodeError';
  }
}

export class OpCodeAssertionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OpCodeAssertionError';
  }
}

export function parsePayload<T extends ClientStructures>(data: string): T {
  let parsed: T;
  try {
    parsed = JSON.parse(data);
  } catch (err) {
    throw new Error('Invalid JSON');
  }
  if (typeof parsed.op !== 'number') {
    throw new InvalidPayloadError('Missing op code');
  }
  if (ClientOpCodes[parsed.op] === undefined) {
    throw new InvalidOpCodeError('Invalid op code');
  }
  return parsed;
}

export function assertOpCode<T extends ClientStructures>(
  payload: ClientStructures,
  op: T['op'],
): asserts payload is T {
  if (payload.op !== op) {
    throw new OpCodeAssertionError(
      `Expected op code ${ClientOpCodes[op]}, got ${
        ClientOpCodes[payload.op] ?? 'unknown'
      }`,
    );
  }
}
