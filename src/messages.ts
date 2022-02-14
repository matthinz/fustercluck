import { MessageBase } from "./types";

// PrimaryControlMessage is a structure sent _to_ the primary from a worker.

const PRIMARY_CONTROL_MESSAGE_TYPES = [
  "message",
  "worker_busy",
  "worker_processed",
  "worker_ready",
  "worker_received",
] as const;

type PrimaryControlMessageType = typeof PRIMARY_CONTROL_MESSAGE_TYPES[number];

type PrimaryControlMessageBase = {
  readonly id: number;
  readonly type: PrimaryControlMessageType;
};

// PrimaryMessageEnvelope is a control message that wraps a message sent by a user.
type PrimaryMessageEnvelope<Message extends MessageBase> =
  PrimaryControlMessageBase & {
    readonly type: "message";
    readonly message: Message;
  };

/**
 * worker_busy indicates that a worker can't take any more messages.
 * Any unprocessed message IDs are returned to the primary for redistribution.
 */
type WorkerBusyMessage = PrimaryControlMessageBase & {
  readonly type: "worker_busy";
  readonly messageIds: number[];
};

/**
 * Indicates that a worker has completed processing the given messages.
 */
type WorkerProcessedMessage = PrimaryControlMessageBase & {
  readonly type: "worker_processed";
  readonly messageIds: number[];
  readonly canTakeMore: boolean;
};

/**
 * worker_ready indicates a worker is ready to receive messages.
 */
type WorkerReadyMessage = PrimaryControlMessageBase & {
  readonly type: "worker_ready";
};

/**
 * Indicates that a worker has started processing the given messages.
 */
type WorkerReceivedMessage = PrimaryControlMessageBase & {
  readonly type: "worker_received";
  readonly messageIds: number[];
  readonly canTakeMore: boolean;
};

/**
 * Control messages are the _actual_ messages sent back and forth in the system.
 * They can wrap actual user messages or be used to communicate other things.
 */
export type PrimaryControlMessage<PrimaryMessage extends MessageBase> =
  | PrimaryMessageEnvelope<PrimaryMessage>
  | WorkerBusyMessage
  | WorkerProcessedMessage
  | WorkerReadyMessage
  | WorkerReceivedMessage;

const WORKER_CONTROL_MESSAGE_TYPES = ["message"] as const;

type WorkerControlMessageType = typeof WORKER_CONTROL_MESSAGE_TYPES[number];

type WorkerControlMessageBase = {
  readonly id: number;
  readonly type: WorkerControlMessageType;
};

type WorkerMessageEnvelope<Message extends MessageBase> =
  WorkerControlMessageBase & {
    readonly type: "message";
    readonly message: Message;
  };

export type WorkerControlMessage<WorkerMessage extends MessageBase> =
  WorkerMessageEnvelope<WorkerMessage>;

/**
 * Attempts to parse a ControlMessage out of an unknown piece of data.
 */
export function parsePrimaryControlMessage<PrimaryMessage extends MessageBase>(
  input: unknown,
  parsePrimaryMessage?: (input: unknown) => PrimaryMessage | undefined
): PrimaryControlMessage<PrimaryMessage> | undefined {
  return parseControlMessage<
    PrimaryMessage,
    PrimaryControlMessage<PrimaryMessage>
  >(
    input,
    PRIMARY_CONTROL_MESSAGE_TYPES as unknown as string[],
    parsePrimaryMessage
  );
}

export function parseWorkerControlMessage<WorkerMessage extends MessageBase>(
  input: unknown,
  parseWorkerMessage?: (input: unknown) => WorkerMessage | undefined
) {
  return parseControlMessage<
    WorkerMessage,
    WorkerControlMessage<WorkerMessage>
  >(
    input,
    WORKER_CONTROL_MESSAGE_TYPES as unknown as string[],
    parseWorkerMessage
  );
}

function parseControlMessage<Message extends MessageBase, ControlMessage>(
  input: unknown,
  validMessageTypes: string[],
  parseUserMessage?: (input: unknown) => Message | undefined
): ControlMessage | undefined {
  if (input == null || typeof input !== "object") {
    return;
  }

  const { id, type } = input as any;

  if (typeof type !== "string" || typeof id !== "number") {
    return;
  }

  const isValidType = validMessageTypes.includes(type);

  if (!isValidType) {
    return;
  }

  if (type === "message") {
    // "message" messages are "envelopes" for user messages.
    const message = (input as any).message;
    if (message == null || typeof input !== "object") {
      return;
    }
    if (parseUserMessage) {
      const parsed = parseUserMessage(message);
      if (parsed == null) {
        return;
      } else {
        return {
          id,
          type,
          message: parsed,
        } as unknown as ControlMessage;
      }
    }
  }

  return input as unknown as ControlMessage;
}
