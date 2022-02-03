import { Envelope } from "./types";

const CONTROL_MESSAGE_TYPES = [
  "worker_busy",
  "worker_processed",
  "worker_ready",
  "worker_received",
] as const;

type ControlMessageBase = {
  __type__: typeof CONTROL_MESSAGE_TYPES[number];
};

/**
 * Indicates that the worker has started processing the given messages.
 */
type WorkerReceivedMessage = ControlMessageBase & {
  __type__: "worker_received";
  messageIds: number[];
  canTakeMore: boolean;
};

/**
 * Indicates that the worker has completed processing the given messages.
 */
type WorkerProcessedMessage = ControlMessageBase & {
  __type__: "worker_processed";
  messageIds: number[];
  canTakeMore: boolean;
};

/**
 * worker_busy indicates that the worker can't take any more messages.
 * Unprocessed messages are returned in their original envelopes.
 */
type WorkerBusyMessage<WorkerMessage> = ControlMessageBase & {
  __type__: "worker_busy";
  envelopes: Envelope<WorkerMessage>[];
};

/**
 * worker_ready indicates a worker is ready to receive messages.
 */
type WorkerReadyMessage = ControlMessageBase & {
  __type__: "worker_ready";
};

/**
 * Control messages are sent to communicate the state of members of the system.
 */
export type ControlMessage<WorkerMessage> =
  | WorkerBusyMessage<WorkerMessage>
  | WorkerReadyMessage
  | WorkerReceivedMessage
  | WorkerProcessedMessage;

/**
 * Attempts to parse a ControlMessage out of an unknown piece of data.
 */
export function parseControlMessage<WorkerMessage>(
  m: unknown
): ControlMessage<WorkerMessage> | undefined {
  if (m == null || typeof m !== "object") {
    return;
  }

  const asRecord = m as Record<string, unknown>;
  const { __type__ } = asRecord;

  if (typeof __type__ !== "string") {
    return;
  }

  if (
    CONTROL_MESSAGE_TYPES.includes(
      __type__ as typeof CONTROL_MESSAGE_TYPES[number]
    )
  ) {
    return asRecord as ControlMessage<WorkerMessage>;
  }
}

export function parseEnvelope<Message>(
  input: unknown,
  parseMessage: (input: unknown) => Message | undefined
): Envelope<Message> | undefined {
  if (input == null || typeof input !== "object") {
    return;
  }
  const { id, message } = input as any;

  if (typeof id === "number" && message != null) {
    const parsed = parseMessage(message);
    if (parsed != null) {
      return { id, message: parsed };
    }
  }
}
