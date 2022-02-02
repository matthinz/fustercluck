import { Envelope } from "./types";

const CONTROL_MESSAGE_TYPES = [
  "worker_not_busy",
  "worker_too_busy",
  "worker_handling",
  "you_up",
] as const;

type ControlMessageBase = {
  __type__: typeof CONTROL_MESSAGE_TYPES[number];
};

/**
 * worker_handling indicates that the worker has started processing the
 * given messages.
 */
type WorkerHandlingMessage = ControlMessageBase & {
  __type__: "worker_handling";
  messageIds: number[];
  canTakeMore: boolean;
};

/**
 * worker_too_busy sent from worker to primary to indicate it's too busy to
 * handle any messages right now (and provide back any extra messages it
 * can't handle ATM).
 */
type WorkerTooBusyMessage<WorkerMessage> = ControlMessageBase & {
  __type__: "worker_too_busy";
  envelopes: Envelope<WorkerMessage>[];
};

/**
 * worker_not_busy sent from worker to primary to indicate that it is ready
 * to receive messages.
 */
type WorkerNotBusyMessage = ControlMessageBase & {
  __type__: "worker_not_busy";
};

/**
 * You Up? Is sent from primary to worker to see if it has any extra capacity.
 */
type YouUpMessage = ControlMessageBase & {
  __type__: "you_up";
};

/**
 * Control messages are sent to communicate the state of members of the system.
 */
export type ControlMessage<WorkerMessage> =
  | WorkerHandlingMessage
  | WorkerTooBusyMessage<WorkerMessage>
  | WorkerNotBusyMessage
  | YouUpMessage;

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
