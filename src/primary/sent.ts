import { ExposedPromise, makePromise } from "./promise";

export type MessageBatch = {
  allProcessed(): Promise<void>;
  allReceived(): Promise<void>;
  toString(): string;
};

export type SendTracker<
  Message extends { id: number },
  Envelope extends { message: Message }
> = {
  createBatch(messages: Message[]): MessageBatch;
  errorSending(envelope: Envelope): void;
  /**
   * Informs the tracker that all sending has been
   */
  stop(): void;
  numberOfMessagesOutstanding(): number;
  sent(envelope: Envelope): void;
  received(messageId: number[]): void;
  rejected(messageIds: number[]): Envelope[];
  processed(messageIds: number[]): void;
  waitForIdle(): Promise<void>;
};

export function createSendTracker<
  Message extends { id: number },
  Envelope extends { message: Message }
>(): SendTracker<Message, Envelope> {
  type SentMessage = {
    envelope: Envelope;
    received: boolean;
    processed: boolean;
  };

  const sentById: { [id: number]: SentMessage } = {};

  const batchesByMessageId: {
    [id: number]: {
      unreceivedMessageIds: number[];
      unprocessedMessageIds: number[];
      received: ExposedPromise<void>;
      processed: ExposedPromise<void>;
    };
  } = {};

  let isStopped = false;

  return {
    createBatch,
    numberOfMessagesOutstanding,
    errorSending,
    stop,
    sent,
    received,
    rejected,
    processed,
    waitForIdle,
  };

  function createBatch(messages: Message[]): MessageBatch {
    const received = makePromise<void>();
    const processed = makePromise<void>();
    const unreceivedMessageIds = messages.map(({ id }) => id);
    const unprocessedMessageIds = [...unreceivedMessageIds];

    if (messages.length === 0) {
      received.resolve();
      processed.resolve();
    }

    const batch = {
      unreceivedMessageIds,
      unprocessedMessageIds,
      received,
      processed,
    };

    messages.forEach(({ id }) => {
      batchesByMessageId[id] = batch;
    });

    return {
      allProcessed: () => processed.promise,
      allReceived: () => received.promise,
      toString() {
        return "";
      },
    };
  }

  function errorSending(envelope: Envelope) {
    delete sentById[envelope.message.id];
  }

  function numberOfMessagesOutstanding(): number {
    return Object.keys(sentById).length;
  }

  function sent(envelope: Envelope) {
    sentById[envelope.message.id] = {
      envelope,
      received: false,
      processed: false,
    };
  }

  function received(messageIds: number[]) {
    messageIds.forEach((id) => {
      const sentMessage = sentById[id];
      if (!sentMessage) {
        throw new Error(`Unknown message id: ${id}`);
      }

      sentMessage.received = true;
      if (sentMessage.received && sentMessage.processed) {
        delete sentById[id];
      }

      const batch = batchesByMessageId[id];
      if (!batch) {
        return;
      }

      let found = false;
      for (let i = 0; i < batch.unreceivedMessageIds.length; i++) {
        if (batch.unreceivedMessageIds[i] === id) {
          batch.unreceivedMessageIds.splice(i, 1);
          found = true;
          break;
        }
      }

      if (found && batch.unreceivedMessageIds.length === 0) {
        batch.received.resolve();
      }
    });
  }

  function rejected(messageIds: number[]): Envelope[] {
    const result: Envelope[] = [];

    messageIds.forEach((id) => {
      const sentMessage = sentById[id];
      if (!sentMessage) {
        throw new Error(`Unknown message id: ${id}`);
      }

      // Once rejected, we stop tracking the ID until it is re-sent
      delete sentById[id];

      result.push(sentMessage.envelope);
    });

    return result;
  }

  function processed(messageIds: number[]) {
    messageIds.forEach((id) => {
      const sentMessage = sentById[id];
      if (!sentMessage) {
        throw new Error(`Unknown message id: ${id}`);
      }

      sentMessage.processed = true;
      if (sentMessage.received && sentMessage.processed) {
        delete sentById[id];
      }

      const batch = batchesByMessageId[id];
      if (!batch) {
        return;
      }

      let found = false;
      for (let i = 0; i < batch.unprocessedMessageIds.length; i++) {
        if (batch.unprocessedMessageIds[i] === id) {
          batch.unprocessedMessageIds.splice(i, 1);
          found = true;
          break;
        }
      }

      if (found && batch.unprocessedMessageIds.length === 0) {
        batch.processed.resolve();
      }
    });
  }

  function stop() {
    isStopped = true;
    Object.keys(batchesByMessageId)
      .map((id) => Number(id))
      .forEach((id) => {
        batchesByMessageId[id].processed.resolve();
        batchesByMessageId[id].received.resolve();
        delete batchesByMessageId[id];
      });
  }

  function waitForIdle(): Promise<void> {
    return new Promise((resolve) => {
      tryResolve();
      function tryResolve() {
        if (isStopped) {
          resolve();
          return;
        }
        if (numberOfMessagesOutstanding() === 0) {
          resolve();
        } else {
          setTimeout(tryResolve, 100);
        }
      }
    });
  }
}
