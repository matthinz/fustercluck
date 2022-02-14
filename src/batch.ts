export type MessageBatch = {
  allProcessed(): Promise<void>;
  allReceived(): Promise<void>;
  isComplete(): boolean;
  markProcessed(ids: number[]): void;
  markReceived(ids: number[]): void;
};

type MessageState = "sent" | "processed" | "received";

export function createMessageBatch<Message extends { readonly id: number }>(
  messages: Message[]
): MessageBatch {
  const messageStatesById = messages.reduce<{
    [id: number]: MessageState | undefined;
  }>((result, message) => {
    result[message.id] = "sent";
    return result;
  }, {});

  const messageCount = Object.keys(messageStatesById).length;

  const processed = makePromise<void>();
  let processedCount = 0;

  const received = makePromise<void>();
  let receivedCount = 0;

  if (messageCount === 0) {
    processed.resolve();
    received.resolve();
  }

  return {
    allProcessed,
    allReceived,
    isComplete,
    markProcessed,
    markReceived,
  };

  function allProcessed(): Promise<void> {
    return processed.promise;
  }

  function allReceived(): Promise<void> {
    return received.promise;
  }

  function isComplete() {
    return processedCount === messageCount && receivedCount === messageCount;
  }

  function markProcessed(ids: number[]) {
    for (let id of ids) {
      if (messageStatesById[id] === "received") {
        messageStatesById[id] = "processed";
        processedCount++;
        if (processedCount === messageCount) {
          processed.resolve();
        }
      }
    }
  }

  function markReceived(ids: number[]) {
    for (let id of ids) {
      if (messageStatesById[id] === "sent") {
        messageStatesById[id] = "received";
        receivedCount++;
        if (receivedCount === messageCount) {
          received.resolve();
        }
      }
    }
  }
}

function makePromise<T>(): {
  readonly promise: Promise<T>;
  resolve: (result: T) => void;
  reject: (err: any) => void;
} {
  let resolveInsidePromise: (value: T) => void | undefined;
  let rejectInsidePromise: (err: any) => void | undefined;

  let onPromiseAvailable:
    | ((resolve: (value: T) => void, reject: (err: any) => void) => void)
    | undefined;

  const promise = new Promise<T>((resolve, reject) => {
    resolveInsidePromise = resolve;
    rejectInsidePromise = reject;
    if (onPromiseAvailable) {
      onPromiseAvailable(resolve, reject);
    }
  });

  return { promise, resolve, reject };

  function resolve(value: T) {
    if (resolveInsidePromise) {
      resolveInsidePromise(value);
    } else if (!onPromiseAvailable) {
      onPromiseAvailable = (resolve, reject) => {
        resolve(value);
      };
    }
  }

  function reject(err: any) {
    if (rejectInsidePromise) {
      rejectInsidePromise(err);
    } else if (!onPromiseAvailable) {
      onPromiseAvailable = (resolve, reject) => {
        reject(err);
      };
    }
  }
}
