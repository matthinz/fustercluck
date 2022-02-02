import * as framework from "..";
import { Primary, Worker } from "../types";

// This example creates a program that counts from 0 - 1,000,000, spreading the
// actual counting across as many workers as possible.

type StartCountingMessage = {
  type: "start_counting";
  from: number;
  to: number;
};

type CountedMessage = {
  type: "counted";
  number: number;
};

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

async function run() {
  let nextBatchStart = 0;
  const { sendToWorkers } = framework.run({
    createPrimary,
    createWorker,
  });

  if (sendToWorkers) {
    // We are the primary, and we can send messages to our workers.
    sendNextBatch();
  }

  function sendNextBatch() {
    if (!sendToWorkers) {
      return;
    }

    const BATCH_SIZE = 10000;
    const MAX = 1000000;

    if (nextBatchStart >= MAX) {
      return;
    }

    const from = nextBatchStart;
    const to = from + BATCH_SIZE - 1;

    nextBatchStart = from + BATCH_SIZE;

    sendToWorkers({
      type: "start_counting",
      from,
      to,
    }).then(() => {
      setImmediate(sendNextBatch);
    });
  }
}

function createPrimary(): Primary<CountedMessage, StartCountingMessage> {
  return {
    handle(m: CountedMessage): Promise<void> {
      console.log(m.number);
      return Promise.resolve();
    },

    initializeWorker() {
      console.log("initializeWorker");
      return Promise.resolve();
    },

    parseMessage(m: unknown): CountedMessage | undefined {
      return m ? (m as CountedMessage) : undefined;
    },
  };
}

function createWorker(): Worker<CountedMessage, StartCountingMessage> {
  let interval: NodeJS.Timer | undefined;

  return {
    handle(m: StartCountingMessage, { sendToPrimary }): Promise<void> {
      return new Promise((resolve, reject) => {
        let number = m.from;
        interval = setInterval(() => {
          if (number >= m.to) {
            interval && clearInterval(interval);
            interval = undefined;
            resolve();
            return;
          }

          sendToPrimary({
            type: "counted",
            number,
          });

          number++;
        }, 100);
      });
    },
    isBusy() {
      return !!interval;
    },
    parseMessage(m: unknown): StartCountingMessage | undefined {
      return m ? (m as StartCountingMessage) : undefined;
    },
  };
}
