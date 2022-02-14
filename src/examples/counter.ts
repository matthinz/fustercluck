import crypto from "crypto";
import * as fc from "..";
import { Primary, Worker } from "../types";

type CountMessage = {
  type: "count";
  number: number;
};

type CountedMessage = {
  type: "counted";
  number: number;
  input: string;
  hash: string;
};

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

async function run() {
  const instance = fc.start<CountedMessage, CountMessage>();

  if (instance.role === "primary") {
    runPrimary(instance);
  } else {
    runWorker(instance);
  }
}

async function runPrimary({
  handle,
  sendToWorkersAndWaitForReceipt,
}: Primary<CountedMessage, CountMessage>): Promise<void> {
  let number = 0;

  handle("counted", (m) => {
    console.log(m.number, m.input, m.hash);
  });

  while (true) {
    number++;
    await sendToWorkersAndWaitForReceipt({
      type: "count",
      number,
    });
  }
}

function runWorker({
  handle,
  addBusyCheck,
  sendToPrimary,
}: Worker<CountedMessage, CountMessage>) {
  let busy = false;

  const ITERATIONS = 1000;

  handle("count", (m) => {
    try {
      busy = true;

      while (true) {
        const values = new Uint8Array(m.number);
        for (let i = 0; i < m.number; i++) {
          values[i] = Math.floor(Math.random() * 255);
        }

        let buffer = Buffer.from(values);
        const input = buffer.toString("hex");

        for (let i = 0; i < ITERATIONS; i++) {
          const hash = crypto.createHash("sha512");
          hash.update(buffer);
          buffer = hash.digest();
        }

        const hash = buffer.toString("hex");

        const product = hash
          .split("")
          .map((digit) => parseInt(digit, 16))
          .reduce<number>((value, digit) => {
            return value * (digit === 0 ? 1 : digit);
          }, 1);

        const remainder = product % m.number;

        // console.error("%d %% %d = %d", product, m.number, remainder);

        if (remainder === 0) {
          sendToPrimary({
            type: "counted",
            number: m.number,
            input,
            hash,
          });
          break;
        }
      }
    } finally {
      busy = false;
    }
  });

  // We are busy we have one count going
  addBusyCheck(() => busy);
}
