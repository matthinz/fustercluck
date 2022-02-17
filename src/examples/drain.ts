import * as fc from "..";

// This example assigns multiple long-running tasks to each worker so we can
// test how work is drained from those workers when stopping.

type PrimaryMessage = never;

type WorkerMessage = {
  type: "do_something";
};

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

async function run() {
  const instance = fc.start<PrimaryMessage, WorkerMessage>();
  if (instance.role === "primary") {
    await runPrimary(instance);
  } else {
    runWorker(instance);
  }
}

async function runPrimary(instance: fc.Primary<never, WorkerMessage>) {
  while (true) {
    await instance.sendToWorkersAndWaitForReceipt({
      type: "do_something",
    });
  }
}

function runWorker(instance: fc.Worker<never, WorkerMessage>) {
  let processing = 0;

  instance.handle(
    "do_something",
    () =>
      new Promise((resolve) => {
        processing++;
        const delay = Math.random() * 10000;
        console.log("Delaying %dms...", delay);
        setTimeout(() => {
          processing--;
          resolve();
        }, delay);
      })
  );

  instance.addBusyCheck(() => processing > 10);
}
