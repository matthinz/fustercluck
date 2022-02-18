import { createMockDriver } from "./driver/mock";
import { startPrimary } from "./primary";

type WorkerMessage = {
  type: "ping";
};

type PrimaryMessage = {
  type: "pong";
};

test("stopping event fires", async () => {
  const driver = createMockDriver<PrimaryMessage, WorkerMessage>(
    4,
    (worker) => {
      worker.handle("ping", () => {});
    }
  );

  const primary = startPrimary({ driver });
  let stoppingFired = false;
  primary.on("stopping", () => {
    stoppingFired = true;
  });

  await primary.stop();

  expect(stoppingFired).toBeTruthy();
});

test("sendToWorkersAndWaitForProcessing", async () => {
  let workerReceiveCount = 0;
  let primaryReceiveCount = 0;

  const driver = createMockDriver<PrimaryMessage, WorkerMessage>(
    4,
    (worker) => {
      worker.handle("ping", () => {
        workerReceiveCount++;
        worker.sendToPrimary({ type: "pong" });
      });
    }
  );

  const primary = startPrimary<PrimaryMessage, WorkerMessage>({ driver });

  primary.handle("pong", () => {
    primaryReceiveCount++;
  });

  for (let i = 0; i < 5; i++) {
    await primary.sendToWorkersAndWaitForProcessing({ type: "ping" });
    expect(workerReceiveCount).toBe(i + 1);
  }

  await primary.idle();

  expect(primaryReceiveCount).toBe(5);

  await primary.stop();
});
