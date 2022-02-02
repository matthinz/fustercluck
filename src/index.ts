import child from "child_process";
import cluster, { Worker as ClusterWorker } from "cluster";
import { runPrimary } from "./primary";
import { Primary, RunOptions, RunResult, Worker } from "./types";
import { runWorker } from "./worker";

export function run<
  PrimaryMessage extends child.Serializable,
  WorkerMessage extends child.Serializable
>(
  options: RunOptions<PrimaryMessage, WorkerMessage>
): RunResult<PrimaryMessage, WorkerMessage> {
  if (cluster.isPrimary) {
    return runPrimary(options);
  } else if (cluster.isWorker) {
    return runWorker(options);
  } else {
    throw new Error("Neither a primary nor a worker. Is this Node 16?");
  }
}
