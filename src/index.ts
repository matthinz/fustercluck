import child from "child_process";
import cluster from "cluster";
import { runPrimary } from "./primary";
import { RunOptions, RunResult } from "./types";
import { runWorker } from "./worker";

export {
  InitializeWorkerOptions,
  Primary,
  PrimaryHandleOptions,
  RunOptions,
  RunResult,
  Worker,
  WorkerHandleOptions,
} from "./types";

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
