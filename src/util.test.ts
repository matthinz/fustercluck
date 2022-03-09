import { toDebuggingJson } from "./util";

test("toDebuggingJson -- catches buffers in objects", () => {
  const actual = toDebuggingJson({ foo: Buffer.from([1, 2, 3, 4]) });
  const expected = '{"foo":"<Buffer: 01020304>"}';
  expect(actual).toBe(expected);
});
