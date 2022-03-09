const MAX_BUFFER_LENGTH = 32;

/**
 * Converts a value to JSON, taking a few shortcuts along the way.
 */
export function toDebuggingJson(value: any, indent?: number): string {
  return JSON.stringify(value, replacer, indent);
}

function replacer(key: string, value: any) {
  let b: Buffer | undefined;

  if (value && value.type === "Buffer" && Array.isArray(value.data)) {
    // We've got a .toJSON'd Buffer
    b = Buffer.from(value.data);
  } else if (value instanceof Buffer) {
    b = value;
  }

  if (b != null) {
    if (b.length <= MAX_BUFFER_LENGTH) {
      return `<Buffer: ${b.toString("hex")}>`;
    } else {
      return `<Buffer: ${b.length} bytes>`;
    }
  }

  return value;
}
