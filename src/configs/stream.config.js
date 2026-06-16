const { StreamClient } = require("@stream-io/node-sdk");

const STREAM_API_KEY = process.env.STREAM_API_KEY;
const STREAM_API_SECRET = process.env.STREAM_API_SECRET;
if (!STREAM_API_KEY || !STREAM_API_SECRET) {
  throw new Error("STREAM_API_KEY atau STREAM_API_SECRET belum diset");
}

// client khusus SERVER (pakai secret)
const streamClient = new StreamClient(STREAM_API_KEY, STREAM_API_SECRET);

module.exports = streamClient;
