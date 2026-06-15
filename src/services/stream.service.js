const streamClient = require('../configs/stream.config');

function createStreamToken(userId) {
  if (!userId) {
    throw new Error('userId wajib diisi');
  }

  // token ini dipakai UNTUK:
  // - StreamChat
  // - Stream Call (voice/video)
  return streamClient.createToken(userId);
}

module.exports = {
  createStreamToken,
};
