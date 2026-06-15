const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const socketController = require('./controllers/socket.controller');
const userRoutes = require('./routes/user.routes');
require('dotenv').config();

const app = express();

app.use(cors());

app.use(express.json());

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const HOST = process.env.HOST || '0.0.0.0';

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  path: '/backend-ade-taxi/socket.io',
  pingInterval: 25000,
  pingTimeout: 60000,
  maxHttpBufferSize: 1e6,
  allowEIO3: true, // jika ada klien lama yang masih pakai EIO3
});

socketController(io);

app.use('/api', userRoutes);
app.get('/health', (req, res) => res.json({ ok: true, pid: process.pid }));

server.listen(PORT, HOST, () => {
  console.log(`Server jalan di ${HOST}:${PORT} (pid=${process.pid})`);
});

server.on('error', (err) => {
  if (err && err.code === 'EADDRINUSE') {
    console.error(
      `Port ${PORT} sudah dipakai (EADDRINUSE). Pastikan tidak ada proses lain yang memakai port ini.`
    );
  } else {
    console.error('Server error:', err);
  }
  // exit agar process manager (pm2) bisa restart atau kamu bisa handle sesuai kebijakan
  process.exit(1);
});

// graceful shutdown
function shutdown(signal) {
  console.log(`Menerima ${signal}. Menutup server...`);
  try {
    io.close(() => {
      console.log('Socket.IO ditutup.');
      server.close(() => {
        console.log('HTTP server ditutup. Keluar.');
        process.exit(0);
      });
    });
    // jika tidak selesai dalam X detik, paksa exit
    setTimeout(() => {
      console.warn('Shutdown timeout, memaksa keluar.');
      process.exit(1);
    }, 5000);
  } catch (e) {
    console.error('Error saat shutdown:', e);
    process.exit(1);
  }
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', (err) => {
  console.error('uncaughtException:', err);
  shutdown('uncaughtException');
});
process.on('unhandledRejection', (reason) => {
  console.error('unhandledRejection:', reason);
  // jangan langsung exit jika mau, tapi log dulu
});
