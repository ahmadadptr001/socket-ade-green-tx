const { createStreamToken } = require("../services/stream.service");

const onlineDrivers = new Map(); // id -> { id, status, coords, socketId, car_information }
const rides = new Map(); // rideId -> { rideId, customerId, driverId, status, destination, customerLocation? }
const socketIdToDriverId = new Map(); // socketId -> driverId (reverse lookup)
const onlineCustomers = new Map(); // socketId -> { socketId, lastSeen, meta }

// CHAT additions:
const userSockets = new Map(); // userId -> Set(socketId)
const messages = new Map(); // roomId -> [message]

// SUPPORT CHAT (live chat ke admin) — sesi EPHEMERAL: hilang saat berakhir
// atau saat user terputus. Tidak ada persistensi (sesuai kebutuhan).
const supportSessions = new Map();
// sessionId -> { sessionId, userId, role:'customer'|'driver', name,
//   status:'waiting'|'active'|'ended', adminId, createdAt, messages:[] }
const supportAdmins = new Set(); // socketId admin yang online di panel

// CALL addition
const activeCalls = new Map();
// Tenggang waktu sebelum panggilan diakhiri saat salah satu pihak terputus.
// Mencegah putus sesaat (mis. saat WebRTC dinyalakan ketika panggilan diangkat
// memicu socket.io reconnect) langsung membatalkan panggilan yang baru saja
// tersambung.
const CALL_DISCONNECT_GRACE_MS = 15000;
// callId -> {
//   callId,
//   fromUserId,
//   toUserId,
//   roomId,
//   status: 'ringing' | 'accepted' | 'rejected' | 'ended',
//   createdAt
// }

// QR-CODE RIDE addition: state perjalanan QR ditrack di backend (bukan relay buta)
const qrRides = new Map();
// rideId -> {
//   rideId, driverId, driver(profile), customerId, customer(profile),
//   status: 'free' | 'waiting_customer' | 'active' | 'completed',
//   driverLocation, customerLocation, destination,
//   originText, destinationText, fare, distance, duration,
//   createdAt, updatedAt
// }

// PRESENCE addition: lokasi terakhir tiap driver, TETAP tersimpan walau driver
// disconnect (online:false). Untuk fitur "driver tidak terhubung + lokasi terakhir".
const driverLastSeen = new Map();
// driverId -> { driverId, coords, online, socketId, car, lastSeen }

function now() {
  return new Date().toISOString();
}

function log(...args) {
  try {
    console.log(`[socket] ${now()} -`, ...args);
  } catch {}
}

// optional: periodic debug log every 10s
setInterval(() => {
  try {
    log("periodic counts", {
      drivers: onlineDrivers.size,
      customers: onlineCustomers.size,
      rides: rides.size,
      users: userSockets.size,
    });
  } catch {}
}, 10 * 1000);

// JANITOR: cegah pertumbuhan memori tanpa batas pada proses long-running.
// State in-memory (messages/qrRides/driverLastSeen) tak pernah dibersihkan
// sendiri, jadi entri basi dibuang berkala.
const STALE_MS = {
  messageRoom: 6 * 60 * 60 * 1000, // 6 jam tanpa pesan baru -> buang room
  qrRide: 2 * 60 * 60 * 1000, // 2 jam sesi QR non-aktif -> buang
  driverLastSeen: 24 * 60 * 60 * 1000, // 24 jam driver offline -> buang
};
setInterval(() => {
  try {
    const nowMs = Date.now();
    let purged = 0;

    for (const [roomId, list] of messages.entries()) {
      // Jangan buang histori chat milik ride / sesi QR yang MASIH aktif
      // (room chat memakai rideId: chat:join(rideId) / chat:send{roomId:rideId}).
      // Hanya room non-aktif & basi yang dibuang.
      if (rides.has(roomId) || qrRides.has(roomId)) continue;
      const last = list[list.length - 1];
      const ts = last ? Date.parse(last.createdAt) : 0;
      if (!list.length || nowMs - ts > STALE_MS.messageRoom) {
        messages.delete(roomId);
        purged++;
      }
    }

    for (const [rideId, qr] of qrRides.entries()) {
      const ts = Date.parse(qr.updatedAt || qr.createdAt || 0) || 0;
      if (qr.status !== "active" && nowMs - ts > STALE_MS.qrRide) {
        qrRides.delete(rideId);
        purged++;
      }
    }

    for (const [id, d] of driverLastSeen.entries()) {
      const ts = Date.parse(d.lastSeen || 0) || 0;
      if (!d.online && nowMs - ts > STALE_MS.driverLastSeen) {
        driverLastSeen.delete(id);
        purged++;
      }
    }

    if (purged) log("janitor purged stale entries", { purged });
  } catch (e) {
    log("janitor error", e?.message ?? e);
  }
}, 5 * 60 * 1000);

module.exports = (io) => {
  // helper: broadcast daftar driver ke semua client.
  // DI-THROTTLE (leading + trailing, maks 1x / 1.5s) supaya saat banyak driver
  // online, burst "driver:location" tidak memicu io.emit beruntun ke SEMUA
  // client (mencegah flood O(driver x client) saat pengguna membludak).
  let _driverListTimer = null;
  let _driverListPending = false;
  function _emitDriverListNow() {
    try {
      io.emit("driver:list", Array.from(onlineDrivers.values()));
    } catch (e) {
      log("emitDriverList error", e?.message ?? e);
    }
  }
  function emitDriverList() {
    if (_driverListTimer) {
      _driverListPending = true;
      return;
    }
    _emitDriverListNow();
    _driverListTimer = setTimeout(() => {
      _driverListTimer = null;
      if (_driverListPending) {
        _driverListPending = false;
        emitDriverList();
      }
    }, 1500);
  }

  // normalize coords helper
  function normalizeCoords(coords) {
    if (!coords) return null;
    const lat = Number(coords.lat ?? coords.latitude ?? coords.latitud ?? NaN);
    const lng = Number(coords.lng ?? coords.longitude ?? coords.long ?? NaN);
    if (!isFinite(lat) || !isFinite(lng)) return null;
    return { lat, lng };
  }

  // Catat lokasi/presence driver. Disimpan walau driver disconnect (online:false)
  // sehingga lokasi terakhir driver yang tidak terhubung tetap bisa dibaca.
  function updateDriverLastSeen(driverId, coords, extra = {}) {
    if (driverId == null) return;
    const id = String(driverId);
    const prev = driverLastSeen.get(id) || {};
    const c = coords ? normalizeCoords(coords) : null;
    driverLastSeen.set(id, {
      driverId: id,
      coords: c || prev.coords || null,
      online: extra.online != null ? extra.online : prev.online ?? true,
      socketId: extra.socketId ?? prev.socketId ?? null,
      car: extra.car ?? prev.car ?? null,
      lastSeen: now(),
    });
  }

  // Tandai driver offline tapi PERTAHANKAN lokasi terakhirnya.
  function markDriverOffline(driverId) {
    if (driverId == null) return null;
    const id = String(driverId);
    const prev = driverLastSeen.get(id);
    if (!prev) return null;
    const updated = { ...prev, online: false, socketId: null, lastSeen: now() };
    driverLastSeen.set(id, updated);
    return updated;
  }

  // --- CHAT helper functions ---
  function registerSocketForUser(userId, socketId) {
    if (!userId) return;
    if (!userSockets.has(userId)) userSockets.set(userId, new Set());
    userSockets.get(userId).add(socketId);
  }

  function unregisterSocketForUser(userId, socketId) {
    if (!userId) return;
    const set = userSockets.get(userId);
    if (!set) return;
    set.delete(socketId);
    if (set.size === 0) userSockets.delete(String(userId));
  }

  function getSocketIdsForUser(userId) {
    return Array.from(userSockets.get(userId) || []);
  }

  // --- SUPPORT CHAT helpers ---
  function serializeSession(s) {
    return {
      sessionId: s.sessionId,
      userId: s.userId,
      role: s.role,
      name: s.name,
      status: s.status,
      adminId: s.adminId ?? null,
      createdAt: s.createdAt,
      unread: s.messages.filter((m) => m.role !== "admin").length,
      lastText: s.messages.length
        ? s.messages[s.messages.length - 1].text
        : null,
      lastAt: s.messages.length
        ? s.messages[s.messages.length - 1].createdAt
        : s.createdAt,
    };
  }
  // Kirim antrian sesi support terkini ke semua admin yang online.
  function emitSupportQueue() {
    try {
      const list = Array.from(supportSessions.values()).map(serializeSession);
      for (const sid of supportAdmins) {
        io.to(sid).emit("support:queue", { ok: true, sessions: list });
      }
    } catch (e) {
      log("emitSupportQueue error", e?.message ?? e);
    }
  }

  // Batalkan timer "akhiri panggilan" yang tertunda untuk user — dipanggil saat
  // user reconnect/registrasi ulang supaya panggilan aktif tidak ikut batal
  // hanya karena putus sesaat saat panggilan diangkat.
  function clearPendingCallEnd(userId) {
    if (userId == null) return;
    const uid = String(userId);
    for (const call of activeCalls.values()) {
      const isParty =
        String(call.fromUserId) === uid || String(call.toUserId) === uid;
      if (isParty && call._endTimer) {
        clearTimeout(call._endTimer);
        call._endTimer = null;
        log("clearPendingCallEnd - reconnect, panggilan dipertahankan", {
          callId: call.callId,
          userId: uid,
        });
      }
    }
  }

  function persistMessage(msg) {
    const list = messages.get(msg.roomId) || [];
    list.push(msg);
    // optional: keep only last N per room to bound memory
    const MAX_PER_ROOM = 1000;
    if (list.length > MAX_PER_ROOM) list.splice(0, list.length - MAX_PER_ROOM);
    messages.set(msg.roomId, list);
    return msg;
  }

  io.on("connection", (socket) => {
    log("connection established", {
      socketId: socket.id,
      handshake: socket.handshake?.auth ?? null,
    });

    // if client provided role in handshake.auth, capture it
    try {
      const auth = socket.handshake?.auth;
      if (auth && auth.role === "customer") {
        onlineCustomers.set(socket.id, {
          socketId: socket.id,
          lastSeen: now(),
          meta: auth,
        });
        log("connection identified as customer via handshake.auth", {
          socketId: socket.id,
          meta: auth,
        });
      }

      // auto-register userId to userSockets if provided in handshake
      if (auth && (auth.userId || auth.id)) {
        const userId = auth.userId ?? auth.id;
        socket.userId = userId;
        registerSocketForUser(userId, socket.id);
        log("auto-registered user via handshake", {
          userId,
          socketId: socket.id,
        });
      }
    } catch (e) {
      // ignore
    }

    // CUSOMER START CALL
    socket.on("call:invite", ({ toUserId, roomId }, ack) => {
      try {
        if (!socket.userId || !toUserId || !roomId) {
          return ack?.({ ok: false, reason: "invalid_payload" });
        }

        const callId = `call_${Date.now()}_${Math.random()
          .toString(36)
          .slice(2, 8)}`;

        const call = {
          callId,
          fromUserId: socket.userId,
          toUserId,
          roomId,
          status: "ringing",
          createdAt: now(),
        };

        activeCalls.set(callId, call);

        // kirim ke semua socket target user (driver)
        const targetSockets = getSocketIdsForUser(String(toUserId));
        targetSockets.forEach((sid) => {
          io.to(sid).emit("call:incoming", {
            callId,
            fromUserId: socket.userId,
            roomId,
          });
        });

        log("call:invite sent", call);

        ack?.({ ok: true, callId });
      } catch (e) {
        log("call:invite error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // DRIVER ACCEPT CALL
    socket.on("call:accept", ({ callId }, ack) => {
      try {
        const call = activeCalls.get(callId);
        if (!call) {
          return ack?.({ ok: false, reason: "call_not_found" });
        }

        if (String(call.toUserId) !== String(socket.userId)) {
          return ack?.({ ok: false, reason: "not_authorized" });
        }

        call.status = "accepted";
        activeCalls.set(callId, call);

        // notify caller
        const callerSockets = getSocketIdsForUser(String(call.fromUserId));
        callerSockets.forEach((sid) => {
          io.to(sid).emit("call:accepted", {
            callId,
            roomId: call.roomId,
          });
        });

        log("call accepted", call);

        ack?.({ ok: true });
      } catch (e) {
        log("call:accept error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // DRIVER REJECT CALL
    socket.on("call:reject", ({ callId }, ack) => {
      try {
        const call = activeCalls.get(callId);
        if (!call) {
          return ack?.({ ok: false, reason: "call_not_found" });
        }

        call.status = "rejected";
        activeCalls.set(callId, call);

        const callerSockets = getSocketIdsForUser(String(call.fromUserId));
        callerSockets.forEach((sid) => {
          io.to(sid).emit("call:rejected", { callId });
        });

        activeCalls.delete(callId);

        log("call rejected", call);

        ack?.({ ok: true });
      } catch (e) {
        log("call:reject error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // END CALL
    socket.on("call:end", ({ callId }, ack) => {
      try {
        const call = activeCalls.get(callId);
        if (!call) return;

        const targets = [
          ...getSocketIdsForUser(String(call.fromUserId)),
          ...getSocketIdsForUser(String(call.toUserId)),
        ];

        targets.forEach((sid) => {
          io.to(sid).emit("call:ended", { callId });
        });

        activeCalls.delete(callId);

        log("call ended", call);

        ack?.({ ok: true });
      } catch (e) {
        log("call:end error", e?.message ?? e);
        ack?.({ ok: false });
      }
    });

    // CALLER CANCELS (saat masih ringing) atau TIMEOUT (tak diangkat)
    // -> beri tahu kedua pihak agar UI panggilan tertutup, lalu bersihkan.
    const cancelCall = ({ callId } = {}, ack, reason) => {
      try {
        const call = activeCalls.get(callId);
        if (!call) return ack?.({ ok: false, reason: "call_not_found" });

        const targets = [
          ...getSocketIdsForUser(String(call.fromUserId)),
          ...getSocketIdsForUser(String(call.toUserId)),
        ];
        targets.forEach((sid) => {
          io.to(sid).emit("call:canceled", { callId, reason });
        });

        activeCalls.delete(callId);
        log("call canceled", { callId, reason });
        ack?.({ ok: true });
      } catch (e) {
        log("call:cancel error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    };

    socket.on("call:cancel", (payload, ack) =>
      cancelCall(payload, ack, "canceled"),
    );
    socket.on("call:timeout", (payload, ack) =>
      cancelCall(payload, ack, "timeout"),
    );

    // --- CHAT: optional explicit register (client can emit after connect) ---
    socket.on("chat:register", ({ userId, role }, ack) => {
      try {
        if (!userId) {
          return ack?.({ ok: false, message: "userId required" });
        }

        // simpan ke socket memory
        socket.userId = String(userId);
        socket.role = role || "user";

        registerSocketForUser(socket.userId, socket.id);
        // Jika user reconnect saat masih ada panggilan aktif (mis. socket sempat
        // putus ketika panggilan diangkat), batalkan rencana pengakhiran call.
        clearPendingCallEnd(socket.userId);

        // 🔑 BUAT STREAM TOKEN
        const streamToken = createStreamToken(socket.userId);

        log("chat:register success", {
          userId: socket.userId,
          role: socket.role,
          socketId: socket.id,
        });

        // kirim ke client
        ack?.({
          ok: true,
          stream: {
            userId: socket.userId,
            token: streamToken,
            apiKey: process.env.STREAM_API_KEY,
          },
        });
      } catch (err) {
        log("chat:register error", err.message);
        ack?.({ ok: false, message: err.message });
      }
    });

    // --- customer set profile (existing) ---
    socket.on("customer:profile", (payload, ack) => {
      try {
        const { id, ...profile } = payload || {};

        const prev = onlineCustomers.get(socket.id) || {};
        const meta = { ...(prev.meta || {}), id, profile };
        onlineCustomers.set(socket.id, {
          socketId: socket.id,
          lastSeen: now(),
          meta,
        });

        // if payload includes id, register userSockets for chat lookup
        if (id) {
          socket.userId = id;
          registerSocketForUser(String(id), socket.id);
        }

        log("customer:profile set", { socketId: socket.id, id, profile });

        // broadcast to all drivers (or to room if you prefer)
        // send flat { id, profile } for discovery and also nested to rooms when booking
        io.emit("customer:profile:update", { id, profile });

        if (typeof ack === "function") ack({ ok: true });
      } catch (e) {
        log("customer:profile error", e?.message ?? e);
        if (typeof ack === "function")
          ack({ ok: false, reason: "internal_error" });
      }
    });

    // customer offline (existing)
    socket.on("customer:offline", ({ userId }, ack) => {
      try {
        if (!userId) return ack?.({ ok: true });

        for (const [sId, info] of onlineCustomers.entries()) {
          const metaId = info?.meta?.id ?? info?.meta?.customerId;
          if (String(metaId) === String(userId)) {
            unregisterSocketForUser(String(userId), sId);
            onlineCustomers.delete(sId);
          }
        }

        ack?.({ ok: true });
      } catch (e) {
        ack?.({ ok: false });
      }
    });

    // driver declare online (existing) - also register for chat mapping
    socket.on("driver:online", ({ id, coords, car } = {}) => {
      try {
        const entry = {
          id,
          status: "online",
          coords: normalizeCoords(coords) || null,
          car_information: car || null,
          socketId: socket.id,
        };
        onlineDrivers.set(id, entry);
        socketIdToDriverId.set(socket.id, id);
        updateDriverLastSeen(id, entry.coords, {
          online: true,
          socketId: socket.id,
          car: car || null,
        });

        // register mapping for chat
        if (id) {
          socket.userId = id;
          registerSocketForUser(String(id), socket.id);
          clearPendingCallEnd(id);
        }

        log("driver:online", {
          driverId: id,
          coords: entry.coords,
          socketId: socket.id,
          totalOnlineDrivers: onlineDrivers.size,
        });
        emitDriverList();
      } catch (e) {
        log("driver:online error", e?.message ?? e);
      }
    });

    // driver go offline (existing) - also unregister chat mapping
    socket.on("driver:offline", ({ id } = {}) => {
      try {
        const removed = onlineDrivers.delete(id);
        for (const [sId, dId] of socketIdToDriverId.entries()) {
          if (dId === id) socketIdToDriverId.delete(sId);
        }

        // unregister from userSockets
        if (id) unregisterSocketForUser(String(id), socket.id);

        // Tandai offline tapi simpan lokasi terakhir + beritahu pemantau.
        const last = markDriverOffline(id);
        if (id) {
          io.emit("driver:disconnected", {
            driverId: String(id),
            reason: "offline",
            coords: last?.coords ?? null,
            lastSeen: last?.lastSeen ?? now(),
          });
        }

        log("driver:offline", {
          driverId: id,
          removed,
          totalOnlineDrivers: onlineDrivers.size,
        });
        emitDriverList();
      } catch (e) {
        log("driver:offline error", e?.message ?? e);
      }
    });

    // driver location update (existing)
    socket.on("driver:location", ({ rideId, id, coords } = {}) => {
      try {
        const normalized = normalizeCoords(coords);
        // Selalu catat lokasi terakhir driver (termasuk driver mode QR yang
        // tidak ada di onlineDrivers) supaya presence/last-location akurat.
        if (id != null && normalized) {
          updateDriverLastSeen(id, normalized, {
            online: true,
            socketId: socket.id,
          });
        }
        const d = onlineDrivers.get(id);
        if (d) {
          const updated = {
            ...d,
            coords: normalized || d.coords,
            socketId: socket.id,
          };
          onlineDrivers.set(id, updated);
          socketIdToDriverId.set(socket.id, id); // refresh mapping
          log("driver:location", {
            driverId: id,
            rideId,
            coords: normalized,
            totalOnlineDrivers: onlineDrivers.size,
          });

          if (rideId) {
            io.to(rideId).emit("location:update", {
              role: "driver",
              id,
              coords: normalized,
              rideId,
            });
          } else {
            emitDriverList();
          }
        } else {
          log("driver:location - driver not found", { id });
        }
      } catch (e) {
        log("driver:location error", e?.message ?? e);
      }
    });

    // customer location update (existing)
    socket.on("customer:location", ({ rideId, id, coords, ...rest } = {}) => {
      try {
        const normalized = normalizeCoords(coords);

        // update or create onlineCustomers entry and store lastCoords
        const prev = onlineCustomers.get(socket.id) || {};
        const meta = {
          ...(prev.meta || {}),
          id: id ?? prev.meta?.id ?? null,
          lastCoords: normalized,
          ...rest,
        };
        onlineCustomers.set(socket.id, {
          socketId: socket.id,
          lastSeen: now(),
          meta,
        });

        // ensure chat mapping if id provided
        if (id) {
          socket.userId = id;
          registerSocketForUser(String(id), socket.id);
        }

        log("customer:location received", {
          socketId: socket.id,
          customerId: id,
          rideId,
          coords: normalized,
          ...rest,
        });

        // persist snapshot into ride record if ride exists
        if (rideId && rides.has(rideId)) {
          const r = rides.get(rideId);
          r.customerLocation = normalized;
          rides.set(rideId, r);
        }

        // emit to room with explicit role and normalized coords
        if (rideId) {
          io.to(rideId).emit("location:update", {
            role: "customer",
            id,
            coords: normalized,
            rideId,
          });
        } else {
          log("customer:location - missing rideId", { customerId: id });
        }
      } catch (e) {
        log("customer:location error", e?.message ?? e);
      }
    });

    // client requests snapshot of drivers (existing)
    socket.on("drivers:request", (payload = {}) => {
      try {
        const prev = onlineCustomers.get(socket.id) || {};

        // Merge meta: pertahankan data lama, timpa dengan payload baru jika ada
        const updatedMeta = {
          ...(prev.meta || {}),
          ...(payload || {}),
        };

        onlineCustomers.set(socket.id, {
          socketId: socket.id,
          lastSeen: now(),
          meta: updatedMeta,
        });

        log("drivers:request - meta merged", {
          socketId: socket.id,
          meta: updatedMeta,
        });
        socket.emit("driver:list", Array.from(onlineDrivers.values()));
      } catch (e) {
        log("drivers:request error", e?.message ?? e);
      }
    });

    // ------------------------
    // --- CHAT: core events ---
    // ------------------------

    // join chat room
    socket.on("chat:join", (roomId, ack) => {
      try {
        if (!roomId) return ack?.({ ok: false, reason: "invalid_room" });
        socket.join(roomId);
        log("chat:join", {
          socketId: socket.id,
          userId: socket.userId,
          roomId,
        });
        ack?.({ ok: true });
      } catch (e) {
        log("chat:join error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // send message
    socket.on("chat:send", (payload = {}, ack) => {
      try {
        const { roomId, to, text, attachments } = payload || {};
        if (!roomId || (!text && !attachments)) {
          return ack?.({ ok: false, reason: "invalid_payload" });
        }

        const msg = {
          id: `${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
          roomId,
          from: socket.userId ?? null,
          to: Array.isArray(to) ? to : to ? [to] : [],
          text: text ?? null,
          attachments: attachments ?? null,
          createdAt: now(),
          status: "sent",
        };

        // persist (in-memory)
        persistMessage(msg);

        // emit to room
        io.to(roomId).emit("chat:message", msg);

        // fallback: also emit directly to recipient sockets (by userId)
        (msg.to || []).forEach((uid) => {
          const sids = getSocketIdsForUser(String(uid));
          sids.forEach((sid) => {
            try {
              io.to(sid).emit("chat:message", msg);
            } catch (e) {
              // ignore per-socket emit errors
            }
          });
        });

        log("chat:send", {
          roomId: msg.roomId,
          from: msg.from,
          to: msg.to,
          id: msg.id,
        });

        ack?.({ ok: true, messageId: msg.id });
      } catch (e) {
        log("chat:send error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // chat history (paginasi: `before` = id pesan ATAU createdAt ISO)
    socket.on("chat:history", ({ roomId, limit = 50, before } = {}, ack) => {
      try {
        if (!roomId) return ack?.({ ok: false, reason: "invalid_room" });
        const list = messages.get(roomId) || [];

        let slice = list;
        if (before != null) {
          // `before` bisa berupa id pesan ATAU timestamp ISO.
          let idx = list.findIndex((m) => m.id === before);

          // Fallback timestamp HANYA bila `before` benar-benar tampak seperti
          // tanggal ISO — BUKAN id pesan (id = `${Date.now()}_${rand}`, selalu
          // mengandung '_'). Tanpa cek ini, id yang tak ditemukan akan salah
          // dibandingkan sebagai tanggal ("2026-..." >= "1719_..." -> true di
          // indeks 0) sehingga mengembalikan halaman KOSONG.
          const looksLikeTimestamp =
            typeof before === "string" &&
            !before.includes("_") &&
            !Number.isNaN(Date.parse(before));
          if (idx === -1 && looksLikeTimestamp) {
            const beforeMs = Date.parse(before);
            idx = list.findIndex((m) => Date.parse(m.createdAt) >= beforeMs);
          }

          // idx tak ditemukan (cursor asing) -> kembalikan halaman terakhir.
          slice = idx > 0 ? list.slice(0, idx) : idx === 0 ? [] : list;
        }

        const result = slice.slice(-limit);
        ack?.({ ok: true, messages: result });
      } catch (e) {
        log("chat:history error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // typing indicator
    socket.on("chat:typing", ({ roomId, isTyping } = {}) => {
      try {
        if (!roomId) return;
        socket.to(roomId).emit("chat:typing", {
          roomId,
          from: socket.userId,
          isTyping: !!isTyping,
        });
      } catch (e) {
        log("chat:typing error", e?.message ?? e);
      }
    });

    // read receipt
    socket.on("chat:read", ({ roomId, messageIds = [] } = {}) => {
      try {
        if (!roomId || !Array.isArray(messageIds)) return;
        const list = messages.get(roomId) || [];
        for (const m of list) {
          if (messageIds.includes(m.id)) m.status = "read";
        }
        io.to(roomId).emit("chat:read", {
          roomId,
          reader: socket.userId,
          messageIds,
        });
      } catch (e) {
        log("chat:read error", e?.message ?? e);
      }
    });

    // ===========================================================
    // SUPPORT CHAT — live chat user (customer/driver) <-> admin.
    // Alur: user "support:request" -> antri -> admin "support:accept"
    // -> chat via "support:message" -> "support:end" (sesi & pesan dihapus).
    // ===========================================================

    // Admin panel online: daftar untuk menerima antrian + dapat snapshot awal.
    socket.on("support:admin:join", (_payload, ack) => {
      try {
        supportAdmins.add(socket.id);
        socket.isSupportAdmin = true;
        const list = Array.from(supportSessions.values()).map(serializeSession);
        log("support:admin:join", {
          socketId: socket.id,
          sessions: list.length,
        });
        if (typeof ack === "function") ack({ ok: true, sessions: list });
        else socket.emit("support:queue", { ok: true, sessions: list });
      } catch (e) {
        log("support:admin:join error", e?.message ?? e);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // Admin keluar dari panel.
    socket.on("support:admin:leave", () => {
      supportAdmins.delete(socket.id);
      socket.isSupportAdmin = false;
    });

    // User (customer/driver) mengajukan sesi chat. Satu user = satu sesi aktif.
    socket.on("support:request", ({ userId, role, name } = {}, ack) => {
      try {
        if (!userId) return ack?.({ ok: false, reason: "invalid_payload" });

        let session = null;
        for (const s of supportSessions.values()) {
          if (String(s.userId) === String(userId) && s.status !== "ended") {
            session = s;
            break;
          }
        }
        if (!session) {
          session = {
            sessionId: `sup_${Date.now()}_${Math.random()
              .toString(36)
              .slice(2, 7)}`,
            userId: String(userId),
            role: role === "driver" ? "driver" : "customer",
            name: name || "Pengguna",
            status: "waiting",
            adminId: null,
            createdAt: now(),
            messages: [],
          };
          supportSessions.set(session.sessionId, session);
        }

        socket.userId = String(userId);
        registerSocketForUser(String(userId), socket.id);
        socket.join(session.sessionId);
        socket.supportSessionId = session.sessionId;

        log("support:request", {
          sessionId: session.sessionId,
          role: session.role,
          status: session.status,
        });
        emitSupportQueue();
        ack?.({
          ok: true,
          sessionId: session.sessionId,
          status: session.status,
          adminId: session.adminId ?? null,
          messages: session.messages,
        });
      } catch (e) {
        log("support:request error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Admin menerima/melayani sebuah sesi (memilih dari antrian).
    socket.on("support:accept", ({ sessionId, adminId } = {}, ack) => {
      try {
        const s = supportSessions.get(sessionId);
        if (!s) return ack?.({ ok: false, reason: "not_found" });

        s.status = "active";
        s.adminId = adminId ?? "admin";
        supportSessions.set(sessionId, s);
        socket.join(sessionId);

        getSocketIdsForUser(String(s.userId)).forEach((sid) =>
          io.to(sid).emit("support:accepted", {
            sessionId,
            adminId: s.adminId,
          }),
        );
        emitSupportQueue();
        log("support:accept", { sessionId, adminId: s.adminId });
        ack?.({ ok: true, session: serializeSession(s), messages: s.messages });
      } catch (e) {
        log("support:accept error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Kirim pesan dalam sesi (dari user ATAU admin).
    socket.on("support:message", ({ sessionId, from, role, text } = {}, ack) => {
      try {
        const s = supportSessions.get(sessionId);
        if (!s || !text) return ack?.({ ok: false, reason: "invalid_payload" });

        const msg = {
          id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          sessionId,
          from: from ?? null,
          role: role === "admin" ? "admin" : "user",
          text,
          createdAt: now(),
        };
        s.messages.push(msg);
        if (s.messages.length > 500) {
          s.messages.splice(0, s.messages.length - 500);
        }
        supportSessions.set(sessionId, s);

        io.to(sessionId).emit("support:message", msg);
        emitSupportQueue();
        ack?.({ ok: true, messageId: msg.id });
      } catch (e) {
        log("support:message error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Indikator mengetik (opsional).
    socket.on("support:typing", ({ sessionId, role, isTyping } = {}) => {
      try {
        if (!sessionId) return;
        socket.to(sessionId).emit("support:typing", {
          sessionId,
          role,
          isTyping: !!isTyping,
        });
      } catch (e) {
        log("support:typing error", e?.message ?? e);
      }
    });

    // Akhiri sesi -> beri tahu kedua pihak, lalu HAPUS sesi + semua pesan.
    socket.on("support:end", ({ sessionId, by } = {}, ack) => {
      try {
        const s = supportSessions.get(sessionId);
        if (!s) return ack?.({ ok: true });
        io.to(sessionId).emit("support:ended", { sessionId, by: by ?? null });
        supportSessions.delete(sessionId);
        emitSupportQueue();
        log("support:end", { sessionId, by });
        ack?.({ ok: true });
      } catch (e) {
        log("support:end error", e?.message ?? e);
        ack?.({ ok: false });
      }
    });

    // ------------------------
    // --- existing ride events continue ---
    // ------------------------

    // Driver secara manual klik "Terima" di HP-nya
    socket.on("ride:accept", ({ rideId }, ack) => {
      try {
        const r = rides.get(rideId);
        if (!r) return ack?.({ ok: false, reason: "ride_not_found" });

        r.status = "accepted"; // Ubah status dari booked ke accepted
        rides.set(rideId, r);

        // Beritahu PENUMPANG + bawa data lengkap (lokasi/tujuan/profil) supaya
        // layar perjalanan driver TIDAK perlu menunggu snapshot lambat.
        io.to(rideId).emit("ride:status", {
          rideId,
          status: "accepted",
          driverId: r.driverId,
          driver: r.driver ?? null,
          destination: r.destination ?? null,
          customerLocation: r.customerLocation ?? null,
          customer: r.customer ?? null,
        });

        log("ride:accepted by driver", { rideId, driverId: r.driverId });
        ack?.({ ok: true });
      } catch (e) {
        ack?.({ ok: false });
      }
    });

    // Ambil snapshot 1 ride dengan cepat (pengganti debug:state yang berat),
    // dipakai layar perjalanan driver untuk init data customer secepatnya.
    socket.on("ride:get", ({ rideId } = {}, ack) => {
      try {
        const r = rideId ? rides.get(rideId) : null;
        if (typeof ack === "function") ack({ ok: !!r, ride: r ?? null });
      } catch (e) {
        log("ride:get error", e?.message ?? e);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // customer book driver (with simple in-memory lock)
    socket.on(
      "ride:book",
      ({ rideId, location_if_needed, customerId, preferredDriverId, destination, fare } = {}) => {
        try {
          const driver = onlineDrivers.get(preferredDriverId);

          // Mencari data customer yang lebih akurat berdasarkan ID, bukan cuma socket
          const customerSnapshot = Array.from(onlineCustomers.values()).find(
            (c) => {
              const metaId = c?.meta?.id ?? c?.meta?.customerId;
              return metaId != null && String(metaId) === String(customerId);
            },
          );

          if (!driver || driver.status !== "online") {
            return socket.emit("ride:booked", {
              ok: false,
              reason: "driver_unavailable",
            });
          }

          // Lock driver
          const lockedDriver = { ...driver, status: "booked" };
          onlineDrivers.set(preferredDriverId, lockedDriver);

          // Siapkan Profile Payload secara lengkap

          let customerLocation = customerSnapshot?.meta?.lastCoords ?? null;
          if (!customerLocation) customerLocation = location_if_needed; 

          // Simpan ke memory rides
          rides.set(rideId, {
            rideId,
            driverId: preferredDriverId,
            status: "booked",
            destination: destination || null,
            customerLocation,
            driver: lockedDriver,
            customer: { id: customerId, ...customerSnapshot } || null, // Simpan profil di objek ride agar awet
          });

          // BUNGKUS SEMUA DATA DALAM SATU EMIT
          const fullPayload = {
            rideId,
            status: "booked",
            driver: lockedDriver,
            destination: destination || null,
            customerLocation,
            customer: { id: customerId, ...customerSnapshot } || null, // Driver langsung dapet data customer di sini
          };

          // Join room
          socket.join(rideId);
          const driverSocket = io.sockets.sockets.get(driver.socketId);
          if (driverSocket) driverSocket.join(rideId);

          // EMIT UTAMA: Ke seluruh room
          io.to(rideId).emit("ride:status", fullPayload);

          // FALLBACK EMIT: Langsung ke socket driver (antisipasi gagal join room)
          if (driver.socketId) {
            io.to(driver.socketId).emit("ride:status", {
              ...fullPayload,
              _fallback: true,
            });
          }

          // Konfirmasi ke pengirim (Customer)
          socket.emit("ride:booked", {
            ok: true,
            rideId,
            driver: lockedDriver,
          });
          emitDriverList();
        } catch (e) {
          log("ride:book error", e.message);
          socket.emit("ride:booked", { ok: false, reason: "internal_error" });
        }
      },
    );

    // ride:ongoing - driver (or server) signals ride moved to ongoing
    socket.on("ride:ongoing", (payload, ack) => {
      try {
        const { rideId } = payload || {};
        log("ride:ongoing received", { rideId, fromSocket: socket.id });
        const r = rides.get(rideId);
        if (!r) {
          log("ride:ongoing - ride not found", { rideId });
          if (typeof ack === "function")
            ack({ ok: false, reason: "ride_not_found" });
          return;
        }

        r.status = "ongoing";
        rides.set(rideId, r);

        const driverEntry = onlineDrivers.get(r.driverId);
        if (driverEntry) {
          onlineDrivers.set(r.driverId, { ...driverEntry, status: "booked" });
          emitDriverList();
        }

        io.to(rideId).emit("ride:status", {
          rideId,
          status: "ongoing",
          driverId: r.driverId,
          destination: r.destination || null,
        });

        log("ride:ongoing processed", { rideId, driverId: r.driverId });
        if (typeof ack === "function") ack({ ok: true, rideId });
      } catch (e) {
        log("ride:ongoing error", e?.message ?? e);
        if (typeof ack === "function")
          ack({ ok: false, reason: "internal_error" });
      }
    });

    // ride cancel (existing)
    socket.on("ride:cancel", (payload, ack) => {
      try {
        const { rideId } = payload || {};
        log("ride:cancel received", { rideId });
        const r = rides.get(rideId);
        if (r) {
          const driver = onlineDrivers.get(r.driverId);
          if (driver) {
            onlineDrivers.set(r.driverId, { ...driver, status: "online" });
            log("ride:cancel - driver unlocked", { driverId: r.driverId });
          }
          rides.delete(rideId);
          log("ride:cancel - ride removed", { rideId, totalRides: rides.size });

          io.to(rideId).emit("ride:status", {
            rideId,
            status: "canceled",
            driverId: r.driverId,
          });

          emitDriverList();

          if (typeof ack === "function") ack({ ok: true });
        } else {
          log("ride:cancel - ride not found", { rideId });
          if (typeof ack === "function")
            ack({ ok: false, reason: "ride_not_found" });
        }
      } catch (e) {
        log("ride:cancel error", e?.message ?? e);
        if (typeof ack === "function")
          ack({ ok: false, reason: "internal_error" });
      }
    });

    // ride arrived (existing)
    socket.on("ride:arrived", (payload, ack) => {
      try {
        const { rideId } = payload || {};
        log("ride:arrived received", { rideId, fromSocket: socket.id });
        const r = rides.get(rideId);
        if (!r) {
          log("ride:arrived - ride not found", { rideId });
          if (typeof ack === "function")
            ack({ ok: false, reason: "ride_not_found" });
          return;
        }

        r.status = "arrived";
        rides.set(rideId, r);

        io.to(rideId).emit("ride:status", {
          rideId,
          status: "arrived",
          driverId: r.driverId,
          destination: r.destination || null,
        });

        log("ride:arrived processed", { rideId, driverId: r.driverId });
        if (typeof ack === "function") ack({ ok: true, rideId });
      } catch (e) {
        log("ride:arrived error", e?.message ?? e);
        if (typeof ack === "function")
          ack({ ok: false, reason: "internal_error" });
      }
    });

    // ride complete (existing)
    socket.on("ride:complete", (payload, ack) => {
      try {
        const { rideId } = payload || {};
        log("ride:complete received", { rideId, fromSocket: socket.id });
        const r = rides.get(rideId);
        if (!r) {
          log("ride:complete - ride not found", { rideId });
          if (typeof ack === "function")
            ack({ ok: false, reason: "ride_not_found" });
          return;
        }

        r.status = "completed";
        rides.set(rideId, r);

        const driver = onlineDrivers.get(r.driverId);
        if (driver) {
          onlineDrivers.set(r.driverId, { ...driver, status: "online" });
          emitDriverList();
        }

        io.to(rideId).emit("ride:status", {
          rideId,
          status: "completed",
          driverId: r.driverId,
        });

        rides.delete(rideId);
        log("ride:complete processed and removed", { rideId });

        if (typeof ack === "function") ack({ ok: true, rideId });
      } catch (e) {
        log("ride:complete error", e?.message ?? e);
        if (typeof ack === "function")
          ack({ ok: false, reason: "internal_error" });
      }
    });

    // join ride room (existing)
    socket.on("join:ride", (rideId) => {
      try {
        socket.join(rideId);
        log("join:ride", { socketId: socket.id, rideId });
      } catch (e) {
        log("join:ride error", e?.message ?? e);
      }
    });

    // ===========================================================
    // QR-CODE RIDE — di-track di backend, signaling via socket (bukan Supabase).
    // Room = rideId (= "ride-" + driverId).
    // ===========================================================

    // Driver buka layar QR: umumkan ketersediaan + simpan profil utk handshake.
    socket.on("qr:driver:ready", ({ driverId, coords, car } = {}, ack) => {
      try {
        if (!driverId) return ack?.({ ok: false, reason: "invalid_payload" });
        const rideId = `ride-${driverId}`;
        socket.userId = String(driverId);
        registerSocketForUser(String(driverId), socket.id);
        socket.join(rideId);

        const prev = qrRides.get(rideId) || {};
        qrRides.set(rideId, {
          ...prev,
          rideId,
          driverId: String(driverId),
          driver: car ?? prev.driver ?? null,
          status: "free",
          createdAt: prev.createdAt ?? now(),
          updatedAt: now(),
        });
        updateDriverLastSeen(driverId, coords, {
          online: true,
          socketId: socket.id,
          car: car ?? null,
        });

        log("qr:driver:ready", { rideId });
        ack?.({ ok: true, rideId });
      } catch (e) {
        log("qr:driver:ready error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Customer scan QR driver: klaim sesi TANPA Supabase. Balas rideId + profil driver.
    socket.on("qr:scan", ({ driverId, customer } = {}, ack) => {
      try {
        if (!driverId) return ack?.({ ok: false, reason: "invalid_payload" });
        const rideId = `ride-${driverId}`;
        const entry = qrRides.get(rideId);

        if (!entry) return ack?.({ ok: false, reason: "driver_unavailable" });
        if (entry.status !== "free")
          return ack?.({ ok: false, reason: "expired" });

        const customerId = customer?.id ?? null;
        qrRides.set(rideId, {
          ...entry,
          customerId: customerId != null ? String(customerId) : null,
          customer: customer ?? null,
          status: "waiting_customer",
          updatedAt: now(),
        });
        if (customerId != null) {
          socket.userId = String(customerId);
          registerSocketForUser(String(customerId), socket.id);
        }
        socket.join(rideId);

        // Beritahu driver: penumpang sedang menentukan rute.
        socket.to(rideId).emit("qr:status", {
          rideId,
          status: "waiting_customer",
        });

        log("qr:scan claimed", { rideId, customerId });
        ack?.({ ok: true, rideId, driver: entry.driver ?? null });
      } catch (e) {
        log("qr:scan error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Update status QR (active/completed/dll) -> simpan state + relay ke room.
    socket.on("qr:status", (payload = {}, ack) => {
      try {
        const rideId = payload?.rideId;
        const status = payload?.status;
        if (!rideId) return ack?.({ ok: false, reason: "invalid_payload" });

        const prev = qrRides.get(rideId) || { rideId, createdAt: now() };
        const entry = {
          ...prev,
          rideId,
          status: status ?? prev.status,
          driverId: payload.driver_id ?? prev.driverId ?? null,
          customerId: payload.customer?.id ?? prev.customerId ?? null,
          driver: payload.driver ?? prev.driver ?? null,
          customer: payload.customer ?? prev.customer ?? null,
          destination: payload.posDestination ?? prev.destination ?? null,
          customerLocation:
            payload.posCustomer ?? prev.customerLocation ?? null,
          originText: payload.originText ?? prev.originText ?? null,
          destinationText:
            payload.destinationText ?? prev.destinationText ?? null,
          fare: payload.fare ?? prev.fare ?? null,
          distance: payload.distance ?? prev.distance ?? null,
          duration: payload.duration ?? prev.duration ?? null,
          updatedAt: now(),
        };

        if (status === "completed") qrRides.delete(rideId);
        else qrRides.set(rideId, entry);

        socket.to(rideId).emit("qr:status", payload);
        log("qr:status", { rideId, status });
        ack?.({ ok: true, rideId });
      } catch (e) {
        log("qr:status error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Pembatalan fase pra-perjalanan QR (driver tekan batal ATAU customer batal).
    // Beritahu pihak lain, lalu RESET sesi ke "free" supaya QR driver siap lagi.
    socket.on("qr:cancel", ({ rideId, by } = {}, ack) => {
      try {
        if (!rideId) return ack?.({ ok: false, reason: "invalid_payload" });
        const entry = qrRides.get(rideId);

        socket.to(rideId).emit("qr:status", {
          rideId,
          status: "canceled",
          by: by ?? null,
          reason: "canceled",
        });

        if (entry && entry.status !== "completed") {
          qrRides.set(rideId, {
            ...entry,
            status: "free",
            customerId: null,
            customer: null,
            destination: null,
            customerLocation: null,
            updatedAt: now(),
          });
        }

        log("qr:cancel", { rideId, by });
        ack?.({ ok: true, rideId });
      } catch (e) {
        log("qr:cancel error", e?.message ?? e);
        ack?.({ ok: false, reason: "internal_error" });
      }
    });

    // Lokasi selama perjalanan QR: track di backend (qrRide + lastSeen) + relay.
    socket.on("qr:location", ({ rideId, role, id, coords } = {}) => {
      try {
        const normalized = normalizeCoords(coords);
        if (!rideId || !normalized) return;
        const entry = qrRides.get(rideId);
        if (entry) {
          if (role === "driver") entry.driverLocation = normalized;
          else if (role === "customer") entry.customerLocation = normalized;
          entry.updatedAt = now();
          qrRides.set(rideId, entry);
        }
        if (role === "driver" && id != null) {
          updateDriverLastSeen(id, normalized, {
            online: true,
            socketId: socket.id,
          });
        }
        socket.to(rideId).emit("location:update", {
          role,
          id,
          coords: normalized,
          rideId,
        });
      } catch (e) {
        log("qr:location error", e?.message ?? e);
      }
    });

    // Query: lokasi terakhir semua driver (online & offline) utk pemantauan.
    socket.on("drivers:lastseen", (payload, ack) => {
      try {
        const list = Array.from(driverLastSeen.values());
        if (typeof ack === "function") ack({ ok: true, drivers: list });
        else
          socket.emit("drivers:lastseen:response", {
            ok: true,
            drivers: list,
          });
      } catch (e) {
        log("drivers:lastseen error", e?.message ?? e);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // debug: return current state snapshot (existing)
    socket.on("debug:state", () => {
      try {
        log("debug:state requested", { socketId: socket.id });
        socket.emit("debug:state:response", {
          onlineDrivers: Array.from(onlineDrivers.values()),
          rides: Array.from(rides.values()),
          onlineCustomers: Array.from(onlineCustomers.values()),
          userSockets: Array.from(userSockets.entries()).map(([u, s]) => [
            u,
            Array.from(s),
          ]),
          messagesCount: Array.from(messages.entries()).map(([r, list]) => ({
            roomId: r,
            count: list.length,
          })),
          qrRides: Array.from(qrRides.values()),
          driverLastSeen: Array.from(driverLastSeen.values()),
        });
      } catch (e) {
        log("debug:state error", e?.message ?? e);
      }
    });

    // debug: counts (drivers/customers) (existing)
    socket.on("debug:counts", (payload, ack) => {
      try {
        const driverCount = onlineDrivers.size;
        const customerCount = onlineCustomers.size;
        const result = {
          drivers: driverCount,
          customers: customerCount,
          users: userSockets.size,
        };

        log("debug:counts requested", { socketId: socket.id, result });

        socket.emit("debug:counts:response", result);
        if (typeof ack === "function") ack({ ok: true, ...result });
      } catch (e) {
        log("debug:counts error", e?.message ?? e);
        if (typeof ack === "function")
          ack({ ok: false, reason: "internal_error" });
      }
    });

    // optional: echo ride:status for quick client-side testing (remove in production)
    socket.on("__debug_echo_ride_status", (payload) => {
      try {
        log("__debug_echo_ride_status received", {
          socketId: socket.id,
          payload,
        });
        socket.emit("ride:status", { echoed: true, original: payload });
      } catch (e) {
        log("__debug_echo_ride_status error", e?.message ?? e);
      }
    });

    // disconnect handling (existing) - enhanced to cleanup userSockets
    socket.on("disconnect", () => {
      try {
        log("disconnect", { socketId: socket.id, userId: socket.userId });

        // --- SUPPORT CHAT cleanup ---
        // Admin keluar dari daftar penerima antrian.
        if (socket.isSupportAdmin) supportAdmins.delete(socket.id);
        // Sesi milik user yang benar-benar offline -> akhiri (ephemeral).
        if (socket.userId) {
          const supUid = String(socket.userId);
          const remainingSup = getSocketIdsForUser(supUid).filter(
            (x) => x !== socket.id,
          );
          if (remainingSup.length === 0) {
            for (const [sid, s] of Array.from(supportSessions.entries())) {
              if (String(s.userId) !== supUid) continue;
              io.to(sid).emit("support:ended", {
                sessionId: sid,
                by: "user_disconnect",
              });
              supportSessions.delete(sid);
              emitSupportQueue();
            }
          }
        }

        // Akhiri panggilan aktif yang melibatkan user ini — TAPI jangan agresif.
        // Putus sesaat (mis. socket.io reconnect saat WebRTC dinyalakan ketika
        // panggilan diangkat) TIDAK boleh langsung membatalkan panggilan yang
        // baru tersambung. Pakai cek "masih ada socket lain" + tenggang waktu.
        if (socket.userId) {
          const callUid = String(socket.userId);
          for (const [cId, call] of Array.from(activeCalls.entries())) {
            const isParty =
              String(call.fromUserId) === callUid ||
              String(call.toUserId) === callUid;
            if (!isParty) continue;

            // Masih ada koneksi lain milik user ini? Berarti belum benar-benar
            // offline (mis. socket lama putus, socket baru sudah masuk) -> skip.
            const remaining = getSocketIdsForUser(callUid).filter(
              (sid) => sid !== socket.id,
            );
            if (remaining.length > 0) continue;

            // Beri tenggang untuk reconnect sebelum benar-benar mengakhiri.
            // Dibatalkan oleh clearPendingCallEnd() jika user kembali.
            if (call._endTimer) continue;
            call._endTimer = setTimeout(() => {
              call._endTimer = null;
              if (!activeCalls.has(cId)) return;
              // Sudah reconnect dalam masa tenggang? jangan akhiri.
              if (getSocketIdsForUser(callUid).length > 0) return;
              const other =
                String(call.fromUserId) === callUid
                  ? call.toUserId
                  : call.fromUserId;
              getSocketIdsForUser(String(other)).forEach((sid) => {
                io.to(sid).emit("call:ended", {
                  callId: cId,
                  reason: "peer_disconnected",
                });
              });
              activeCalls.delete(cId);
              log("call ended after disconnect grace", {
                callId: cId,
                userId: callUid,
              });
            }, CALL_DISCONNECT_GRACE_MS);
          }
        }

        // --- Presence driver: tandai offline, simpan lokasi terakhir, notify ---
        // Termasuk driver mode QR (tidak ada di socketIdToDriverId, dikenali via userId).
        const discDriverId =
          socketIdToDriverId.get(socket.id) ??
          (socket.userId && driverLastSeen.has(String(socket.userId))
            ? String(socket.userId)
            : null);
        if (discDriverId) {
          const last = markDriverOffline(discDriverId);
          io.emit("driver:disconnected", {
            driverId: String(discDriverId),
            reason: "disconnected",
            coords: last?.coords ?? null,
            lastSeen: last?.lastSeen ?? now(),
          });
        }

        // --- Cleanup perjalanan QR saat disconnect ---
        if (socket.userId) {
          const uid = String(socket.userId);
          for (const [rId, qr] of Array.from(qrRides.entries())) {
            const isDriver = String(qr.driverId) === uid;
            const isCustomer = qr.customerId && String(qr.customerId) === uid;
            if (!isDriver && !isCustomer) continue;

            const inJourney = qr.status === "active";

            if (isDriver) {
              // DRIVER putus -> perjalanan otomatis BERAKHIR; beri tahu penumpang.
              socket.to(rId).emit("qr:status", {
                rideId: rId,
                status: "canceled",
                by: "driver",
                reason: "peer_disconnected",
              });
              if (qr.status !== "completed") qrRides.delete(rId);
            } else if (isCustomer) {
              if (inJourney) {
                // PENUMPANG putus SAAT PERJALANAN -> JANGAN akhiri perjalanan.
                // Driver tetap lanjut & lihat maps; data terakhir penumpang
                // (customerLocation) sudah tersimpan di qrRide. Tandai offline saja.
                qrRides.set(rId, {
                  ...qr,
                  customerOnline: false,
                  updatedAt: now(),
                });
              } else if (qr.status !== "completed") {
                // Penumpang keluar saat PRA-perjalanan -> reset sesi ke "free"
                // supaya QR driver kembali siap menerima penumpang baru.
                socket.to(rId).emit("qr:status", {
                  rideId: rId,
                  status: "canceled",
                  by: "customer",
                  reason: "peer_disconnected",
                });
                qrRides.set(rId, {
                  ...qr,
                  status: "free",
                  customerId: null,
                  customer: null,
                  destination: null,
                  customerLocation: null,
                  updatedAt: now(),
                });
              }
            }
          }
        }

        // cleanup chat mapping if present
        if (socket.userId) {
          unregisterSocketForUser(String(socket.userId), socket.id);
        } else {
          // Failsafe: Jika socket.userId hilang, cari manual di semua Set
          for (const [uid, set] of userSockets.entries()) {
            if (set.has(socket.id)) {
              unregisterSocketForUser(uid, socket.id);
            }
          }
        }

        // remove from onlineCustomers if present
        if (onlineCustomers.has(socket.id)) {
          onlineCustomers.delete(socket.id);
          log("customer disconnected and removed", {
            socketId: socket.id,
            totalCustomers: onlineCustomers.size,
          });
        }

        // ATURAN: penumpang (customer) yang terputus TIDAK mengakhiri perjalanan
        // — driver harus tetap melihat maps & lanjut dengan data terakhir.
        // Hanya DRIVER yang terputus yang mengakhiri ride (di-handle juga oleh
        // blok socketIdToDriverId di bawah). Jadi cek di sini KHUSUS driver.
        // (tidak ada aksi cancel di sini — dihapus untuk menghindari emit ganda
        // dengan blok socketIdToDriverId, yang bisa men-trigger rating/komentar
        // pembatalan 2x di sisi customer.)

        const driverId = socketIdToDriverId.get(socket.id);
        if (driverId) {
          // if driver was booked on a ride, remove ride and notify room
          const activeRide = Array.from(rides.values()).find(
            (r) => r.driverId === driverId,
          );
          if (activeRide) {
            rides.delete(activeRide.rideId);
            log("driver disconnected while booked - ride removed", {
              rideId: activeRide.rideId,
              driverId,
            });
            io.to(activeRide.rideId).emit("ride:status", {
              rideId: activeRide.rideId,
              status: "canceled",
              reason: "driver_disconnected",
            });
          }

          // remove driver from online list and reverse map
          onlineDrivers.delete(driverId);
          socketIdToDriverId.delete(socket.id);

          // also cleanup chat mapping
          unregisterSocketForUser(String(driverId), socket.id);

          log("driver disconnected and removed", {
            driverId,
            socketId: socket.id,
            totalOnlineDrivers: onlineDrivers.size,
          });

          // broadcast updated driver list
          emitDriverList();
        } else {
          // fallback: scan onlineDrivers if mapping missing
          for (const [id, d] of Array.from(onlineDrivers.entries())) {
            if (d.socketId === socket.id) {
              const activeRide = Array.from(rides.values()).find(
                (r) => r.driverId === id,
              );
              if (activeRide) {
                rides.delete(activeRide.rideId);
                log(
                  "driver disconnected while booked - ride removed (fallback)",
                  {
                    rideId: activeRide.rideId,
                    driverId: id,
                  },
                );
                io.to(activeRide.rideId).emit("ride:status", {
                  rideId: activeRide.rideId,
                  status: "canceled",
                  reason: "driver_disconnected",
                });
              }

              onlineDrivers.delete(id);
              socketIdToDriverId.delete(socket.id);
              unregisterSocketForUser(String(id), socket.id);

              log("driver disconnected and removed (fallback)",             {   driverId: id,
                socketId: socket.id,
                totalOnlineDrivers: onlineDrivers.size,
              });
              emitDriverList();
              break;
            }
          }
        }
      } catch (e) {
        log("disconnect handler error", e?.message ?? e);
      }
    });
  });
};
