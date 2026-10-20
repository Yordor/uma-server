const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8080", 10);
const MAX_TABLES_LIMIT = 16;
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function acceptKey(key) {
  return crypto.createHash("sha1").update(key + GUID).digest("base64");
}

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.from([0x80 | opcode, len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

class Conn {
  constructor(socket, onMessage, onClose) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.fragOp = 0;
    this.alive = true;
    this.onMessage = onMessage;
    this.onClose = onClose;
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.close());
    socket.on("error", () => this.close());
  }

  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    while (this.alive) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0];
      const b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0;
      const op = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (this.buf.length < 4) return;
        len = this.buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (this.buf.length < 10) return;
        len = Number(this.buf.readBigUInt64BE(2));
        off = 10;
      }
      if (len > 4 * 1024 * 1024) return this.close();
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.slice(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = this.buf.slice(off, off + 4);
        const out = Buffer.alloc(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3];
        payload = out;
      }
      this.buf = this.buf.slice(need);

      if (op === 0x8) {
        this.sendRaw(encodeFrame(0x8, Buffer.alloc(0)));
        return this.close();
      }
      if (op === 0x9) {
        this.sendRaw(encodeFrame(0xa, payload));
        continue;
      }
      if (op === 0xa) continue;

      if (op === 0x1 || op === 0x2) {
        this.fragOp = op;
        this.frags = [payload];
      } else if (op === 0x0) {
        this.frags.push(payload);
      }
      if (fin) {
        const full = Buffer.concat(this.frags);
        this.frags = [];
        if (this.fragOp === 0x1) this.onMessage(full.toString("utf8"));
      }
    }
  }

  sendRaw(b) {
    if (this.alive) this.socket.write(b);
  }

  send(text) {
    this.sendRaw(encodeFrame(0x1, Buffer.from(text, "utf8")));
  }

  close() {
    if (!this.alive) return;
    this.alive = false;
    try {
      this.socket.destroy();
    } catch (e) {}
    this.onClose();
  }
}

const clients = new Map();
const rooms = new Map();
let nextClientId = 1;
let nextTableId = 1;

function genCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  for (;;) {
    let c = "";
    for (let i = 0; i < 4; i++) c += chars[Math.floor(Math.random() * chars.length)];
    if (!rooms.has(c)) return c;
  }
}

function send(client, obj) {
  if (client && client.conn.alive) client.conn.send(JSON.stringify(obj));
}

function err(client, msg) {
  send(client, { t: "error", msg });
}

function tableOf(client) {
  const room = client.room ? rooms.get(client.room) : null;
  if (!room) return null;
  const id = client.tableId;
  if (!id) return null;
  return room.tables.get(id) || null;
}

function roomSummary(room) {
  return {
    code: room.code,
    name: room.name,
    host: clients.get(room.hostId) ? clients.get(room.hostId).name : "",
    players: room.members.size,
    tables: room.tables.size,
    maxTables: room.maxTables,
  };
}

function statusOf(client) {
  if (client.tableId) return client.seat >= 0 ? "playing" : "watching";
  if (client.queued) return "queue";
  return "idle";
}

function broadcastRoom(room) {
  const state = {
    t: "room_state",
    code: room.code,
    name: room.name,
    hostId: room.hostId,
    maxTables: room.maxTables,
    queue: room.queue.length,
    members: [...room.members].map((id) => {
      const c = clients.get(id);
      return { id, name: c.name, status: statusOf(c) };
    }),
    tables: [...room.tables.values()].map((tb) => ({
      id: tb.id,
      phase: tb.phase,
      players: tb.seats.map((id) => (id ? clients.get(id).name : "")),
      spectators: tb.spectators.size,
    })),
  };
  for (const id of room.members) send(clients.get(id), state);
}

function tableRecipients(tb, exceptId) {
  const out = [];
  for (const id of tb.seats) if (id && id !== exceptId) out.push(clients.get(id));
  for (const id of tb.spectators) if (id !== exceptId) out.push(clients.get(id));
  return out;
}

function tableBroadcast(tb, obj, exceptId) {
  for (const c of tableRecipients(tb, exceptId)) send(c, obj);
}

function leaveRoom(client) {
  const room = client.room ? rooms.get(client.room) : null;
  if (!room) return;
  leaveTable(client, true);
  removeFromQueue(client);
  room.members.delete(client.id);
  client.room = null;
  if (room.members.size === 0) {
    rooms.delete(room.code);
    return;
  }
  if (room.hostId === client.id) room.hostId = room.members.values().next().value;
  broadcastRoom(room);
}

function removeFromQueue(client) {
  const room = client.room ? rooms.get(client.room) : null;
  client.queued = false;
  if (!room) return;
  room.queue = room.queue.filter((id) => id !== client.id);
}

function leaveTable(client, silent) {
  const room = client.room ? rooms.get(client.room) : null;
  const tb = tableOf(client);
  if (!room || !tb) {
    client.tableId = 0;
    client.seat = -1;
    return;
  }
  if (client.seat >= 0) {
    if (tb.phase !== "done") {
      const other = tb.seats[1 - client.seat];
      tb.phase = "done";
      if (other) {
        tableBroadcast(tb, { t: "opponent_left", seat: client.seat }, client.id);
        tableBroadcast(tb, { t: "game_over", winner: 1 - client.seat, reason: "left" }, client.id);
      }
    }
    closeTable(room, tb);
  } else {
    tb.spectators.delete(client.id);
    client.tableId = 0;
    client.seat = -1;
    if (!silent) {
      send(client, { t: "table_left" });
      broadcastRoom(room);
    }
  }
}

function closeTable(room, tb) {
  for (const id of tb.seats) {
    if (!id) continue;
    const c = clients.get(id);
    if (!c) continue;
    c.tableId = 0;
    c.seat = -1;
    send(c, { t: "table_left" });
  }
  for (const id of tb.spectators) {
    const c = clients.get(id);
    if (!c) continue;
    c.tableId = 0;
    c.seat = -1;
    send(c, { t: "table_left" });
  }
  room.tables.delete(tb.id);
  tryMatch(room);
  broadcastRoom(room);
}

function tryMatch(room) {
  while (room.queue.length >= 2 && room.tables.size < room.maxTables) {
    const a = clients.get(room.queue.shift());
    const b = clients.get(room.queue.shift());
    if (!a || !b) continue;
    a.queued = false;
    b.queued = false;
    const tb = {
      id: nextTableId++,
      seats: [a.id, b.id],
      spectators: new Set(),
      phase: "ready",
      decks: [null, null],
      rps: [null, null],
      rpsWinner: -1,
      firstSeat: -1,
      seed: crypto.randomInt(1, 2147483647),
      log: [],
    };
    room.tables.set(tb.id, tb);
    a.tableId = tb.id;
    a.seat = 0;
    b.tableId = tb.id;
    b.seat = 1;
    send(a, { t: "match_found", tableId: tb.id, seat: 0, opponent: b.name, seed: tb.seed });
    send(b, { t: "match_found", tableId: tb.id, seat: 1, opponent: a.name, seed: tb.seed });
  }
  for (let i = 0; i < room.queue.length; i++) {
    const c = clients.get(room.queue[i]);
    send(c, { t: "queue_status", position: i + 1, tablesFull: room.tables.size >= room.maxTables });
  }
}

function rpsResolve(a, b) {
  if (a === b) return -1;
  return (a - b + 3) % 3 === 1 ? 0 : 1;
}

function startGame(tb) {
  tb.phase = "playing";
  tableBroadcast(
    tb,
    {
      t: "game_start",
      firstSeat: tb.firstSeat,
      seed: tb.seed,
      decks: tb.decks,
      names: tb.seats.map((id) => clients.get(id).name),
    },
    0
  );
}

function handle(client, m) {
  const room = client.room ? rooms.get(client.room) : null;
  switch (m.t) {
    case "hello": {
      client.name = String(m.name || "Player").slice(0, 20) || "Player";
      send(client, { t: "welcome", id: client.id, name: client.name });
      break;
    }
    case "list_rooms": {
      send(client, { t: "rooms", rooms: [...rooms.values()].map(roomSummary) });
      break;
    }
    case "create_room": {
      if (room) return err(client, "อยู่ในห้องแล้ว");
      const code = genCode();
      const r = {
        code,
        name: String(m.name || client.name + "'s Room").slice(0, 30),
        hostId: client.id,
        maxTables: Math.max(1, Math.min(MAX_TABLES_LIMIT, parseInt(m.maxTables || 2, 10) || 2)),
        members: new Set([client.id]),
        tables: new Map(),
        queue: [],
      };
      rooms.set(code, r);
      client.room = code;
      send(client, { t: "joined_room", code });
      broadcastRoom(r);
      break;
    }
    case "join_room": {
      if (room) return err(client, "อยู่ในห้องแล้ว");
      const r = rooms.get(String(m.code || "").toUpperCase());
      if (!r) return err(client, "ไม่พบห้อง");
      r.members.add(client.id);
      client.room = r.code;
      send(client, { t: "joined_room", code: r.code });
      broadcastRoom(r);
      break;
    }
    case "leave_room": {
      leaveRoom(client);
      send(client, { t: "left_room" });
      break;
    }
    case "set_tables": {
      if (!room) return;
      if (room.hostId !== client.id) return err(client, "เฉพาะเจ้าของห้อง");
      const n = Math.max(1, Math.min(MAX_TABLES_LIMIT, parseInt(m.n, 10) || 1));
      room.maxTables = Math.max(n, room.tables.size);
      tryMatch(room);
      broadcastRoom(room);
      break;
    }
    case "find_match": {
      if (!room) return err(client, "ยังไม่ได้เข้าห้อง");
      if (client.tableId || client.queued) return;
      client.queued = true;
      room.queue.push(client.id);
      tryMatch(room);
      broadcastRoom(room);
      break;
    }
    case "cancel_match": {
      if (!room) return;
      removeFromQueue(client);
      send(client, { t: "queue_cancelled" });
      broadcastRoom(room);
      break;
    }
    case "watch": {
      if (!room) return err(client, "ยังไม่ได้เข้าห้อง");
      if (client.tableId) return err(client, "อยู่ที่โต๊ะอยู่แล้ว");
      const tb = room.tables.get(parseInt(m.tableId, 10));
      if (!tb) return err(client, "ไม่พบโต๊ะ");
      removeFromQueue(client);
      tb.spectators.add(client.id);
      client.tableId = tb.id;
      client.seat = -1;
      send(client, {
        t: "watch_snapshot",
        tableId: tb.id,
        phase: tb.phase,
        names: tb.seats.map((id) => (id ? clients.get(id).name : "")),
        decks: tb.decks,
        firstSeat: tb.firstSeat,
        seed: tb.seed,
        log: tb.log,
      });
      broadcastRoom(room);
      break;
    }
    case "unwatch": {
      if (client.seat === -1) leaveTable(client, false);
      break;
    }
    case "ready": {
      const tb = tableOf(client);
      if (!tb || client.seat < 0 || tb.phase !== "ready") return;
      tb.decks[client.seat] = m.deck;
      if (tb.decks[0] && tb.decks[1]) {
        tb.phase = "rps";
        tb.rps = [null, null];
        tableBroadcast(tb, { t: "rps_start" }, 0);
        broadcastRoom(room);
      }
      break;
    }
    case "rps": {
      const tb = tableOf(client);
      if (!tb || client.seat < 0 || tb.phase !== "rps") return;
      const c = parseInt(m.choice, 10);
      if (!(c >= 0 && c <= 2)) return;
      if (tb.rps[client.seat] !== null) return;
      tb.rps[client.seat] = c;
      tableBroadcast(tb, { t: "rps_locked", seat: client.seat }, client.id);
      if (tb.rps[0] !== null && tb.rps[1] !== null) {
        const w = rpsResolve(tb.rps[0], tb.rps[1]);
        const choices = tb.rps.slice();
        if (w < 0) {
          tb.rps = [null, null];
          tableBroadcast(tb, { t: "rps_result", draw: true, choices, winner: -1 }, 0);
        } else {
          tb.rpsWinner = w;
          tb.phase = "order";
          tableBroadcast(tb, { t: "rps_result", draw: false, choices, winner: w }, 0);
        }
      }
      break;
    }
    case "choose_order": {
      const tb = tableOf(client);
      if (!tb || client.seat < 0 || tb.phase !== "order") return;
      if (client.seat !== tb.rpsWinner) return err(client, "ไม่ใช่ผู้ชนะเป่ายิงฉุบ");
      tb.firstSeat = m.first ? client.seat : 1 - client.seat;
      startGame(tb);
      broadcastRoom(room);
      break;
    }
    case "action": {
      const tb = tableOf(client);
      if (!tb || client.seat < 0 || tb.phase !== "playing") return;
      const entry = { seq: tb.log.length, seat: client.seat, data: m.data };
      tb.log.push(entry);
      tableBroadcast(tb, { t: "action", seq: entry.seq, seat: entry.seat, data: entry.data }, client.id);
      break;
    }
    case "game_over": {
      const tb = tableOf(client);
      if (!tb || client.seat < 0 || tb.phase !== "playing") return;
      tb.phase = "done";
      tableBroadcast(tb, { t: "game_over", winner: m.winner, reason: "normal" }, client.id);
      closeTable(room, tb);
      break;
    }
    case "leave_table": {
      if (client.tableId) leaveTable(client, false);
      break;
    }
    case "ping": {
      send(client, { t: "pong" });
      break;
    }
    default:
      err(client, "unknown message: " + m.t);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, clients: clients.size, rooms: rooms.size }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("UmaCardGame server running");
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) {
    socket.destroy();
    return;
  }
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      "Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n\r\n"
  );
  socket.setNoDelay(true);

  const client = { id: nextClientId++, name: "Player", room: null, tableId: 0, seat: -1, queued: false, conn: null };
  client.conn = new Conn(
    socket,
    (text) => {
      let m;
      try {
        m = JSON.parse(text);
      } catch (e) {
        return;
      }
      if (!m || typeof m.t !== "string") return;
      try {
        handle(client, m);
      } catch (e) {
        console.error("handler error", e);
      }
    },
    () => {
      try {
        leaveRoom(client);
      } catch (e) {
        console.error(e);
      }
      clients.delete(client.id);
    }
  );
  clients.set(client.id, client);
  send(client, { t: "welcome", id: client.id, name: client.name });
});

server.listen(PORT, () => console.log("UmaCardGame server listening on :" + PORT));

module.exports = server;
