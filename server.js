const http = require("http");
const crypto = require("crypto");
const { createStore } = require("./store");
const { createAccounts } = require("./accounts");

const ALLOW_GUEST = process.env.ALLOW_GUEST === "1";
const ADMIN_KEY = process.env.ADMIN_KEY || "";
const store = createStore();
const accounts = createAccounts(store);

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

function buildState(room) {
  return {
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
}

function broadcastRoom(room) {
  const state = buildState(room);
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
  if (client.seat >= 0 && tb.phase === "waiting") {
    tb.seats[client.seat] = null;
    client.tableId = 0;
    client.seat = -1;
    if (!tb.seats[0] && !tb.seats[1] && tb.spectators.size === 0) room.tables.delete(tb.id);
    if (!silent) send(client, { t: "table_left" });
    tryMatch(room);
    broadcastRoom(room);
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
  if (tb.voteTimer) clearTimeout(tb.voteTimer);
  tb.voteTimer = null;
  room.tables.delete(tb.id);
  tryMatch(room);
  broadcastRoom(room);
}

function newTable(seats) {
  return {
    id: nextTableId++,
    seats,
    spectators: new Set(),
    phase: "ready",
    decks: [null, null],
    rps: [null, null],
    rpsWinner: -1,
    votes: [null, null],
    voteTimer: null,
    firstSeat: -1,
    seed: crypto.randomInt(1, 2147483647),
    log: [],
    seqCounter: 0,
  };
}

function startMatch(tb) {
  tb.phase = "ready";
  const a = clients.get(tb.seats[0]);
  const b = clients.get(tb.seats[1]);
  send(a, { t: "match_found", tableId: tb.id, seat: 0, opponent: b.name, seed: tb.seed });
  send(b, { t: "match_found", tableId: tb.id, seat: 1, opponent: a.name, seed: tb.seed });
}

function tryMatch(room) {
  while (room.queue.length >= 2 && room.tables.size < room.maxTables) {
    const a = clients.get(room.queue.shift());
    const b = clients.get(room.queue.shift());
    if (!a || !b) continue;
    a.queued = false;
    b.queued = false;
    const tb = newTable([a.id, b.id]);
    room.tables.set(tb.id, tb);
    a.tableId = tb.id;
    a.seat = 0;
    b.tableId = tb.id;
    b.seat = 1;
    startMatch(tb);
  }
  for (let i = 0; i < room.queue.length; i++) {
    const c = clients.get(room.queue[i]);
    send(c, { t: "queue_status", position: i + 1, tablesFull: room.tables.size >= room.maxTables });
  }
}

const VOTE_SECONDS = 15;

function methodIndex(m) {
  return m === "coin" ? 0 : m === "rps" ? 1 : m === "dice" ? 2 : -1;
}

function startVote(room, tb) {
  tb.phase = "vote";
  tb.votes = [null, null];
  tableBroadcast(tb, { t: "vote_start", seconds: VOTE_SECONDS }, 0);
  tb.voteTimer = setTimeout(() => resolveVote(room, tb), VOTE_SECONDS * 1000 + 300);
}

function resolveVote(room, tb) {
  if (tb.phase !== "vote") return;
  if (tb.voteTimer) clearTimeout(tb.voteTimer);
  tb.voteTimer = null;
  const v = tb.votes;
  let method;
  let tie = false;
  const all = ["coin", "dice", "rps"];
  if (v[0] && v[1]) {
    if (v[0] === v[1]) method = v[0];
    else {
      tie = true;
      method = crypto.randomInt(0, 2) === 0 ? v[0] : v[1];
    }
  } else if (v[0] || v[1]) {
    method = v[0] || v[1];
  } else {
    tie = true;
    method = all[crypto.randomInt(0, 3)];
  }
  tb.phase = "method";
  tableBroadcast(tb, { t: "vote_result", method, tie, votes: [methodIndex(v[0]), methodIndex(v[1])] }, 0);
  tb.voteTimer = setTimeout(() => {
    tb.voteTimer = null;
    if (!room.tables.has(tb.id) || tb.phase !== "method") return;
    if (method === "coin") {
      const headsSeat = crypto.randomInt(0, 2);
      const heads = crypto.randomInt(0, 2) === 0;
      const winner = heads ? headsSeat : 1 - headsSeat;
      tb.rpsWinner = winner;
      tb.phase = "order";
      tableBroadcast(tb, { t: "coin_result", headsSeat, coin: heads ? "heads" : "tails", winner }, 0);
    } else if (method === "dice") {
      const dice = [];
      for (let i = 0; i < 12; i++) {
        const d0 = crypto.randomInt(1, 7);
        const d1 = crypto.randomInt(1, 7);
        dice.push(d0, d1);
        if (d0 !== d1) break;
      }
      let last = dice.length - 2;
      if (dice[last] === dice[last + 1]) {
        dice[last + 1] = dice[last] === 6 ? 5 : dice[last] + 1;
      }
      const winner = dice[last] > dice[last + 1] ? 0 : 1;
      tb.rpsWinner = winner;
      tb.phase = "order";
      tableBroadcast(tb, { t: "dice_result", dice, winner }, 0);
    } else {
      tb.phase = "rps";
      tb.rps = [null, null];
      tableBroadcast(tb, { t: "rps_start" }, 0);
    }
    broadcastRoom(room);
  }, 2200);
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

const OPEN_TYPES = new Set(["hello", "ping", "register", "login", "resume", "logout"]);

function isAuthed(client) {
  return !!client.account || (ALLOW_GUEST && client.guest);
}

function attachAccount(client, user, token) {
  for (const c of clients.values()) {
    if (c !== client && c.account && c.account.key === user.key) {
      send(c, { t: "kicked", msg: "มีการเข้าสู่ระบบบัญชีนี้จากที่อื่น" });
      try {
        c.conn.close();
      } catch (e) {}
    }
  }
  if (client.room) leaveRoom(client);
  client.account = { key: user.key, name: user.name, collection: user.collection };
  client.token = token;
  client.guest = false;
  client.name = user.name;
  send(client, accounts.payload(user, token));
  send(client, { t: "welcome", id: client.id, name: client.name });
}

async function handleAuth(client, m) {
  const ip = client.ip || "";
  if (m.t === "register" || m.t === "login") {
    const r = m.t === "register" ? await accounts.register(ip, m.user, m.pass) : await accounts.login(ip, m.user, m.pass);
    if (r.error) return send(client, { t: "auth_fail", msg: r.error });
    attachAccount(client, r.user, r.token);
    return;
  }
  if (m.t === "resume") {
    const r = await accounts.resume(m.token);
    if (r.error) return send(client, { t: "auth_fail", msg: r.error, resume: true });
    attachAccount(client, r.user, r.token);
    return;
  }
  if (m.t === "logout") {
    await accounts.logout(m.token || client.token);
    if (client.room) leaveRoom(client);
    client.account = null;
    client.token = "";
    client.guest = false;
    client.name = "Player";
    send(client, { t: "logged_out" });
  }
}

function handle(client, m) {
  const room = client.room ? rooms.get(client.room) : null;
  if (!OPEN_TYPES.has(m.t) && !isAuthed(client)) {
    return send(client, { t: "auth_required", msg: "ต้องเข้าสู่ระบบก่อน" });
  }
  switch (m.t) {
    case "hello": {
      if (ALLOW_GUEST && !client.account) {
        client.guest = true;
        client.name = String(m.name || "Player").slice(0, 20) || "Player";
      }
      send(client, { t: "welcome", id: client.id, name: client.name });
      break;
    }
    case "register":
    case "login":
    case "resume":
    case "logout": {
      handleAuth(client, m).catch((e) => {
        console.error("auth error", e);
        send(client, { t: "auth_fail", msg: "เซิร์ฟเวอร์ผิดพลาด ลองใหม่อีกครั้ง" });
      });
      break;
    }
    case "save_decks": {
      if (!client.account) return err(client, "บัญชีผู้เล่นรับเด็คได้เฉพาะเมื่อเข้าสู่ระบบ");
      const list = m.data && m.data.decks;
      accounts
        .saveDecks(client.account.key, list)
        .then((r) => {
          if (r.error) send(client, { t: "decks_error", msg: r.error });
          else send(client, { t: "decks_saved", n: r.decks.length });
        })
        .catch((e) => {
          console.error("save_decks error", e);
          send(client, { t: "decks_error", msg: "บันทึกเด็คไม่สำเร็จ" });
        });
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
    case "quick_room": {
      if (room) return err(client, "อยู่ในห้องแล้ว");
      const code = String(m.code || "DEV1").toUpperCase().slice(0, 8);
      let r = rooms.get(code);
      if (!r) {
        r = {
          code,
          name: "DEV " + code,
          hostId: client.id,
          maxTables: 4,
          members: new Set(),
          tables: new Map(),
          queue: [],
        };
        rooms.set(code, r);
      }
      r.members.add(client.id);
      client.room = code;
      send(client, { t: "joined_room", code });
      broadcastRoom(r);
      break;
    }
    case "quick_sit": {
      if (!room) return err(client, "ยังไม่ได้เข้าห้อง");
      if (client.tableId) return;
      let waiting = null;
      for (const tb of room.tables.values()) {
        if (tb.phase === "waiting" && (!tb.seats[0] || !tb.seats[1])) {
          waiting = tb;
          break;
        }
      }
      if (waiting) handle(client, { t: "sit", tableId: waiting.id });
      else handle(client, { t: "create_table" });
      break;
    }
    case "create_table": {
      if (!room) return err(client, "ยังไม่ได้เข้าห้อง");
      if (client.tableId) return err(client, "อยู่ที่โต๊ะอยู่แล้ว");
      if (room.tables.size >= room.maxTables) return err(client, "โต๊ะเต็ม");
      removeFromQueue(client);
      const tb = newTable([client.id, null]);
      tb.phase = "waiting";
      room.tables.set(tb.id, tb);
      client.tableId = tb.id;
      client.seat = 0;
      send(client, { t: "seated", tableId: tb.id, seat: 0 });
      broadcastRoom(room);
      break;
    }
    case "sit": {
      if (!room) return err(client, "ยังไม่ได้เข้าห้อง");
      if (client.tableId) return err(client, "อยู่ที่โต๊ะอยู่แล้ว");
      const tb = room.tables.get(parseInt(m.tableId, 10));
      if (!tb) return err(client, "ไม่พบโต๊ะ");
      if (tb.phase !== "waiting") return err(client, "โต๊ะนี้เริ่มแข่งแล้ว");
      const seat = tb.seats[0] ? (tb.seats[1] ? -1 : 1) : 0;
      if (seat < 0) return err(client, "โต๊ะเต็มแล้ว");
      removeFromQueue(client);
      tb.seats[seat] = client.id;
      client.tableId = tb.id;
      client.seat = seat;
      if (tb.seats[0] && tb.seats[1]) {
        startMatch(tb);
      } else {
        send(client, { t: "seated", tableId: tb.id, seat });
      }
      broadcastRoom(room);
      break;
    }
    case "watch": {
      if (!room) return err(client, "ยังไม่ได้เข้าห้อง");
      if (client.tableId) return err(client, "อยู่ที่โต๊ะอยู่แล้ว");
      const tb = room.tables.get(parseInt(m.tableId, 10));
      if (!tb) return err(client, "ไม่พบโต๊ะ");
      if (tb.phase === "waiting") return err(client, "โต๊ะนี้ยังรอคู่แข่งอยู่ กดนั่งได้เลย");
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
      if (client.account && !accounts.deckWithinCollection(m.deck, client.account.collection)) {
        err(client, "เด็คนี้มีการ์ดที่ไม่มีในคอลเลกชันของคุณ");
        leaveTable(client, false);
        return;
      }
      tb.decks[client.seat] = m.deck;
      if (tb.decks[0] && tb.decks[1]) {
        startVote(room, tb);
        broadcastRoom(room);
      }
      break;
    }
    case "vote": {
      const tb = tableOf(client);
      if (!tb || client.seat < 0 || tb.phase !== "vote") return;
      const mth = m.method === "coin" ? "coin" : m.method === "rps" ? "rps" : m.method === "dice" ? "dice" : null;
      if (!mth) return;
      if (tb.votes[client.seat]) return;
      tb.votes[client.seat] = mth;
      tableBroadcast(tb, { t: "vote_locked", seat: client.seat }, client.id);
      if (tb.votes[0] && tb.votes[1]) resolveVote(room, tb);
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
      const entry = { seq: tb.seqCounter++, seat: client.seat, data: m.data };
      tb.log.push(entry);
      if (tb.log.length > 300) tb.log.shift();
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
    case "get_state": {
      if (room) send(client, buildState(room));
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

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const parts = [];
    req.on("data", (d) => {
      size += d.length;
      if (size > limit) {
        reject(new Error("too large"));
        req.destroy();
        return;
      }
      parts.push(d);
    });
    req.on("end", () => resolve(Buffer.concat(parts).toString("utf8")));
    req.on("error", reject);
  });
}

function jsonRes(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(obj));
}

async function adminGrant(req, res) {
  if (!ADMIN_KEY) return jsonRes(res, 404, { error: "admin ปิดอยู่ (ไม่ได้ตั้ง ADMIN_KEY)" });
  const given = Buffer.from(String(req.headers["x-admin-key"] || ""));
  const want = Buffer.from(ADMIN_KEY);
  if (given.length !== want.length || !crypto.timingSafeEqual(given, want)) return jsonRes(res, 403, { error: "คีย์ไม่ถูกต้อง" });
  let body;
  try {
    body = JSON.parse(await readBody(req, 200000));
  } catch (e) {
    return jsonRes(res, 400, { error: "JSON ไม่ถูกต้อง" });
  }
  const key = String(body.user || "").toLowerCase();
  const user = await store.getUser(key);
  if (!user) return jsonRes(res, 404, { error: "ไม่พบบัญชี" });
  let coll = user.collection;
  if (body.all === true) {
    coll = null;
  } else if (body.cards && typeof body.cards === "object") {
    if (!coll && body.mode !== "set") return jsonRes(res, 400, { error: "บัญชีนี้ปลดล็อกทุกใบอยู่แล้ว ใช้ mode set พร้อม cards ทั้งชุดเพื่อจำกัด", allUnlocked: true });
    coll = body.mode === "set" && !coll ? {} : Object.assign({}, coll);
    for (const k of Object.keys(body.cards)) {
      const id = String(parseInt(k, 10));
      const n = parseInt(body.cards[k], 10);
      if (!(parseInt(k, 10) >= 0) || isNaN(n)) continue;
      const cur = coll[id] || 0;
      const next = body.mode === "set" ? n : cur + n;
      if (next > 0) coll[id] = Math.min(next, 99);
      else delete coll[id];
    }
  } else {
    return jsonRes(res, 400, { error: "ต้องส่ง all:true หรือ cards:{id:จำนวน}" });
  }
  await accounts.setCollection(key, coll);
  for (const c of clients.values()) {
    if (c.account && c.account.key === key) {
      c.account.collection = coll;
      send(c, accounts.payload(Object.assign({}, user, { collection: coll }), c.token));
    }
  }
  jsonRes(res, 200, { ok: true, allUnlocked: !coll, kinds: coll ? Object.keys(coll).length : -1 });
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, clients: clients.size, rooms: rooms.size, store: store.kind }));
    return;
  }
  if (req.method === "POST" && req.url === "/admin/grant") {
    adminGrant(req, res).catch((e) => {
      console.error("admin error", e);
      jsonRes(res, 500, { error: "server error" });
    });
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

  const fwd = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  const client = {
    id: nextClientId++,
    name: "Player",
    room: null,
    tableId: 0,
    seat: -1,
    queued: false,
    conn: null,
    account: null,
    token: "",
    guest: false,
    ip: fwd || socket.remoteAddress || "",
  };
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

store
  .init()
  .then(() => {
    server.listen(PORT, () => console.log("UmaCardGame server listening on :" + PORT + " store=" + store.kind + " guest=" + ALLOW_GUEST));
  })
  .catch((e) => {
    console.error("เริ่ม store ไม่สำเร็จ", e);
    process.exit(1);
  });

module.exports = server;
