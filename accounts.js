const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const NAME_RE = /^[A-Za-z0-9_฀-๿]{3,16}$/;
const MAX_DECKS = 40;
const MAX_CARD_ID = 100000;
const MAX_DECK_CARDS = 120;
const MAX_FAILS = 5;
const LOCK_MS = 5 * 60 * 1000;
const REG_PER_HOUR = 10;

function scrypt(pass, salt) {
  return new Promise((resolve, reject) => {
    crypto.scrypt(pass, salt, 64, { N: 16384, r: 8, p: 1 }, (err, key) => (err ? reject(err) : resolve(key.toString("hex"))));
  });
}

function sha256(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function loadStarter() {
  try {
    const f = path.join(__dirname, "starter.json");
    if (!fs.existsSync(f)) return null;
    const j = JSON.parse(fs.readFileSync(f, "utf8"));
    if (j && j.cards && typeof j.cards === "object") {
      const out = {};
      for (const k of Object.keys(j.cards)) {
        const n = parseInt(j.cards[k], 10);
        if (n > 0) out[String(parseInt(k, 10))] = Math.min(n, 99);
      }
      return Object.keys(out).length ? out : null;
    }
  } catch (e) {
    console.error("accounts: อ่าน starter.json ไม่ได้", e.message);
  }
  return null;
}

function createAccounts(store) {
  const fails = new Map();
  const regs = new Map();
  const starter = loadStarter();

  function checkLock(ipKey) {
    const f = fails.get(ipKey);
    if (!f) return 0;
    if (f.until && f.until > Date.now()) return Math.ceil((f.until - Date.now()) / 1000);
    if (f.until && f.until <= Date.now()) fails.delete(ipKey);
    return 0;
  }

  function addFail(ipKey) {
    const f = fails.get(ipKey) || { n: 0, until: 0 };
    f.n++;
    if (f.n >= MAX_FAILS) {
      f.until = Date.now() + LOCK_MS;
      f.n = 0;
    }
    fails.set(ipKey, f);
  }

  function validName(name) {
    return typeof name === "string" && NAME_RE.test(name);
  }

  function validPass(pass) {
    return typeof pass === "string" && pass.length >= 6 && pass.length <= 64;
  }

  function cleanDecks(input) {
    if (!Array.isArray(input)) return null;
    if (input.length > MAX_DECKS) return null;
    const out = [];
    for (const d of input) {
      if (!d || typeof d !== "object") return null;
      const name = String(d.name == null ? "" : d.name).slice(0, 30);
      const main = Array.isArray(d.main) ? d.main : [];
      const wit = Array.isArray(d.wit) ? d.wit : [];
      if (main.length > MAX_DECK_CARDS || wit.length > MAX_DECK_CARDS) return null;
      const m = [];
      const w = [];
      for (const x of main) {
        const n = parseInt(x, 10);
        if (!(n >= 0 && n < MAX_CARD_ID)) return null;
        m.push(n);
      }
      for (const x of wit) {
        const n = parseInt(x, 10);
        if (!(n >= 0 && n < MAX_CARD_ID)) return null;
        w.push(n);
      }
      out.push({ name, main: m, wit: w });
    }
    return out;
  }

  function deckWithinCollection(deck, collection) {
    if (!collection) return true;
    if (!deck || typeof deck !== "object") return false;
    const count = {};
    for (const list of [deck.main, deck.wit]) {
      if (!Array.isArray(list)) return false;
      if (list.length > MAX_DECK_CARDS) return false;
      for (const x of list) {
        const id = String(parseInt(x, 10));
        count[id] = (count[id] || 0) + 1;
      }
    }
    for (const id of Object.keys(count)) {
      if ((collection[id] || 0) < count[id]) return false;
    }
    return true;
  }

  function payload(user, token) {
    const ids = [];
    const counts = [];
    if (user.collection) {
      for (const k of Object.keys(user.collection)) {
        ids.push(parseInt(k, 10));
        counts.push(user.collection[k]);
      }
    }
    return {
      t: "auth_ok",
      user: user.name,
      token: token || "",
      accDecks: user.decks || [],
      collIds: ids,
      collCounts: counts,
      allUnlocked: !user.collection,
    };
  }

  async function register(ip, name, pass) {
    if (!validName(name)) return { error: "ชื่อผู้ใช้ต้อง 3-16 ตัว ใช้ได้เฉพาะ a-z 0-9 _ และตัวอักษรไทย" };
    if (!validPass(pass)) return { error: "รหัสผ่านต้องมี 6-64 ตัวอักษร" };
    const now = Date.now();
    const list = (regs.get(ip) || []).filter((t) => now - t < 3600000);
    if (list.length >= REG_PER_HOUR) return { error: "สมัครบ่อยเกินไป ลองใหม่ภายหลัง" };
    const key = name.toLowerCase();
    const salt = crypto.randomBytes(16).toString("hex");
    const hash = await scrypt(pass, salt);
    const ok = await store.createUser({
      key,
      name,
      salt,
      hash,
      decks: [],
      collection: starter ? JSON.parse(JSON.stringify(starter)) : null,
    });
    if (!ok) return { error: "ชื่อนี้มีคนใช้แล้ว" };
    list.push(now);
    regs.set(ip, list);
    const user = await store.getUser(key);
    const token = crypto.randomBytes(32).toString("hex");
    await store.createSession(sha256(token), key);
    return { user, token };
  }

  async function login(ip, name, pass) {
    if (typeof name !== "string" || typeof pass !== "string") return { error: "ข้อมูลไม่ถูกต้อง" };
    const key = name.toLowerCase();
    const lockKey = ip + "|" + key;
    const wait = checkLock(lockKey);
    if (wait > 0) return { error: "ใส่รหัสผิดหลายครั้ง รออีก " + wait + " วินาที" };
    const user = await store.getUser(key);
    let good = false;
    if (user) {
      const h = await scrypt(pass, user.salt);
      good = h.length === user.hash.length && crypto.timingSafeEqual(Buffer.from(h, "hex"), Buffer.from(user.hash, "hex"));
    } else {
      await scrypt(pass, "0000000000000000");
    }
    if (!good) {
      addFail(lockKey);
      return { error: "ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง" };
    }
    fails.delete(lockKey);
    const token = crypto.randomBytes(32).toString("hex");
    await store.createSession(sha256(token), key);
    return { user, token };
  }

  async function resume(token) {
    if (typeof token !== "string" || token.length < 32 || token.length > 128) return { error: "เซสชันหมดอายุ" };
    const key = await store.getSession(sha256(token));
    if (!key) return { error: "เซสชันหมดอายุ ให้เข้าสู่ระบบใหม่" };
    const user = await store.getUser(key);
    if (!user) return { error: "ไม่พบบัญชี" };
    return { user, token };
  }

  async function logout(token) {
    if (typeof token === "string" && token.length >= 32 && token.length <= 128) await store.deleteSession(sha256(token));
  }

  async function saveDecks(key, input) {
    const decks = cleanDecks(input);
    if (!decks) return { error: "ข้อมูลเด็คไม่ถูกต้อง" };
    await store.setDecks(key, decks);
    return { decks };
  }

  async function setCollection(key, collection) {
    await store.setCollection(key, collection);
  }

  return { register, login, resume, logout, saveDecks, setCollection, payload, deckWithinCollection, validName, store };
}

module.exports = { createAccounts };
