const fs = require("fs");
const path = require("path");

const SESSION_DAYS = 60;

function createFileStore(file) {
  let data = { users: {}, sessions: {} };
  let timer = null;

  function load() {
    try {
      if (fs.existsSync(file)) data = JSON.parse(fs.readFileSync(file, "utf8"));
      if (!data.users) data.users = {};
      if (!data.sessions) data.sessions = {};
    } catch (e) {
      console.error("store: อ่านไฟล์ไม่ได้", e.message);
    }
  }

  function flush() {
    timer = null;
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const tmp = file + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(data));
      fs.renameSync(tmp, file);
    } catch (e) {
      console.error("store: เขียนไฟล์ไม่ได้", e.message);
    }
  }

  function dirty() {
    if (!timer) timer = setTimeout(flush, 300);
  }

  return {
    kind: "file",
    async init() {
      load();
    },
    async getUser(key) {
      const u = data.users[key];
      return u ? JSON.parse(JSON.stringify(u)) : null;
    },
    async createUser(u) {
      if (data.users[u.key]) return false;
      data.users[u.key] = { key: u.key, name: u.name, salt: u.salt, hash: u.hash, decks: u.decks, collection: u.collection };
      dirty();
      return true;
    },
    async setDecks(key, decks) {
      if (data.users[key]) data.users[key].decks = decks;
      dirty();
    },
    async setCollection(key, collection) {
      if (data.users[key]) data.users[key].collection = collection;
      dirty();
    },
    async createSession(tokenHash, key) {
      data.sessions[tokenHash] = { key, at: Date.now() };
      dirty();
    },
    async getSession(tokenHash) {
      const s = data.sessions[tokenHash];
      if (!s) return null;
      if (Date.now() - s.at > SESSION_DAYS * 86400000) {
        delete data.sessions[tokenHash];
        dirty();
        return null;
      }
      return s.key;
    },
    async deleteSession(tokenHash) {
      delete data.sessions[tokenHash];
      dirty();
    },
    async close() {
      if (timer) {
        clearTimeout(timer);
        flush();
      }
    },
  };
}

function createPgStore(pool) {
  return {
    kind: "postgres",
    async init() {
      await pool.query(
        "CREATE TABLE IF NOT EXISTS users (" +
          "key TEXT PRIMARY KEY, name TEXT NOT NULL, salt TEXT NOT NULL, hash TEXT NOT NULL, " +
          "decks TEXT NOT NULL DEFAULT '[]', collection TEXT, created_at TIMESTAMP DEFAULT NOW())"
      );
      await pool.query(
        "CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, user_key TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())"
      );
    },
    async getUser(key) {
      const r = await pool.query("SELECT key, name, salt, hash, decks, collection FROM users WHERE key = $1", [key]);
      if (!r.rows.length) return null;
      const row = r.rows[0];
      return {
        key: row.key,
        name: row.name,
        salt: row.salt,
        hash: row.hash,
        decks: JSON.parse(row.decks || "[]"),
        collection: row.collection ? JSON.parse(row.collection) : null,
      };
    },
    async createUser(u) {
      try {
        await pool.query("INSERT INTO users (key, name, salt, hash, decks, collection) VALUES ($1, $2, $3, $4, $5, $6)", [
          u.key,
          u.name,
          u.salt,
          u.hash,
          JSON.stringify(u.decks || []),
          u.collection ? JSON.stringify(u.collection) : null,
        ]);
        return true;
      } catch (e) {
        if (e && (e.code === "23505" || /duplicate|unique/i.test(String(e.message)))) return false;
        throw e;
      }
    },
    async setDecks(key, decks) {
      await pool.query("UPDATE users SET decks = $1 WHERE key = $2", [JSON.stringify(decks), key]);
    },
    async setCollection(key, collection) {
      await pool.query("UPDATE users SET collection = $1 WHERE key = $2", [collection ? JSON.stringify(collection) : null, key]);
    },
    async createSession(tokenHash, key) {
      await pool.query("INSERT INTO sessions (token_hash, user_key) VALUES ($1, $2)", [tokenHash, key]);
    },
    async getSession(tokenHash) {
      const r = await pool.query("SELECT user_key, created_at FROM sessions WHERE token_hash = $1", [tokenHash]);
      if (!r.rows.length) return null;
      const at = new Date(r.rows[0].created_at).getTime();
      if (Date.now() - at > SESSION_DAYS * 86400000) {
        await pool.query("DELETE FROM sessions WHERE token_hash = $1", [tokenHash]);
        return null;
      }
      return r.rows[0].user_key;
    },
    async deleteSession(tokenHash) {
      await pool.query("DELETE FROM sessions WHERE token_hash = $1", [tokenHash]);
    },
    async close() {
      await pool.end();
    },
  };
}

function createStore(opts) {
  opts = opts || {};
  if (opts.pool) return createPgStore(opts.pool);
  const url = process.env.DATABASE_URL;
  if (url) {
    const { Pool } = require("pg");
    const local = /localhost|127\.0\.0\.1/.test(url);
    const ssl = process.env.PGSSL === "0" || local ? false : { rejectUnauthorized: false };
    const pool = new Pool({ connectionString: url, ssl, max: 5 });
    pool.on("error", (e) => console.error("pg pool error", e.message));
    return createPgStore(pool);
  }
  console.warn("store: ไม่มี DATABASE_URL ใช้ไฟล์ชั่วคราว ข้อมูลบัญชีจะหายเมื่อ Render รีสตาร์ต");
  return createFileStore(process.env.DATA_FILE || path.join(__dirname, "data", "accounts.json"));
}

module.exports = { createStore };
