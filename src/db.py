import aiosqlite
import datetime

CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE,
    username TEXT,
    is_admin INTEGER DEFAULT 0
);
"""

CREATE_TASKS = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_telegram_id INTEGER,
    title TEXT,
    description TEXT,
    category TEXT,
    status TEXT DEFAULT 'open',
    created_at TEXT
);
"""

CREATE_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

class DB:
    def __init__(self, path):
        self.path = path
        self.conn = None

    async def init(self):
        self.conn = await aiosqlite.connect(self.path)
        await self.conn.execute("PRAGMA foreign_keys = ON;")
        await self.conn.executescript(CREATE_USERS + CREATE_TASKS + CREATE_SETTINGS)
        await self.conn.commit()
        # настройка экспорта по умолчанию
        cur = await self.conn.execute("SELECT value FROM settings WHERE key=?", ("export_enabled",))
        row = await cur.fetchone()
        if not row:
            await self.conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ("export_enabled", "1"))
            await self.conn.commit()

    async def ensure_user(self, tg_id: int, username: str | None):
        cur = await self.conn.execute("SELECT id FROM users WHERE telegram_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row:
            await self.conn.execute("INSERT INTO users (telegram_id, username) VALUES (?, ?)", (tg_id, username or ""))
            await self.conn.commit()

    async def add_task(self, tg_id: int, title: str, category: str, description: str = ""):
        created = datetime.datetime.utcnow().isoformat()
        await self.conn.execute(
            "INSERT INTO tasks (user_telegram_id, title, description, category, status, created_at) VALUES (?, ?, ?, ?, 'open', ?)",
            (tg_id, title, description, category, created)
        )
        await self.conn.commit()

    async def list_tasks(self, tg_id: int):
        cur = await self.conn.execute(
            "SELECT id, title, description, category, status, created_at FROM tasks WHERE user_telegram_id=? ORDER BY created_at DESC",
            (tg_id,)
        )
        return await cur.fetchall()

    async def get_all_tasks_for_user(self, tg_id: int):
        cur = await self.conn.execute(
            "SELECT id, title, description, category, status, created_at FROM tasks WHERE user_telegram_id=?",
            (tg_id,)
        )
        return await cur.fetchall()

    async def stats_by_category(self, tg_id: int):
        cur = await self.conn.execute(
            "SELECT category, COUNT(*) FROM tasks WHERE user_telegram_id=? GROUP BY category",
            (tg_id,)
        )
        return await cur.fetchall()

    async def search_tasks(self, tg_id: int, q: str):
        cur = await self.conn.execute(
            "SELECT id, title, description, category, status, created_at FROM tasks WHERE user_telegram_id=?",
            (tg_id,)
        )
        return await cur.fetchall()

    async def close_task(self, task_id: int, tg_id: int):
        await self.conn.execute(
            "UPDATE tasks SET status='done' WHERE id=? AND user_telegram_id=?",
            (task_id, tg_id)
        )
        await self.conn.commit()

    async def delete_task(self, task_id: int, tg_id: int):
        await self.conn.execute(
            "DELETE FROM tasks WHERE id=? AND user_telegram_id=?",
            (task_id, tg_id)
        )
        await self.conn.commit()

    # ====== Настройки админа ======
    async def get_setting(self, key: str) -> str | None:
        cur = await self.conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        return row[0] if row else None

    async def set_setting(self, key: str, value: str):
        await self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await self.conn.commit()