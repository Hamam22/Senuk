import asyncio
import glob
import json
import os
import sqlite3
import zipfile
from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

import httpx
from ..code.encrypt import CipherHandler


class DataBase:
    def __init__(self, **options):
        self.storage_type = options.get("storage_type", "local")
        self.file_name = options.get("file_name", "database")
        self.keys_encrypt = options.get("keys_encrypt", "default_db_key_12345")
        self.method_encrypt = options.get("method_encrypt", "bytes")
        self.cipher = CipherHandler(key=self.keys_encrypt, method=self.method_encrypt)

        self.auto_backup = options.get("auto_backup", False)
        self.backup_bot_token = options.get("backup_bot_token")
        self.backup_chat_id = options.get("backup_chat_id")
        self.backup_cron_spec = options.get("backup_cron_spec", "0 */3 * * *")
        self.scheduler = options.get("scheduler_instance")

        if self.storage_type == "mongo":
            import pymongo
            self.mongo_url = options.get("mongo_url")
            if not self.mongo_url:
                raise ValueError("mongo_url is required")
            self.client = pymongo.MongoClient(self.mongo_url)
            self.data = self.client[self.file_name]

        elif self.storage_type == "sqlite":
            self.db_file = f"{self.file_name}.db"
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self._init_sqlite()

        else:
            self.data_file = f"{self.file_name}.json"
            if not os.path.exists(self.data_file):
                self._sync_save({"vars": {}, "bots": []})

        self._register_backup_task()

    async def _run_sync(self, func, *a, **kw):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *a, **kw))

    def _safe_json(self, value):
        if isinstance(value, (dict, list)):
            return value
        if not isinstance(value, str):
            return value
        try:
            return json.loads(value)
        except Exception:
            return value

    def _safe_decrypt(self, value):
        try:
            decrypted = self.cipher.decrypt(value)
        except Exception:
            return value
        return self._safe_json(decrypted)

    def _register_backup_task(self):
        if not (self.auto_backup and self.scheduler):
            return
        if not self.backup_bot_token or not self.backup_chat_id:
            return

        @self.scheduler.cron(self.backup_cron_spec)
        async def _job():
            await self.perform_backup()

    async def perform_backup(self):
        db_path = self.data_file if self.storage_type == "local" else self.db_file
        if not await self._run_sync(os.path.exists, db_path):
            return

        zip_name = f"backup_{self.file_name}_{datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y%m%d_%H%M%S')}.zip"

        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(db_path, os.path.basename(db_path))

        try:
            await self._send_zip(zip_name)
        finally:
            if os.path.exists(zip_name):
                os.remove(zip_name)

    async def _send_zip(self, path):
        url = f"https://api.telegram.org/bot{self.backup_bot_token}/sendDocument"
        async with httpx.AsyncClient(timeout=60) as client:
            with open(path, "rb") as f:
                await client.post(
                    url,
                    data={"chat_id": self.backup_chat_id},
                    files={"document": f},
                )

    def _sync_load(self):
        try:
            with open(self.data_file, "r") as f:
                return json.load(f)
        except Exception:
            return {"vars": {}, "bots": []}

    def _sync_save(self, data):
        with open(self.data_file, "w") as f:
            json.dump(data, f, indent=4)

    async def _load(self):
        return await self._run_sync(self._sync_load)

    async def _save(self, data):
        await self._run_sync(self._sync_save, data)

    def _init_sqlite(self):
        c = self.conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS vars (user_id TEXT PRIMARY KEY, data TEXT)")
        c.execute(
            "CREATE TABLE IF NOT EXISTS bots (user_id TEXT PRIMARY KEY, api_id TEXT, api_hash TEXT, bot_token TEXT, session_string TEXT)"
        )
        self.conn.commit()

    async def _get_user_vars(self, user_id):
        uid = str(user_id)

        if self.storage_type == "sqlite":
            row = await self._run_sync(
                lambda: self.conn.cursor().execute(
                    "SELECT data FROM vars WHERE user_id = ?", (uid,)
                ).fetchone()
            )
            if not row:
                return {}
            return self._safe_decrypt(row[0])

        if self.storage_type == "mongo":
            data = await self._run_sync(lambda: self.data.vars.find_one({"_id": uid}))
            return data or {}

        data = await self._load()
        return data.get("vars", {}).get(uid, {})

    async def _set_user_vars(self, user_id, user_data):
        uid = str(user_id)

        if self.storage_type == "sqlite":
            enc = self.cipher.encrypt(json.dumps(user_data))
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "INSERT OR REPLACE INTO vars (user_id, data) VALUES (?, ?)", (uid, enc)
                    ),
                    self.conn.commit(),
                )
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.vars.update_one({"_id": uid}, {"$set": user_data}, upsert=True)
            )
            return

        data = await self._load()
        data.setdefault("vars", {})[uid] = user_data
        await self._save(data)

    async def setVars(self, user_id, query_name, value, var_key="variabel"):
        enc = self.cipher.encrypt(json.dumps(value) if isinstance(value, (dict, list)) else str(value))
        data = await self._get_user_vars(user_id)
        data.setdefault(var_key, {})[query_name] = enc
        await self._set_user_vars(user_id, data)

    async def getVars(self, user_id, query_name, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        val = data.get(var_key, {}).get(query_name)
        if not val:
            return None
        return self._safe_decrypt(val)

    async def removeVars(self, user_id, query_name, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        if data.get(var_key, {}).pop(query_name, None):
            await self._set_user_vars(user_id, data)

    async def setListVars(self, user_id, query_name, value, var_key="variabel"):
        enc = self.cipher.encrypt(json.dumps(value) if isinstance(value, (dict, list)) else str(value))
        data = await self._get_user_vars(user_id)
        data.setdefault(var_key, {}).setdefault(query_name, [])
        if enc not in data[var_key][query_name]:
            data[var_key][query_name].append(enc)
            await self._set_user_vars(user_id, data)

    async def getListVars(self, user_id, query_name, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        return [self._safe_decrypt(v) for v in data.get(var_key, {}).get(query_name, [])]

    async def removeListVars(self, user_id, query_name, value, var_key="variabel"):
        enc = self.cipher.encrypt(json.dumps(value) if isinstance(value, (dict, list)) else str(value))
        data = await self._get_user_vars(user_id)
        try:
            data.get(var_key, {}).get(query_name, []).remove(enc)
            await self._set_user_vars(user_id, data)
        except Exception:
            pass

    async def allVars(self, user_id, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        out = {}
        for k, v in data.get(var_key, {}).items():
            if isinstance(v, list):
                out[k] = [self._safe_decrypt(i) for i in v]
            else:
                out[k] = self._safe_decrypt(v)
        return out

    async def saveBot(self, user_id, api_id, api_hash, value, is_token=False):
        uid = str(user_id)
        field = "bot_token" if is_token else "session_string"
        bot = {
            "api_id": self.cipher.encrypt(str(api_id)),
            "api_hash": self.cipher.encrypt(api_hash),
        }
        if value:
            bot[field] = self.cipher.encrypt(value)

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.bot.update_one({"_id": uid}, {"$set": bot}, upsert=True)
            )
            return

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "INSERT OR REPLACE INTO bots VALUES (?, ?, ?, ?, ?)",
                        (uid, bot["api_id"], bot["api_hash"], bot.get("bot_token"), bot.get("session_string")),
                    ),
                    self.conn.commit(),
                )
            )
            return

        data = await self._load()
        data.setdefault("bots", [])
        found = next((b for b in data["bots"] if b.get("user_id") == uid), None)
        if found:
            found.update(bot)
        else:
            data["bots"].append({"user_id": uid, **bot})
        await self._save(data)

    async def getBots(self, is_token=False):
        raw = []

        if self.storage_type == "mongo":
            raw = await self._run_sync(lambda: list(self.data.bot.find()))
        elif self.storage_type == "sqlite":
            rows = await self._run_sync(
                lambda: self.conn.cursor().execute(
                    "SELECT user_id, api_id, api_hash, bot_token, session_string FROM bots"
                ).fetchall()
            )
            raw = [
                dict(zip(["user_id", "api_id", "api_hash", "bot_token", "session_string"], r))
                for r in rows
            ]
        else:
            raw = (await self._load()).get("bots", [])

        out = []
        for b in raw:
            try:
                d = {"name": b.get("user_id") or b.get("_id")}
                for k in ["api_id", "api_hash", "bot_token", "session_string"]:
                    if b.get(k):
                        v = self.cipher.decrypt(b[k])
                        d[k] = int(v) if k == "api_id" else v
                if (is_token and "bot_token" in d) or (not is_token and "session_string" in d):
                    out.append(d)
            except Exception:
                continue
        return out

    async def removeBot(self, user_id):
        uid = str(user_id)

        if self.storage_type == "mongo":
            await self._run_sync(lambda: self.data.bot.delete_one({"_id": uid}))
            return

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute("DELETE FROM bots WHERE user_id = ?", (uid,)),
                    self.conn.commit(),
                )
            )
            return

        data = await self._load()
        data["bots"] = [b for b in data.get("bots", []) if b.get("user_id") != uid]
        await self._save(data)
