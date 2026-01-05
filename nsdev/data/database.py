import asyncio
import glob
import json
import os
import shutil
import sqlite3
import zipfile
from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

import aiohttp
import aiofiles

from ..code.encrypt import CipherHandler


class DataBase:
    def __init__(self, **options):
        self.storage_type = options.get("storage_type", "local")
        self.file_name = options.get("file_name", "database")
        self.keys_encrypt = options.get("keys_encrypt", "default_db_key_12345")
        self.method_encrypt = options.get("method_encrypt", "bytes")
        self.cipher = CipherHandler(
            key=self.keys_encrypt,
            method=self.method_encrypt
        )

        self._lock = asyncio.Lock()

        self.auto_backup = options.get("auto_backup", False)
        self.backup_bot_token = options.get("backup_bot_token")
        self.backup_chat_id = options.get("backup_chat_id")
        self.backup_cron_spec = options.get("backup_cron_spec", "0 */3 * * *")
        self.scheduler = options.get("scheduler_instance")

        if self.storage_type == "mongo":
            import pymongo
            self.mongo_url = options.get("mongo_url")
            self.client = pymongo.MongoClient(self.mongo_url)
            self.data = self.client[self.file_name]

        elif self.storage_type == "sqlite":
            self.db_file = f"{self.file_name}.db"
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self._initialize_sqlite()

        else:
            self.data_file = f"{self.file_name}.json"
            if not os.path.exists(self.data_file):
                with open(self.data_file, "w") as f:
                    json.dump({"vars": {}, "bots": []}, f)

        self._register_backup_task()

    async def _run_sync(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    def _safe_json_loads(self, data):
        if isinstance(data, (str, bytes, bytearray)):
            try:
                return json.loads(data)
            except Exception:
                return {}
        if isinstance(data, dict):
            return data
        return {}

    def _register_backup_task(self):
        if self.auto_backup and self.scheduler:
            @self.scheduler.cron(self.backup_cron_spec)
            async def _auto_backup():
                asyncio.create_task(self.perform_backup())

    async def perform_backup(self):
        async with self._lock:
            if self.storage_type == "sqlite":
                db_path = self.db_file
            else:
                db_path = self.data_file

            if not os.path.exists(db_path):
                return

            tmp_dir = "temp_db_backup"
            os.makedirs(tmp_dir, exist_ok=True)
            zip_path = None

            try:
                tmp_db = os.path.join(tmp_dir, os.path.basename(db_path))
                shutil.copy2(db_path, tmp_db)

                sources = [tmp_db]
                sources.extend(glob.glob("*.env"))

                zip_path = self._create_zip_archive(sources)

                if zip_path:
                    await self._send_zip_to_telegram(zip_path)
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                if zip_path and os.path.exists(zip_path):
                    os.remove(zip_path)

    def _create_zip_archive(self, files):
        ts = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d_%H%M%S")
        zip_name = f"backup_{self.file_name}_{ts}.zip"
        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as z:
            for f in files:
                z.write(f, os.path.basename(f))
        return zip_name

    async def _send_zip_to_telegram(self, path):
        if not self.backup_bot_token or not self.backup_chat_id:
            return

        url = f"https://api.telegram.org/bot{self.backup_bot_token}/sendDocument"
        form = aiohttp.FormData()
        form.add_field("chat_id", str(self.backup_chat_id))
        form.add_field("document", open(path, "rb"))

        async with aiohttp.ClientSession() as session:
            await session.post(url, data=form)

    def _initialize_sqlite(self):
        c = self.conn.cursor()
        c.execute(
            "CREATE TABLE IF NOT EXISTS vars (user_id TEXT PRIMARY KEY, data TEXT)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS bots ("
            "user_id TEXT PRIMARY KEY, "
            "api_id TEXT, "
            "api_hash TEXT, "
            "bot_token TEXT, "
            "session_string TEXT)"
        )
        self.conn.commit()

    async def _get_user_vars(self, user_id):
        uid = str(user_id)

        if self.storage_type == "sqlite":
            row = await self._run_sync(
                lambda: self.conn.cursor()
                .execute(
                    "SELECT data FROM vars WHERE user_id = ?",
                    (uid,)
                )
                .fetchone()
            )
            if not row:
                return {}
            decrypted = self.cipher.decrypt(row[0])
            return self._safe_json_loads(decrypted)

        if self.storage_type == "mongo":
            data = await self._run_sync(
                lambda: self.data.vars.find_one({"_id": uid})
            )
            return data or {}

        data = await self._load_data()
        return data.get("vars", {}).get(uid, {})

    async def _set_user_vars(self, user_id, user_data):
        uid = str(user_id)
        payload = json.dumps(user_data)
        encrypted = self.cipher.encrypt(payload)

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "INSERT OR REPLACE INTO vars (user_id, data) VALUES (?, ?)",
                        (uid, encrypted),
                    ),
                    self.conn.commit(),
                )
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.vars.update_one(
                    {"_id": uid},
                    {"$set": user_data},
                    upsert=True,
                )
            )
            return

        data = await self._load_data()
        data.setdefault("vars", {})[uid] = user_data
        await self._save_data(data)

    async def setVars(self, user_id, key, value, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        data.setdefault(var_key, {})[key] = value
        await self._set_user_vars(user_id, data)

    async def getVars(self, user_id, key, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        return data.get(var_key, {}).get(key)

    async def removeVars(self, user_id, key, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        if key in data.get(var_key, {}):
            data[var_key].pop(key)
            await self._set_user_vars(user_id, data)

    async def setListVars(self, user_id, key, value, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        data.setdefault(var_key, {}).setdefault(key, [])
        if value not in data[var_key][key]:
            data[var_key][key].append(value)
            await self._set_user_vars(user_id, data)

    async def getListVars(self, user_id, key, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        return list(data.get(var_key, {}).get(key, []))

    async def removeListVars(self, user_id, key, value, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        try:
            data[var_key][key].remove(value)
            await self._set_user_vars(user_id, data)
        except Exception:
            pass

    async def allVars(self, user_id, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        return data.get(var_key, {})

    async def removeAllVars(self, user_id):
        uid = str(user_id)

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "DELETE FROM vars WHERE user_id = ?",
                        (uid,),
                    ),
                    self.conn.commit(),
                )
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.vars.delete_one({"_id": uid})
            )
            return

        data = await self._load_data()
        data.get("vars", {}).pop(uid, None)
        await self._save_data(data)

    async def saveBot(self, user_id, api_id, api_hash, value, is_token=False):
        uid = str(user_id)
        field = "bot_token" if is_token else "session_string"

        payload = {
            "api_id": self.cipher.encrypt(str(api_id)),
            "api_hash": self.cipher.encrypt(api_hash),
            "bot_token": None,
            "session_string": None,
        }

        if value:
            payload[field] = self.cipher.encrypt(value)

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "INSERT OR REPLACE INTO bots "
                        "(user_id, api_id, api_hash, bot_token, session_string) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            uid,
                            payload["api_id"],
                            payload["api_hash"],
                            payload["bot_token"],
                            payload["session_string"],
                        ),
                    ),
                    self.conn.commit(),
                )
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.bot.update_one(
                    {"_id": uid},
                    {"$set": payload},
                    upsert=True,
                )
            )
            return

        data = await self._load_data()
        bots = data.get("bots", [])
        bots = [b for b in bots if b.get("user_id") != uid]
        bots.append({"user_id": uid, **payload})
        data["bots"] = bots
        await self._save_data(data)

    async def getBots(self, is_token=False):
        result = []

        if self.storage_type == "sqlite":
            rows = await self._run_sync(
                lambda: self.conn.cursor()
                .execute(
                    "SELECT user_id, api_id, api_hash, bot_token, session_string FROM bots"
                )
                .fetchall()
            )
            for r in rows:
                try:
                    bot = {
                        "user_id": r[0],
                        "api_id": int(self.cipher.decrypt(r[1])),
                        "api_hash": self.cipher.decrypt(r[2]),
                        "bot_token": self.cipher.decrypt(r[3]) if r[3] else None,
                        "session_string": self.cipher.decrypt(r[4]) if r[4] else None,
                    }
                    if (is_token and bot["bot_token"]) or (not is_token and bot["session_string"]):
                        result.append(bot)
                except Exception:
                    pass
            return result

        if self.storage_type == "mongo":
            rows = await self._run_sync(lambda: list(self.data.bot.find()))
            for r in rows:
                try:
                    bot = {
                        "user_id": r.get("_id"),
                        "api_id": int(self.cipher.decrypt(r["api_id"])),
                        "api_hash": self.cipher.decrypt(r["api_hash"]),
                        "bot_token": self.cipher.decrypt(r.get("bot_token")) if r.get("bot_token") else None,
                        "session_string": self.cipher.decrypt(r.get("session_string")) if r.get("session_string") else None,
                    }
                    if (is_token and bot["bot_token"]) or (not is_token and bot["session_string"]):
                        result.append(bot)
                except Exception:
                    pass
            return result

        data = await self._load_data()
        for r in data.get("bots", []):
            try:
                bot = {
                    "user_id": r.get("user_id"),
                    "api_id": int(self.cipher.decrypt(r["api_id"])),
                    "api_hash": self.cipher.decrypt(r["api_hash"]),
                    "bot_token": self.cipher.decrypt(r.get("bot_token")) if r.get("bot_token") else None,
                    "session_string": self.cipher.decrypt(r.get("session_string")) if r.get("session_string") else None,
                }
                if (is_token and bot["bot_token"]) or (not is_token and bot["session_string"]):
                    result.append(bot)
            except Exception:
                pass
        return result

    async def removeBot(self, user_id):
        uid = str(user_id)

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "DELETE FROM bots WHERE user_id = ?",
                        (uid,),
                    ),
                    self.conn.commit(),
                )
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.bot.delete_one({"_id": uid})
            )
            return

        data = await self._load_data()
        data["bots"] = [b for b in data.get("bots", []) if b.get("user_id") != uid]
        await self._save_data(data)

    async def _load_data(self):
        try:
            async with aiofiles.open(self.data_file, "r") as f:
                return json.loads(await f.read())
        except Exception:
            return {"vars": {}, "bots": []}

    async def _save_data(self, data):
        async with aiofiles.open(self.data_file, "w") as f:
            await f.write(json.dumps(data))

    async def close_async(self):
        await self._run_sync(self.close)

    def close(self):
        if self.storage_type == "sqlite" and hasattr(self, "conn"):
            self.conn.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
