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

        if not self.keys_encrypt or self.keys_encrypt == "default_db_key_12345":
            raise ValueError("keys_encrypt must be explicitly set")

        self.cipher = CipherHandler(key=self.keys_encrypt, method=self.method_encrypt)
        self._lock = asyncio.Lock()

        self.auto_backup = options.get("auto_backup", False)
        self.backup_bot_token = options.get("backup_bot_token")
        self.backup_chat_id = options.get("backup_chat_id")
        self.backup_cron_spec = options.get("backup_cron_spec", "0 */3 * * *")
        self.scheduler = options.get("scheduler_instance")

        if self.storage_type == "mongo":
            import pymongo

            self.mongo_url = options.get("mongo_url")
            if not self.mongo_url:
                raise ValueError("mongo_url is required for MongoDB storage")

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
                    json.dump({"vars": {}, "bots": []}, f, indent=4)

        self._register_backup_task()

    async def _run_sync(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args))

    def _register_backup_task(self):
        if not (self.auto_backup and self.scheduler):
            return
        if not self.backup_bot_token or not self.backup_chat_id:
            return

        @self.scheduler.cron(self.backup_cron_spec)
        async def _():
            asyncio.create_task(self.perform_backup())

    async def perform_backup(self):
        async with self._lock:
            db_path = self.data_file if self.storage_type == "local" else self.db_file
            if not os.path.exists(db_path):
                return

            temp_backup_dir = "temp_db_backup"
            os.makedirs(temp_backup_dir, exist_ok=True)
            zip_path = None

            try:
                temp_db_path = os.path.join(temp_backup_dir, os.path.basename(db_path))
                shutil.copy2(db_path, temp_db_path)

                sources = [temp_db_path]
                sources.extend(glob.glob("*.env"))

                zip_path = self._create_zip_archive(sources)

                if zip_path:
                    timestamp = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S %Z")
                    caption = (
                        f"Backup otomatis `{os.path.basename(zip_path)}`\n"
                        f"Tipe DB: `{self.storage_type}`\n"
                        f"Waktu: `{timestamp}`"
                    )
                    await self._send_zip_to_telegram(zip_path, caption)
            finally:
                shutil.rmtree(temp_backup_dir, ignore_errors=True)
                if zip_path and os.path.exists(zip_path):
                    os.remove(zip_path)

    def _create_zip_archive(self, source_paths: list):
        timestamp = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d_%H%M%S")
        zip_filename = f"backup_{self.file_name}_{timestamp}.zip"
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in source_paths:
                if os.path.exists(path):
                    zf.write(path, arcname=os.path.basename(path))
        return zip_filename

    async def _send_zip_to_telegram(self, file_path, caption):
        url = f"https://api.telegram.org/bot{self.backup_bot_token}/sendDocument"
        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as f:
                data = aiohttp.FormData()
                data.add_field("chat_id", str(self.backup_chat_id))
                data.add_field("caption", caption)
                data.add_field("parse_mode", "Markdown")
                data.add_field("document", f)
                async with session.post(url, data=data):
                    pass

    async def _load_data(self):
        async with self._lock:
            try:
                async with aiofiles.open(self.data_file, "r") as f:
                    content = await f.read()
                    return json.loads(content) if content.strip() else {"vars": {}, "bots": []}
            except (FileNotFoundError, json.JSONDecodeError):
                return {"vars": {}, "bots": []}

    async def _save_data(self, data):
        async with self._lock:
            temp = f"{self.data_file}.tmp"
            async with aiofiles.open(temp, "w") as f:
                await f.write(json.dumps(data, indent=4))
            os.replace(temp, self.data_file)

    def _initialize_sqlite(self):
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS vars (user_id TEXT PRIMARY KEY, data TEXT)")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS bots (user_id TEXT PRIMARY KEY, api_id TEXT, api_hash TEXT, bot_token TEXT, session_string TEXT)"
        )
        self.conn.commit()

    def close(self):
        if self.storage_type == "sqlite":
            self.conn.close()

    async def close_async(self):
        await self._run_sync(self.close)

    async def _get_user_vars(self, user_id):
        uid = str(user_id)
        if self.storage_type == "sqlite":
            row = await self._run_sync(
                lambda: self.conn.cursor()
                .execute("SELECT data FROM vars WHERE user_id = ?", (uid,))
                .fetchone()
            )
            return json.loads(self.cipher.decrypt(row[0])) if row else {}

        if self.storage_type == "mongo":
            doc = await self._run_sync(lambda: self.data.vars.find_one({"_id": uid}))
            return doc.get("data", {}) if doc else {}

        data = await self._load_data()
        return data.get("vars", {}).get(uid, {})

    async def _set_user_vars(self, user_id, user_data):
        uid = str(user_id)
        if self.storage_type == "sqlite":
            enc = self.cipher.encrypt(json.dumps(user_data))
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "INSERT OR REPLACE INTO vars VALUES (?, ?)", (uid, enc)
                    ),
                    self.conn.commit(),
                )
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.vars.update_one(
                    {"_id": uid}, {"$set": {"data": user_data}}, upsert=True
                )
            )
            return

        data = await self._load_data()
        data.setdefault("vars", {})[uid] = user_data
        await self._save_data(data)

    async def setVars(self, user_id, query_name, value, var_key="variabel"):
        val = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        enc = self.cipher.encrypt(val)
        data = await self._get_user_vars(user_id)
        data.setdefault(var_key, {})[query_name] = enc
        await self._set_user_vars(user_id, data)

    async def getVars(self, user_id, query_name, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        enc = data.get(var_key, {}).get(query_name)
        if not enc:
            return None
        dec = self.cipher.decrypt(enc)
        try:
            return json.loads(dec)
        except:
            return dec

    async def removeVars(self, user_id, query_name, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        if data.get(var_key, {}).pop(query_name, None) is not None:
            await self._set_user_vars(user_id, data)

    async def setListVars(self, user_id, query_name, value, var_key="variabel"):
        val = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        enc = self.cipher.encrypt(val)
        data = await self._get_user_vars(user_id)
        data.setdefault(var_key, {}).setdefault(query_name, [])
        if enc not in data[var_key][query_name]:
            data[var_key][query_name].append(enc)
            await self._set_user_vars(user_id, data)

    async def getListVars(self, user_id, query_name, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        res = []
        for v in data.get(var_key, {}).get(query_name, []):
            dec = self.cipher.decrypt(v)
            try:
                res.append(json.loads(dec))
            except:
                res.append(dec)
        return res

    async def removeListVars(self, user_id, query_name, value, var_key="variabel"):
        val = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        enc = self.cipher.encrypt(val)
        data = await self._get_user_vars(user_id)
        try:
            data[var_key][query_name].remove(enc)
            await self._set_user_vars(user_id, data)
        except:
            pass

    async def removeAllVars(self, user_id):
        uid = str(user_id)
        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute("DELETE FROM vars WHERE user_id = ?", (uid,)),
                    self.conn.commit(),
                )
            )
        elif self.storage_type == "mongo":
            await self._run_sync(lambda: self.data.vars.delete_one({"_id": uid}))
        else:
            data = await self._load_data()
            data.get("vars", {}).pop(uid, None)
            await self._save_data(data)

    async def allVars(self, user_id, var_key="variabel"):
        data = await self._get_user_vars(user_id)
        out = {}
        for k, v in data.get(var_key, {}).items():
            if isinstance(v, list):
                out[k] = [json.loads(self.cipher.decrypt(i)) for i in v]
            else:
                out[k] = json.loads(self.cipher.decrypt(v))
        return out

    async def saveBot(self, user_id, api_id, api_hash, value, is_token=False):
        uid = str(user_id)
        field = "bot_token" if is_token else "session_string"
        bot_data = {
            "api_id": self.cipher.encrypt(str(api_id)),
            "api_hash": self.cipher.encrypt(api_hash),
            field: self.cipher.encrypt(value) if value else None,
        }

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.bot.update_one(
                    {"_id": uid}, {"$set": bot_data}, upsert=True
                )
            )
            return

        if self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute(
                        "INSERT OR REPLACE INTO bots VALUES (?, ?, ?, ?, ?)",
                        (
                            uid,
                            bot_data.get("api_id"),
                            bot_data.get("api_hash"),
                            bot_data.get("bot_token"),
                            bot_data.get("session_string"),
                        ),
                    ),
                    self.conn.commit(),
                )
            )
            return

        data = await self._load_data()
        bots = data.get("bots", [])
        bots[:] = [b for b in bots if b.get("user_id") != uid]
        bots.append({"user_id": uid, **bot_data})
        await self._save_data(data)

    async def getBots(self, is_token=False):
        if self.storage_type == "mongo":
            rows = await self._run_sync(lambda: list(self.data.bot.find()))
        elif self.storage_type == "sqlite":
            rows = await self._run_sync(
                lambda: self.conn.cursor()
                .execute("SELECT * FROM bots")
                .fetchall()
            )
            rows = [
                {
                    "user_id": r[0],
                    "api_id": r[1],
                    "api_hash": r[2],
                    "bot_token": r[3],
                    "session_string": r[4],
                }
                for r in rows
            ]
        else:
            rows = (await self._load_data()).get("bots", [])

        out = []
        for r in rows:
            try:
                if is_token and not r.get("bot_token"):
                    continue
                if not is_token and not r.get("session_string"):
                    continue
                out.append(
                    {
                        "name": r.get("user_id") or r.get("_id"),
                        "api_id": int(self.cipher.decrypt(r["api_id"])),
                        "api_hash": self.cipher.decrypt(r["api_hash"]),
                        "bot_token": self.cipher.decrypt(r["bot_token"]) if r.get("bot_token") else None,
                        "session_string": self.cipher.decrypt(r["session_string"]) if r.get("session_string") else None,
                    }
                )
            except:
                continue
        return out

    async def removeBot(self, user_id):
        uid = str(user_id)
        if self.storage_type == "mongo":
            await self._run_sync(lambda: self.data.bot.delete_one({"_id": uid}))
        elif self.storage_type == "sqlite":
            await self._run_sync(
                lambda: (
                    self.conn.execute("DELETE FROM bots WHERE user_id = ?", (uid,)),
                    self.conn.commit(),
                )
            )
        else:
            data = await self._load_data()
            data["bots"] = [b for b in data.get("bots", []) if b.get("user_id") != uid]
            await self._save_data(data)
