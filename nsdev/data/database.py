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
                    json.dump({"vars": {}, "bots": []}, f)

        self._register_backup_task()

    async def _run_sync(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    def _safe_json_loads(self, value):
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode(errors="ignore")
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return json.loads(value)
            except Exception:
                return value
        return value

    def _ensure_str(self, value):
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    def _register_backup_task(self):
        if self.auto_backup and self.scheduler and self.storage_type in ["local", "sqlite"]:
            if not self.backup_bot_token or not self.backup_chat_id:
                return

            @self.scheduler.cron(self.backup_cron_spec)
            async def scheduled_backup_task():
                asyncio.create_task(self.perform_backup())

    async def perform_backup(self):
        async with self._lock:
            db_path = self.data_file if self.storage_type == "local" else self.db_file
            if not await self._run_sync(os.path.exists, db_path):
                return

            temp_backup_dir = "temp_db_backup"
            os.makedirs(temp_backup_dir, exist_ok=True)

            zip_path = None
            try:
                temp_db_path = os.path.join(temp_backup_dir, os.path.basename(db_path))
                await self._run_sync(shutil.copy2, db_path, temp_db_path)

                source_paths = [temp_db_path]
                env_files = await self._run_sync(glob.glob, "*.env")
                source_paths.extend(env_files or [])

                zip_path = self._create_zip_archive(source_paths)
                if zip_path:
                    timestamp = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S %Z")
                    caption = (
                        f"Backup `{os.path.basename(zip_path)}`\n"
                        f"DB: `{self.storage_type}`\n"
                        f"Waktu: `{timestamp}`"
                    )
                    await self._send_zip_to_telegram(zip_path, caption)
            finally:
                await self._run_sync(shutil.rmtree, temp_backup_dir, ignore_errors=True)
                if zip_path and os.path.exists(zip_path):
                    try:
                        os.remove(zip_path)
                    except Exception:
                        pass

    def _create_zip_archive(self, source_paths):
        timestamp = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d_%H%M%S")
        zip_filename = f"backup_{self.file_name}_{timestamp}.zip"
        try:
            with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
                for path in source_paths:
                    zf.write(path, os.path.basename(path))
            return zip_filename
        except Exception:
            return None

    async def _send_zip_to_telegram(self, file_path, caption):
        url = f"https://api.telegram.org/bot{self.backup_bot_token}/sendDocument"
        data = aiohttp.FormData()
        data.add_field("chat_id", str(self.backup_chat_id))
        data.add_field("caption", caption)
        data.add_field("parse_mode", "Markdown")
        data.add_field("document", open(file_path, "rb"))

        async with aiohttp.ClientSession() as session:
            try:
                await session.post(url, data=data)
            except Exception:
                pass

    async def _load_data(self):
        async with self._lock:
            try:
                async with aiofiles.open(self.data_file, "r") as f:
                    content = await f.read()
                    return json.loads(content) if content.strip() else {"vars": {}, "bots": []}
            except Exception:
                return {"vars": {}, "bots": []}

    async def _save_data(self, data):
        async with self._lock:
            temp_file = f"{self.data_file}.tmp"
            async with aiofiles.open(temp_file, "w") as f:
                await f.write(json.dumps(data, ensure_ascii=False))
            await self._run_sync(os.replace, temp_file, self.data_file)

    def _initialize_sqlite(self):
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS vars (user_id TEXT PRIMARY KEY, data TEXT)")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS bots (user_id TEXT PRIMARY KEY, api_id TEXT, api_hash TEXT, bot_token TEXT, session_string TEXT)"
        )
        self.conn.commit()

    async def _get_user_vars(self, user_id):
        uid = str(user_id)

        if self.storage_type == "sqlite":
            row = await self._run_sync(
                lambda: self.conn.cursor()
                .execute("SELECT data FROM vars WHERE user_id = ?", (uid,))
                .fetchone()
            )
            if not row or not row[0]:
                return {}
            decrypted = self.cipher.decrypt(row[0])
            return self._safe_json_loads(decrypted) or {}

        if self.storage_type == "mongo":
            data = await self._run_sync(lambda: self.data.vars.find_one({"_id": uid}))
            return data or {}

        data = await self._load_data()
        return data.get("vars", {}).get(uid, {})

    async def _set_user_vars(self, user_id, user_data):
        uid = str(user_id)
        payload = self._ensure_str(user_data)
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
                    {"_id": uid}, {"$set": user_data}, upsert=True
                )
            )
            return

        full = await self._load_data()
        full.setdefault("vars", {})[uid] = user_data
        await self._save_data(full)

    async def setVars(self, user_id, query_name, value, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        val_str = self._ensure_str(value)
        encrypted_value = self.cipher.encrypt(val_str)
        user_data.setdefault(var_key, {})[query_name] = encrypted_value
        await self._set_user_vars(user_id, user_data)

    async def getVars(self, user_id, query_name, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted_value = user_data.get(var_key, {}).get(query_name)
        if not encrypted_value:
            return None
        decrypted = self.cipher.decrypt(encrypted_value)
        return self._safe_json_loads(decrypted)

    async def removeVars(self, user_id, query_name, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        if user_data.get(var_key, {}).pop(query_name, None):
            await self._set_user_vars(user_id, user_data)

    async def setListVars(self, user_id, query_name, value, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        val_str = self._ensure_str(value)
        encrypted = self.cipher.encrypt(val_str)
        user_data.setdefault(var_key, {}).setdefault(query_name, [])
        if encrypted not in user_data[var_key][query_name]:
            user_data[var_key][query_name].append(encrypted)
            await self._set_user_vars(user_id, user_data)

    async def getListVars(self, user_id, query_name, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted_list = user_data.get(var_key, {}).get(query_name, [])
        out = []
        for v in encrypted_list:
            decrypted = self.cipher.decrypt(v)
            out.append(self._safe_json_loads(decrypted))
        return out

    async def removeListVars(self, user_id, query_name, value, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted = self.cipher.encrypt(self._ensure_str(value))
        try:
            user_data.get(var_key, {}).get(query_name, []).remove(encrypted)
            await self._set_user_vars(user_id, user_data)
        except Exception:
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
            full = await self._load_data()
            full.get("vars", {}).pop(uid, None)
            await self._save_data(full)

    async def allVars(self, user_id, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted = user_data.get(var_key, {})
        out = {}
        for k, v in encrypted.items():
            if isinstance(v, list):
                out[k] = [self._safe_json_loads(self.cipher.decrypt(x)) for x in v]
            else:
                out[k] = self._safe_json_loads(self.cipher.decrypt(v))
        return out
