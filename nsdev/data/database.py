import asyncio
import glob
import json
import os
import shutil
import sqlite3
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

import aiofiles
import aiohttp

from ..code.encrypt import CipherHandler


class DataBase:
    def __init__(self, **options):
        self.storage_type = options.get("storage_type", "local")
        self.file_name = options.get("file_name", "database")
        self.keys_encrypt = options.get("keys_encrypt", "default_db_key_12345")
        self.method_encrypt = options.get("method_encrypt", "bytes")
        self.cipher = CipherHandler(key=self.keys_encrypt, method=self.method_encrypt)

        self._lock = asyncio.Lock()
        self._sqlite_lock = asyncio.Lock()

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
            self.db_file = self.file_name if str(self.file_name).endswith(".db") else f"{self.file_name}.db"
            self.conn = sqlite3.connect(
                self.db_file,
                check_same_thread=False,
                isolation_level=None,
            )
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self._initialize_sqlite()

        else:
            self.data_file = self.file_name if str(self.file_name).endswith(".json") else f"{self.file_name}.json"
            if not os.path.exists(self.data_file):
                with open(self.data_file, "w", encoding="utf-8") as f:
                    json.dump({"vars": {}, "bots": []}, f, indent=4, ensure_ascii=False)

        self._register_backup_task()

    async def _run_sync(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    def _safe_json_loads(self, value):
        if isinstance(value, dict):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return {}
        return {}

    def _register_backup_task(self):
        if not (self.auto_backup and self.scheduler and self.storage_type in ["local", "sqlite"]):
            return

        if not self.backup_bot_token or not self.backup_chat_id:
            try:
                self.cipher.log.warning("Auto backup is disabled because token/chat_id is missing.")
            except Exception:
                pass
            return

        @self.scheduler.cron(self.backup_cron_spec)
        async def scheduled_backup_task():
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
                await self._run_sync(shutil.copy2, db_path, temp_db_path)

                source_paths = [temp_db_path]
                env_files = glob.glob("*.env")
                if env_files:
                    source_paths.extend(env_files)

                zip_path = await self._run_sync(self._create_zip_archive, source_paths)

                if zip_path:
                    timestamp = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S %Z")
                    caption = (
                        f"Backup otomatis untuk `{os.path.basename(zip_path)}`\n"
                        f"Tipe DB: `{self.storage_type}`\n"
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
        zip_filename = f"backup_{os.path.basename(self.file_name)}_{timestamp}.zip"
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in source_paths:
                if os.path.exists(path):
                    zf.write(path, os.path.basename(path))
        return zip_filename

    async def _send_zip_to_telegram(self, file_path, caption):
        url = f"https://api.telegram.org/bot{self.backup_bot_token}/sendDocument"
        data = aiohttp.FormData()
        data.add_field("chat_id", str(self.backup_chat_id))
        data.add_field("caption", caption)
        data.add_field("parse_mode", "Markdown")

        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as f:
                data.add_field("document", f, filename=os.path.basename(file_path))
                await session.post(url, data=data)

    async def _load_data(self):
        async with self._lock:
            try:
                async with aiofiles.open(self.data_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    if not content.strip():
                        return {"vars": {}, "bots": []}
                    return json.loads(content)
            except Exception:
                return {"vars": {}, "bots": []}

    async def _save_data(self, data):
        async with self._lock:
            temp_file = f"{self.data_file}.tmp"
            async with aiofiles.open(temp_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=4, ensure_ascii=False))
            os.replace(temp_file, self.data_file)

    def _initialize_sqlite(self):
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS vars (user_id TEXT PRIMARY KEY, data TEXT)"
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bots (
                user_id TEXT PRIMARY KEY,
                api_id TEXT,
                api_hash TEXT,
                bot_token TEXT,
                session_string TEXT
            )
            """
        )

    async def _sqlite_fetchone(self, query, params=()):
        async with self._sqlite_lock:
            return await self._run_sync(
                lambda: self.conn.execute(query, params).fetchone()
            )

    async def _sqlite_fetchall(self, query, params=()):
        async with self._sqlite_lock:
            return await self._run_sync(
                lambda: self.conn.execute(query, params).fetchall()
            )

    async def _sqlite_execute(self, query, params=()):
        async with self._sqlite_lock:
            await self._run_sync(
                lambda: self.conn.execute(query, params)
            )

    async def _get_user_vars(self, user_id):
        user_id_str = str(user_id)

        if self.storage_type == "sqlite":
            row = await self._sqlite_fetchone(
                "SELECT data FROM vars WHERE user_id = ?",
                (user_id_str,),
            )
            if not row or row[0] is None:
                return {}
            try:
                decrypted = self.cipher.decrypt(row[0])
            except Exception:
                return {}
            return self._safe_json_loads(decrypted)

        if self.storage_type == "mongo":
            data = await self._run_sync(lambda: self.data.vars.find_one({"_id": user_id_str}))
            return data if data else {}

        data = await self._load_data()
        return data.get("vars", {}).get(user_id_str, {})

    async def _set_user_vars(self, user_id, user_data):
        user_id_str = str(user_id)

        if self.storage_type == "sqlite":
            encrypted_data = self.cipher.encrypt(json.dumps(user_data, ensure_ascii=False))
            await self._sqlite_execute(
                "INSERT OR REPLACE INTO vars (user_id, data) VALUES (?, ?)",
                (user_id_str, encrypted_data),
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.vars.update_one(
                    {"_id": user_id_str},
                    {"$set": user_data},
                    upsert=True,
                )
            )
            return

        full_data = await self._load_data()
        full_data.setdefault("vars", {})[user_id_str] = user_data
        await self._save_data(full_data)

    async def setVars(self, user_id, query_name, value, var_key="variabel"):
        val_str = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        encrypted_value = self.cipher.encrypt(val_str)
        user_data = await self._get_user_vars(user_id)
        user_data.setdefault(var_key, {})[query_name] = encrypted_value
        await self._set_user_vars(user_id, user_data)

    async def getVars(self, user_id, query_name, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted_value = user_data.get(var_key, {}).get(query_name)
        if not encrypted_value:
            return None

        try:
            decrypted = self.cipher.decrypt(encrypted_value)
        except Exception:
            return None

        if isinstance(decrypted, dict):
            return decrypted

        try:
            return json.loads(decrypted)
        except Exception:
            return decrypted

    async def removeVars(self, user_id, query_name, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        if user_data.get(var_key, {}).pop(query_name, None) is not None:
            await self._set_user_vars(user_id, user_data)

    async def setListVars(self, user_id, query_name, value, var_key="variabel"):
        val_str = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        encrypted_value = self.cipher.encrypt(val_str)
        user_data = await self._get_user_vars(user_id)
        user_data.setdefault(var_key, {}).setdefault(query_name, [])
        if encrypted_value not in user_data[var_key][query_name]:
            user_data[var_key][query_name].append(encrypted_value)
            await self._set_user_vars(user_id, user_data)

    async def getListVars(self, user_id, query_name, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted_list = user_data.get(var_key, {}).get(query_name, [])
        result = []

        for item in encrypted_list:
            try:
                decrypted = self.cipher.decrypt(item)
                if isinstance(decrypted, dict):
                    result.append(decrypted)
                else:
                    try:
                        result.append(json.loads(decrypted))
                    except Exception:
                        result.append(decrypted)
            except Exception:
                continue

        return result

    async def removeListVars(self, user_id, query_name, value, var_key="variabel"):
        val_str = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        encrypted_value = self.cipher.encrypt(val_str)
        user_data = await self._get_user_vars(user_id)
        try:
            user_data.get(var_key, {}).get(query_name, []).remove(encrypted_value)
            await self._set_user_vars(user_id, user_data)
        except Exception:
            pass

    async def removeAllVars(self, user_id):
        user_id_str = str(user_id)

        if self.storage_type == "sqlite":
            await self._sqlite_execute(
                "DELETE FROM vars WHERE user_id = ?",
                (user_id_str,),
            )
            return

        if self.storage_type == "mongo":
            await self._run_sync(lambda: self.data.vars.delete_one({"_id": user_id_str}))
            return

        full_data = await self._load_data()
        full_data.get("vars", {}).pop(user_id_str, None)
        await self._save_data(full_data)

    async def allVars(self, user_id, var_key="variabel"):
        user_data = await self._get_user_vars(user_id)
        encrypted_data = user_data.get(var_key, {})
        decrypted = {}

        for key, value in encrypted_data.items():
            if isinstance(value, list):
                temp = []
                for v in value:
                    try:
                        d = self.cipher.decrypt(v)
                        try:
                            temp.append(json.loads(d))
                        except Exception:
                            temp.append(d)
                    except Exception:
                        continue
                decrypted[key] = temp
            else:
                try:
                    d = self.cipher.decrypt(value)
                    try:
                        decrypted[key] = json.loads(d)
                    except Exception:
                        decrypted[key] = d
                except Exception:
                    continue

        return decrypted

    async def saveBot(self, user_id, api_id, api_hash, value, is_token=False):
        user_id_str = str(user_id)
        field = "bot_token" if is_token else "session_string"

        bot_data = {
            "api_id": self.cipher.encrypt(str(api_id)),
            "api_hash": self.cipher.encrypt(api_hash),
        }
        if value:
            bot_data[field] = self.cipher.encrypt(value)

        if self.storage_type == "mongo":
            await self._run_sync(
                lambda: self.data.bot.update_one(
                    {"_id": user_id_str},
                    {"$set": bot_data},
                    upsert=True,
                )
            )
            return

        if self.storage_type == "sqlite":
            await self._sqlite_execute(
                """
                INSERT OR REPLACE INTO bots
                (user_id, api_id, api_hash, bot_token, session_string)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_id_str,
                    bot_data["api_id"],
                    bot_data["api_hash"],
                    bot_data.get("bot_token"),
                    bot_data.get("session_string"),
                ),
            )
            return

        full_data = await self._load_data()
        bots = full_data.get("bots", [])
        for b in bots:
            if b.get("user_id") == user_id_str:
                b.update(bot_data)
                break
        else:
            bots.append({"user_id": user_id_str, **bot_data})
        full_data["bots"] = bots
        await self._save_data(full_data)

    async def getBots(self, is_token=False):
        raw = []

        if self.storage_type == "mongo":
            raw = await self._run_sync(lambda: list(self.data.bot.find()))

        elif self.storage_type == "sqlite":
            rows = await self._sqlite_fetchall(
                "SELECT user_id, api_id, api_hash, bot_token, session_string FROM bots"
            )
            raw = [
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
            raw = (await self._load_data()).get("bots", [])

        result = []
        for b in raw:
            data = {"name": b.get("user_id") or b.get("_id")}
            for k in ["api_id", "api_hash", "bot_token", "session_string"]:
                if b.get(k):
                    try:
                        val = self.cipher.decrypt(b[k])
                        data[k] = int(val) if k == "api_id" else val
                    except Exception:
                        continue

            if (is_token and "bot_token" in data) or (not is_token and "session_string" in data):
                result.append(data)

        return result

    async def removeBot(self, user_id):
        user_id_str = str(user_id)

        if self.storage_type == "mongo":
            await self._run_sync(lambda: self.data.bot.delete_one({"_id": user_id_str}))
            return

        if self.storage_type == "sqlite":
            await self._sqlite_execute(
                "DELETE FROM bots WHERE user_id = ?",
                (user_id_str,),
            )
            return

        data = await self._load_data()
        data["bots"] = [b for b in data.get("bots", []) if b.get("user_id") != user_id_str]
        await self._save_data(data)

    async def close(self):
        if self.storage_type == "sqlite" and getattr(self, "conn", None):
            async with self._sqlite_lock:
                await self._run_sync(self.conn.close)
