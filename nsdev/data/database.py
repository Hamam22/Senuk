import asyncio
import glob
import json
import os
import shutil
import sqlite3
import zipfile
from datetime import datetime
from functools import partial
from typing import Any
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

        self.auto_backup = options.get("auto_backup", False)
        self.backup_bot_token = options.get("backup_bot_token")
        self.backup_chat_id = options.get("backup_chat_id")
        self.backup_cron_spec = options.get("backup_cron_spec", "0 */3 * * *")
        self.scheduler = options.get("scheduler_instance")

        self.mongo_url = options.get("mongo_url")
        self.client = None
        self.data = None
        self.conn = None

        if self.storage_type == "mongo":
            import pymongo

            self.client = pymongo.MongoClient(self.mongo_url)
            self.data = self.client[self.file_name]

        elif self.storage_type == "sqlite":
            self.db_file = self._normalize_db_filename(self.file_name)
            self.conn = self._connect_sqlite(self.db_file)
            self._initialize_sqlite()

        else:
            self.data_file = self._normalize_json_filename(self.file_name)
            if not os.path.exists(self.data_file):
                with open(self.data_file, "w", encoding="utf-8") as file:
                    json.dump({"vars": {}, "bots": []}, file, indent=4, ensure_ascii=False)

        self._register_backup_task()

    @staticmethod
    def _normalize_db_filename(name: str) -> str:
        return name if str(name).endswith(".db") else f"{name}.db"

    @staticmethod
    def _normalize_json_filename(name: str) -> str:
        return name if str(name).endswith(".json") else f"{name}.json"

    @staticmethod
    def _connect_sqlite(path: str) -> sqlite3.Connection:
        return sqlite3.connect(path, check_same_thread=False)

    async def _run_sync(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    def _safe_json_loads(self, value: Any) -> dict:
        if isinstance(value, dict):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode()
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return {}
        return {}

    def _serialize_value(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)

    def _deserialize_decrypted(self, value: Any) -> Any:
        if isinstance(value, dict):
            return value
        try:
            return json.loads(value)
        except Exception:
            return value

    def _decrypt_value(self, value: Any) -> Any:
        decrypted = self.cipher.decrypt(value)
        return self._deserialize_decrypted(decrypted)

    def _register_backup_task(self) -> None:
        if not self.auto_backup:
            return
        if not self.scheduler:
            return
        if self.storage_type not in {"local", "sqlite"}:
            return
        if not self.backup_bot_token or not self.backup_chat_id:
            return

        @self.scheduler.cron(self.backup_cron_spec)
        async def scheduled_backup_task():
            asyncio.create_task(self.perform_backup())

    def _get_storage_file_path(self) -> str | None:
        if self.storage_type == "local":
            return self.data_file
        if self.storage_type == "sqlite":
            return self.db_file
        return None

    def _initialize_sqlite(self) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS vars (user_id TEXT PRIMARY KEY, data TEXT)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS bots ("
            "user_id TEXT PRIMARY KEY, "
            "api_id TEXT, "
            "api_hash TEXT, "
            "bot_token TEXT, "
            "session_string TEXT)"
        )
        self.conn.commit()

    def _sqlite_fetchone(self, query: str, params: tuple = ()):
        cursor = self.conn.cursor()
        return cursor.execute(query, params).fetchone()

    def _sqlite_fetchall(self, query: str, params: tuple = ()):
        cursor = self.conn.cursor()
        return cursor.execute(query, params).fetchall()

    def _sqlite_execute(self, query: str, params: tuple = ()) -> None:
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self.conn.commit()

    async def close(self) -> None:
        async with self._lock:
            if self.storage_type == "sqlite" and self.conn is not None:
                await self._run_sync(self.conn.close)
                self.conn = None

            if self.storage_type == "mongo" and self.client is not None:
                await self._run_sync(self.client.close)
                self.client = None
                self.data = None

    async def reopen(self) -> None:
        async with self._lock:
            if self.storage_type == "sqlite" and self.conn is None:
                self.conn = self._connect_sqlite(self.db_file)
                self._initialize_sqlite()

            elif self.storage_type == "mongo" and self.client is None:
                import pymongo

                self.client = pymongo.MongoClient(self.mongo_url)
                self.data = self.client[self.file_name]

    def _create_sqlite_snapshot(self, src_db_path: str, dst_db_path: str) -> None:
        src = None
        dst = None
        try:
            src = sqlite3.connect(src_db_path)
            dst = sqlite3.connect(dst_db_path)
            src.backup(dst)
            dst.commit()
        finally:
            if dst is not None:
                dst.close()
            if src is not None:
                src.close()

    async def _create_backup_snapshot(self, src_path: str, dst_path: str) -> None:
        if self.storage_type == "sqlite":
            await self._run_sync(self._create_sqlite_snapshot, src_path, dst_path)
            return

        await self._run_sync(shutil.copy2, src_path, dst_path)

    def _create_zip_archive(self, source_paths: list[str]) -> str:
        timestamp = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d_%H%M%S")
        zip_filename = f"backup_{self.file_name}_{timestamp}.zip"
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in source_paths:
                archive.write(path, os.path.basename(path))
        return zip_filename

    async def _send_zip_to_telegram(self, file_path: str, caption: str) -> None:
        url = f"https://api.telegram.org/bot{self.backup_bot_token}/sendDocument"
        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as file:
                data = aiohttp.FormData()
                data.add_field("chat_id", str(self.backup_chat_id))
                data.add_field("caption", caption)
                data.add_field("parse_mode", "Markdown")
                data.add_field("document", file, filename=os.path.basename(file_path))
                await session.post(url, data=data)

    async def perform_backup(self) -> None:
        async with self._lock:
            db_path = self._get_storage_file_path()

            if not db_path or not await self._run_sync(os.path.exists, db_path):
                return

            temp_backup_dir = "temp_db_backup"
            zip_path = None

            try:
                await self._run_sync(os.makedirs, temp_backup_dir, exist_ok=True)

                temp_db_path = os.path.join(temp_backup_dir, os.path.basename(db_path))
                await self._create_backup_snapshot(db_path, temp_db_path)

                source_paths = [temp_db_path]
                env_files = await self._run_sync(glob.glob, "*.env")
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

    async def _load_data(self) -> dict:
        async with self._lock:
            try:
                async with aiofiles.open(self.data_file, "r", encoding="utf-8") as file:
                    content = await file.read()
                    if not content.strip():
                        return {"vars": {}, "bots": []}
                    return json.loads(content)
            except Exception:
                return {"vars": {}, "bots": []}

    async def _save_data(self, data: dict) -> None:
        async with self._lock:
            temp_file = f"{self.data_file}.tmp"
            async with aiofiles.open(temp_file, "w", encoding="utf-8") as file:
                await file.write(json.dumps(data, indent=4, ensure_ascii=False))
            await self._run_sync(os.replace, temp_file, self.data_file)

    async def _get_user_vars(self, user_id: Any) -> dict:
        user_id_str = str(user_id)

        if self.storage_type == "sqlite":
            row = await self._run_sync(
                self._sqlite_fetchone,
                "SELECT data FROM vars WHERE user_id = ?",
                (user_id_str,),
            )
            if not row:
                return {}
            decrypted = self.cipher.decrypt(row[0])
            return self._safe_json_loads(decrypted)

        if self.storage_type == "mongo":
            data = await self._run_sync(
                lambda: self.data.vars.find_one({"_id": user_id_str})
            )
            return data if data else {}

        data = await self._load_data()
        return data.get("vars", {}).get(user_id_str, {})

    async def _set_user_vars(self, user_id: Any, user_data: dict) -> None:
        user_id_str = str(user_id)

        if self.storage_type == "sqlite":
            encrypted_data = self.cipher.encrypt(json.dumps(user_data, ensure_ascii=False))
            await self._run_sync(
                self._sqlite_execute,
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

    async def setVars(self, user_id: Any, query_name: str, value: Any, var_key: str = "variabel") -> None:
        encrypted_value = self.cipher.encrypt(self._serialize_value(value))
        user_data = await self._get_user_vars(user_id)
        user_data.setdefault(var_key, {})[query_name] = encrypted_value
        await self._set_user_vars(user_id, user_data)

    async def getVars(self, user_id: Any, query_name: str, var_key: str = "variabel") -> Any:
        user_data = await self._get_user_vars(user_id)
        encrypted_value = user_data.get(var_key, {}).get(query_name)
        if not encrypted_value:
            return None
        return self._decrypt_value(encrypted_value)

    async def removeVars(self, user_id: Any, query_name: str, var_key: str = "variabel") -> None:
        user_data = await self._get_user_vars(user_id)
        if user_data.get(var_key, {}).pop(query_name, None) is not None:
            await self._set_user_vars(user_id, user_data)

    async def setListVars(self, user_id: Any, query_name: str, value: Any, var_key: str = "variabel") -> None:
        encrypted_value = self.cipher.encrypt(self._serialize_value(value))
        user_data = await self._get_user_vars(user_id)
        user_data.setdefault(var_key, {}).setdefault(query_name, [])

        if encrypted_value not in user_data[var_key][query_name]:
            user_data[var_key][query_name].append(encrypted_value)
            await self._set_user_vars(user_id, user_data)

    async def getListVars(self, user_id: Any, query_name: str, var_key: str = "variabel") -> list[Any]:
        user_data = await self._get_user_vars(user_id)
        encrypted_list = user_data.get(var_key, {}).get(query_name, [])
        return [self._decrypt_value(item) for item in encrypted_list]

    async def removeListVars(self, user_id: Any, query_name: str, value: Any, var_key: str = "variabel") -> None:
        encrypted_value = self.cipher.encrypt(self._serialize_value(value))
        user_data = await self._get_user_vars(user_id)

        try:
            user_data.get(var_key, {}).get(query_name, []).remove(encrypted_value)
            await self._set_user_vars(user_id, user_data)
        except Exception:
            pass

    async def removeAllVars(self, user_id: Any) -> None:
        user_id_str = str(user_id)

        if self.storage_type == "sqlite":
            await self._run_sync(
                self._sqlite_execute,
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

    async def allVars(self, user_id: Any, var_key: str = "variabel") -> dict[str, Any]:
        user_data = await self._get_user_vars(user_id)
        encrypted_data = user_data.get(var_key, {})
        result: dict[str, Any] = {}

        for key, value in encrypted_data.items():
            if isinstance(value, list):
                result[key] = [self._decrypt_value(item) for item in value]
            else:
                result[key] = self._decrypt_value(value)

        return result

    async def saveBot(
        self,
        user_id: Any,
        api_id: Any,
        api_hash: str,
        value: str | None,
        is_token: bool = False,
    ) -> None:
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
            await self._run_sync(
                self._sqlite_execute,
                (
                    "INSERT OR REPLACE INTO bots "
                    "(user_id, api_id, api_hash, bot_token, session_string) "
                    "VALUES (?, ?, ?, ?, ?)"
                ),
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

        for bot in bots:
            if bot.get("user_id") == user_id_str:
                bot.update(bot_data)
                break
        else:
            bots.append({"user_id": user_id_str, **bot_data})

        full_data["bots"] = bots
        await self._save_data(full_data)

    async def getBots(self, is_token: bool = False) -> list[dict[str, Any]]:
        raw: list[dict[str, Any]] = []

        if self.storage_type == "mongo":
            raw = await self._run_sync(lambda: list(self.data.bot.find()))

        elif self.storage_type == "sqlite":
            rows = await self._run_sync(
                self._sqlite_fetchall,
                "SELECT user_id, api_id, api_hash, bot_token, session_string FROM bots",
            )
            raw = [
                {
                    "user_id": row[0],
                    "api_id": row[1],
                    "api_hash": row[2],
                    "bot_token": row[3],
                    "session_string": row[4],
                }
                for row in rows
            ]

        else:
            raw = (await self._load_data()).get("bots", [])

        result = []
        for item in raw:
            data = {"name": item.get("user_id") or item.get("_id")}

            for key in ("api_id", "api_hash", "bot_token", "session_string"):
                if item.get(key):
                    value = self.cipher.decrypt(item[key])
                    data[key] = int(value) if key == "api_id" else value

            if is_token and "bot_token" in data:
                result.append(data)
            elif not is_token and "session_string" in data:
                result.append(data)

        return result

    async def removeBot(self, user_id: Any) -> None:
        user_id_str = str(user_id)

        if self.storage_type == "mongo":
            await self._run_sync(lambda: self.data.bot.delete_one({"_id": user_id_str}))
            return

        if self.storage_type == "sqlite":
            await self._run_sync(
                self._sqlite_execute,
                "DELETE FROM bots WHERE user_id = ?",
                (user_id_str,),
            )
            return

        data = await self._load_data()
        data["bots"] = [bot for bot in data.get("bots", []) if bot.get("user_id") != user_id_str]
        await self._save_data(data)
