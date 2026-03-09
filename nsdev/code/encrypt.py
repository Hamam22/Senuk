import base64
import json
import ast

from ..utils.colorize import AnsiColors
from ..utils.logger import LoggerHandler


class CipherHandler:

    def __init__(self, **options):
        self.method = options.get("method", "shift")
        self.key = self._normalize_key(options.get("key", "my_s3cr3t_k3y_@2024!"))
        self.numeric_key = self._get_numeric_key()
        self.delimiter = options.get("delimiter", "|")

        if not self.key:
            raise ValueError("Key cannot be empty.")

        self.log = LoggerHandler()

    def _normalize_key(self, key):
        if isinstance(key, list):
            return "".join(map(str, key))
        return str(key)

    def _get_numeric_key(self):
        total = sum(ord(c) for c in self.key)
        return "".join(str((ord(c) + total + i) % 10) for i, c in enumerate(self.key))

    def _xor(self, data: bytes):
        key_bytes = self.key.encode()
        return bytes(data[i] ^ key_bytes[i % len(key_bytes)] for i in range(len(data)))

    def _b64e(self, text):
        return base64.b64encode(text.encode()).decode().rstrip("=")

    def _b64d(self, text):
        pad = (4 - len(text) % 4) % 4
        return base64.b64decode(text + "=" * pad).decode()

    def decrypt(self, data, only_base64=False):
        if only_base64:
            return self._b64d(data)

        if self.method == "bytes":
            result = self._decrypt_bytes(data)
        elif self.method == "binary":
            result = self._decrypt_binary(data)
        elif self.method == "shift":
            result = self._decrypt_shift(data)
        else:
            raise ValueError("Invalid method")

        if isinstance(result, (list, dict)):
            return result

        try:
            return json.loads(result)
        except:
            pass

        if isinstance(result, str):
            try:
                parsed = ast.literal_eval(result)
                if isinstance(parsed, list):
                    return parsed
            except:
                pass

        return result

    def _decrypt_bytes(self, data):
        if not isinstance(data, str):
            return data

        s = data.strip()
        if not s:
            return data

        if not all(c in "0123456789abcdefABCDEF" for c in s):
            return data

        try:
            raw = bytes.fromhex(s)
            return self._xor(raw).decode()
        except:
            return data

    def _decrypt_binary(self, data):
        if not isinstance(data, str) or len(data) % 8 != 0:
            return data
        try:
            key_val = int(self.numeric_key) % 256
            return "".join(chr(int(data[i:i+8], 2) ^ key_val) for i in range(0, len(data), 8))
        except:
            return data

    def _decrypt_shift(self, data):
        if not isinstance(data, str):
            return data
        try:
            parts = data.split(self.delimiter)
            return "".join(chr(int(p, 16) - ord(self.key[i % len(self.key)])) for i, p in enumerate(parts))
        except:
            return data

    def encrypt(self, data, only_base64=False):
        if only_base64:
            return self._b64e(data)

        text = json.dumps(data, separators=(",", ":")) if not isinstance(data, str) else data

        if self.method == "bytes":
            return self._encrypt_bytes(text)
        if self.method == "binary":
            return self._encrypt_binary(text)
        if self.method == "shift":
            return self._encrypt_shift(text)

        raise ValueError("Invalid method")

    def _encrypt_bytes(self, text):
        return self._xor(text.encode()).hex()

    def _encrypt_binary(self, text):
        key_val = int(self.numeric_key) % 256
        return "".join(format(ord(c) ^ key_val, "08b") for c in text)

    def _encrypt_shift(self, text):
        return self.delimiter.join(
            hex(ord(text[i]) + ord(self.key[i % len(self.key)]))
            for i in range(len(text))
        )

    def save(self, filename: str, code: str, key_by_config: str = None):
        encrypted_code = self.encrypt(code)

        to_hex = lambda s: s.encode().hex()
        key_expr = key_by_config if key_by_config else repr(self.key)

        hex_map = {
            "n": to_hex("nsdev"),
            "C": to_hex("CipherHandler"),
            "b": to_hex("builtins"),
            "t": to_hex("types"),
            "g": to_hex("globals"),
            "i": to_hex("__import__"),
            "a": to_hex("getattr"),
            "c": to_hex("compile"),
            "f": to_hex("FunctionType"),
            "e": to_hex("eval"),
            "M": to_hex(self.method),
            "K": to_hex(key_expr),
        }

        result = f"(lambda d, h, x: (lambda b, i, g, c, t, f, e: f(c(g(g(i(x(h['n'])), x(h['C']))(**{{'method': x(h['M']), 'key': e(x(h['K']))}}), 'decrypt')(d), '<string>', 'exec'), t())())(__import__(x(h['b'])),lambda n: __import__(x(h['b'])).__dict__[x(h['i'])](n),lambda o, n: __import__(x(h['b'])).__dict__[x(h['a'])](o, n),lambda *a: __import__(x(h['b'])).__dict__[x(h['c'])](*a),lambda: __import__(x(h['b'])).__dict__[x(h['g'])](),lambda *a: __import__(x(h['b'])).__dict__[x(h['a'])](__import__(x(h['t'])), x(h['f']))(*a),lambda s: __import__(x(h['b'])).__dict__[x(h['e'])](s)))('{encrypted_code}', {hex_map}, lambda s: bytes.fromhex(s).decode())"

        with open(filename, "w") as f:
            f.write(result)

        self.log.info(f"Kode berhasil disimpan ke file {filename}")


class AsciiManager(AnsiColors):

    def __init__(self, key):
        super().__init__()
        self.raw_key = key
        self.key = self._normalize_key(key)

        if not self.key:
            raise ValueError("Key cannot be empty.")

    def _normalize_key(self, key):
        if isinstance(key, list):
            return "".join(map(str, key))
        return str(key)

    def _offset(self, i):
        return len(self.key) * (i + 1) + ord(self.key[i % len(self.key)])

    def encrypt(self, data):
        text = json.dumps(data, separators=(",", ":")) if not isinstance(data, str) else data
        return [ord(c) + self._offset(i) for i, c in enumerate(text)]

    def decrypt(self, data):
        if not isinstance(data, list):
            return data
        try:
            text = "".join(chr(int(v) - self._offset(i)) for i, v in enumerate(data))
            try:
                return json.loads(text)
            except:
                return text
        except:
            return data

    def save_data(self, filename: str, code: str, key_by_config: str = None):
        encrypted_code = self.encrypt(code)

        to_hex = lambda s: s.encode().hex()
        key_expr = key_by_config if key_by_config else repr(self.raw_key)

        hex_map = {
            "n": to_hex("nsdev"),
            "A": to_hex("AsciiManager"),
            "b": to_hex("builtins"),
            "t": to_hex("types"),
            "g": to_hex("globals"),
            "i": to_hex("__import__"),
            "a": to_hex("getattr"),
            "c": to_hex("compile"),
            "f": to_hex("FunctionType"),
            "e": to_hex("eval"),
            "K": to_hex(key_expr),
        }

        result = f"(lambda d, h, x: (lambda b, i, g, c, t, f, e: f(c(g(g(i(x(h['n'])), x(h['A']))(e(x(h['K']))), 'decrypt')(d), '<string>', 'exec'), t())())(__import__(x(h['b'])),lambda n: __import__(x(h['b'])).__dict__[x(h['i'])](n),lambda o, n: __import__(x(h['b'])).__dict__[x(h['a'])](o, n),lambda *a: __import__(x(h['b'])).__dict__[x(h['c'])](*a),lambda: __import__(x(h['b'])).__dict__[x(h['g'])](),lambda *a: __import__(x(h['b'])).__dict__[x(h['a'])](__import__(x(h['t'])), x(h['f']))(*a),lambda s: __import__(x(h['b'])).__dict__[x(h['e'])](s)))({str(encrypted_code)}, {hex_map}, lambda s: bytes.fromhex(s).decode())"

        with open(filename, "w") as f:
            f.write(result)

        print(f"{self.GREEN}Kode berhasil disimpan ke file {filename}{self.RESET}")
