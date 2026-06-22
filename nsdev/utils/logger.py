import datetime
import logging
import os
import sys
import zoneinfo

from .colorize import AnsiColors


class LoggerHandler(AnsiColors):
    def __init__(self, **options):
        super().__init__()

        self.tz = zoneinfo.ZoneInfo(options.get("tz", "Asia/Jakarta"))
        self.fmt = options.get(
            "fmt",
            "{asctime} {levelname} {module}:{funcName}:{lineno} {message}",
        )
        self.datefmt = options.get("datefmt", "%Y-%m-%d %H:%M:%S %Z")
        self.use_color = options.get("use_color", True)

        self.colors = {
            "INFO": self.GREEN,
            "DEBUG": self.BLUE,
            "WARNING": self.YELLOW,
            "ERROR": self.RED,
            "CRITICAL": self.MAGENTA,
            "TIME": self.WHITE,
            "MODULE": self.CYAN,
            "PIPE": self.PURPLE,
            "RESET": self.RESET,
        }

        if not self.use_color:
            self.colors = {key: "" for key in self.colors}

    def formatTime(self):
        utc_time = datetime.datetime.now(datetime.timezone.utc)
        local_time = utc_time.astimezone(self.tz)
        return local_time.strftime(self.datefmt)

    def format(self, record):
        record = dict(record)

        levelname = str(record.get("levelname", "INFO")).upper()
        message = str(record.get("message", ""))
        module = os.path.basename(str(record.get("module", "<unknown>")))
        func_name = str(record.get("funcName", "<unknown>"))
        lineno = int(record.get("lineno", 0) or 0)

        level_color = self.colors.get(levelname, self.colors["RESET"])
        pipe_color = self.colors["PIPE"]
        reset = self.colors["RESET"]

        return self.fmt.format(
            asctime=f"{self.colors['TIME']}[ {self.formatTime()} ]{reset}",
            levelname=f"{pipe_color}│ {level_color}{levelname:<8}{reset}",
            module=f"{pipe_color}│ {self.colors['MODULE']}{module}{reset}",
            funcName=func_name,
            lineno=lineno,
            message=f"{pipe_color}│ {level_color}{message}{reset}",
        )

    def print(self, message, isPrint=True):
        text = (
            f"{self.CYAN}[ {self.WHITE}{self.formatTime()} {self.CYAN}] "
            f"{self.WHITE}│ {message}{self.RESET}"
        )
        if isPrint:
            print(f"\033[2K{text}")
            return None
        return text

    def log(self, level, message):
        try:
            frame = sys._getframe(2)
            filename = os.path.basename(frame.f_globals.get("__file__", "<unknown>"))
            func_name = frame.f_code.co_name
            lineno = frame.f_lineno
        except Exception:
            filename = "<unknown>"
            func_name = "<unknown>"
            lineno = 0

        record = {
            "levelname": str(level).upper(),
            "module": filename,
            "funcName": func_name,
            "lineno": lineno,
            "message": message,
        }

        print(f"\033[2K{self.format(record)}")

    def debug(self, message):
        self.log("DEBUG", message)

    def info(self, message):
        self.log("INFO", message)

    def warning(self, message):
        self.log("WARNING", message)

    def error(self, message):
        self.log("ERROR", message)

    def critical(self, message):
        self.log("CRITICAL", message)


class CustomLogHandler(logging.Handler):
    def __init__(self, **options):
        super().__init__()
        self.formatter_util = LoggerHandler(**options)

    def emit(self, record):
        try:
            custom_record = {
                "levelname": record.levelname,
                "module": record.module,
                "funcName": record.funcName,
                "lineno": record.lineno,
                "message": record.getMessage(),
            }
            print(f"\033[2K{self.formatter_util.format(custom_record)}")
        except Exception:
            self.handleError(record)
