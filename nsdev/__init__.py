from types import SimpleNamespace
from typing import Any

from .ai import (
    ChatbotGemini,
    HuggingFaceGenerator,
    ImageGenerator,
    ImageUpscaler,
    OCR,
    QrCodeGenerator,
    SpeechToText,
    TextToSpeech,
    Translator,
    VisionAnalyzer,
    VoiceCloner,
    WebSearch,
    WebSummarizer,
)
from .analytics import AnalyticsManager, ChatAnalyzer
from .auth import AuthManager
from .code import AsciiManager, CipherHandler
from .data import DataBase, KeyManager, YamlHandler
from .payment import (
    PaymentCashify,
    PaymentMidtrans,
    PaymentQRPW,
    PaymentTripay,
    SaweriaApi,
    SaweriaScraper,
    VioletMediaPayClient,
)
from .pinterest import Pinterest
from .schedule import Scheduler
from .server import ProcessManager, ServerMonitor, SpeedtestRunner, SSHUserManager
from .telegram import (
    Argument,
    Button,
    ErrorHandler,
    MessageCopier,
    StoryDownloader,
    TelegramActions,
    TextFormatter,
    VideoFX,
)
from .tempmail import TempMailManager
from .utils import (
    AnsiColors,
    AudioFX,
    AudioSplitter,
    CarbonClient,
    CustomLogHandler,
    FakeInfoGenerator,
    FileManager,
    FontChanger,
    GitHubInfo,
    GoFileUploader,
    Gradient,
    ImageManipulator,
    LoggerHandler,
    MediaDownloader,
    MediaInspector,
    OsintTools,
    PasteClient,
    RateLimiter,
    ShellExecutor,
    TelegramProgressBar,
    TMDbClient,
    UrlUtils,
    WeatherWttr,
    WebAutomation,
    WikipediaSearch,
    memoize,
)

__version__ = "0.40"
__author__ = "@Norsodikin"

__all__ = [
    "NsDev",
    "ns",
]


class NsDev:
    def __init__(self, client: Any) -> None:
        self._client = client

        self.ai = SimpleNamespace(
            bing=ImageGenerator,
            gemini=ChatbotGemini,
            hf=HuggingFaceGenerator,
            ocr=OCR,
            qrcode=QrCodeGenerator(),
            search=WebSearch,
            stt=SpeechToText,
            translate=Translator,
            tts=TextToSpeech,
            upscaler=ImageUpscaler,
            vision=VisionAnalyzer,
            voicecloning=VoiceCloner,
            web=WebSummarizer,
        )

        self.analytics = SimpleNamespace(
            manager=AnalyticsManager,
            chat=ChatAnalyzer,
        )

        self.auth = AuthManager

        self.code = SimpleNamespace(
            ascii=AsciiManager,
            cipher=CipherHandler,
        )

        self.data = SimpleNamespace(
            db=DataBase,
            key=KeyManager,
            yaml=YamlHandler(),
        )

        self.payment = SimpleNamespace(
            cashify=PaymentCashify,
            midtrans=PaymentMidtrans,
            qrpw=PaymentQRPW,
            saweria=SaweriaApi,
            saweria_scraper=SaweriaScraper,
            tripay=PaymentTripay,
            violet=VioletMediaPayClient,
        )

        self.pinterest = Pinterest()
        self.schedule = Scheduler()

        self.server = SimpleNamespace(
            monitor=ServerMonitor(),
            process=ProcessManager(),
            speedtest=SpeedtestRunner(),
            user=SSHUserManager,
        )

        self.telegram = SimpleNamespace(
            actions=TelegramActions(client),
            arg=Argument(client),
            button=Button(),
            copier=MessageCopier(client),
            errors=ErrorHandler(client),
            formatter=TextFormatter,
            story=StoryDownloader(client),
            videofx=VideoFX(),
        )

        self.tempmail = TempMailManager()

        self.utils = SimpleNamespace(
            audiofx=AudioFX(),
            cache=memoize,
            carbon=CarbonClient,
            color=AnsiColors(),
            downloader=MediaDownloader,
            faker=FakeInfoGenerator(),
            files=FileManager(),
            font=FontChanger(),
            github=GitHubInfo,
            gofile=GoFileUploader(),
            grad=Gradient(),
            image=ImageManipulator(),
            log=LoggerHandler,
            lookup=TMDbClient,
            mediainfo=MediaInspector(),
            osint=OsintTools,
            paste=PasteClient,
            progress=TelegramProgressBar,
            ratelimit=RateLimiter(client),
            shell=ShellExecutor(),
            splitter=AudioSplitter,
            url=UrlUtils(),
            weather=WeatherWttr,
            web=WebAutomation(),
            wikipedia=WikipediaSearch,
        )


@property
def ns(self: Any) -> NsDev:
    instance = getattr(self, "_nsdev_instance", None)

    if instance is None:
        instance = NsDev(self)
        setattr(self, "_nsdev_instance", instance)

    return instance


try:
    from pyrogram import Client
except ImportError:
    Client = None

if Client is not None and not hasattr(Client, "ns"):
    Client.ns = ns
