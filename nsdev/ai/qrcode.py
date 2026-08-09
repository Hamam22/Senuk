import asyncio
import colorsys
import io
import math
import random
from typing import Optional, Union
from urllib.parse import urlencode

import qrcode
from PIL import Image, ImageDraw
from pyzbar import pyzbar
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers import CircleModuleDrawer

from ..utils.font_manager import FontManager


class QrCodeGenerator(FontManager):
    QR_GENERATOR_URL = "https://larabert-qrgen.hf.space/v1/create-qr-code"

    def __init__(self):
        super().__init__()

    @staticmethod
    def _validate_data(data: str) -> str:
        data = str(data or "").strip()
        if not data:
            raise ValueError("Data QR tidak boleh kosong.")
        return data

    def generate_stylish_qr(
        self,
        data: str,
        size: str = "700x700",
        style: int = 1,
        color: str = "000000",
    ) -> str:
        data = self._validate_data(data)
        size = str(size or "700x700").strip()
        color = str(color or "000000").strip().lstrip("#")

        if "x" not in size.lower():
            size = "700x700"

        if not color:
            color = "000000"

        query = urlencode({
            "size": size,
            "style": max(1, int(style or 1)),
            "color": color,
            "data": data,
        })
        return f"{self.QR_GENERATOR_URL}?{query}"

    def _sync_create_glow_background(
        self,
        size: int,
        color: tuple[int, int, int],
    ) -> Image.Image:
        background = Image.new("RGB", (size, size))
        draw = ImageDraw.Draw(background)
        center = size / 2
        max_dist = math.sqrt(center**2 + center**2)

        for y in range(size):
            for x in range(size):
                distance = math.sqrt((x - center) ** 2 + (y - center) ** 2)
                intensity = max(0, 1 - (distance / max_dist) ** 2)
                draw.point(
                    (x, y),
                    fill=tuple(int(value * intensity) for value in color),
                )

        return background

    def _sync_generate(
        self,
        data: str,
        use_dots: bool,
        glow_background: bool,
        bottom_text: Optional[str] = None,
        creator_text: Optional[str] = None,
    ) -> bytes:
        data = self._validate_data(data)

        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=10,
            border=4,
        )
        qr.add_data(data)
        qr.make(fit=True)

        drawer = CircleModuleDrawer() if use_dots else None

        if glow_background:
            qr_img = qr.make_image(
                image_factory=StyledPilImage,
                module_drawer=drawer,
                fill_color="black",
                back_color=(0, 0, 0, 0),
            ).convert("RGBA")

            qr_size = qr_img.width
            padding = qr_size // 5
            bg_size = qr_size + padding * 2

            rgb = colorsys.hsv_to_rgb(
                random.random(),
                0.95,
                1.0,
            )
            glow_color = tuple(int(value * 255) for value in rgb)

            background = self._sync_create_glow_background(
                bg_size,
                glow_color,
            ).convert("RGBA")

            background.paste(
                qr_img,
                (padding, padding),
                qr_img,
            )
            img = background.convert("RGB")

        else:
            kwargs = {
                "fill_color": "black",
                "back_color": "white",
            }

            if use_dots:
                kwargs.update({
                    "image_factory": StyledPilImage,
                    "module_drawer": drawer,
                })

            img = qr.make_image(**kwargs).convert("RGB")

        if not bottom_text and not creator_text:
            return self._image_to_bytes(img)

        return self._add_footer(
            img,
            bottom_text,
            creator_text,
        )

    @staticmethod
    def _image_to_bytes(image: Image.Image) -> bytes:
        stream = io.BytesIO()
        image.save(stream, format="PNG")
        return stream.getvalue()

    def _add_footer(
        self,
        img: Image.Image,
        bottom_text: Optional[str],
        creator_text: Optional[str],
    ) -> bytes:
        font_button = self._get_font_from_package(
            "NotoSans-Bold.ttf",
            30,
        )
        font_creator = self._get_font_from_package(
            "NotoSans-Regular.ttf",
            40,
        )

        button_w = (
            font_button.getbbox(bottom_text)[2] + 80
            if bottom_text
            else 0
        )
        creator_w = (
            font_creator.getbbox(creator_text)[2]
            if creator_text
            else 0
        )

        horizontal_padding = 40
        canvas_w = max(
            img.width,
            button_w,
            creator_w,
        ) + horizontal_padding * 2

        if canvas_w % 2:
            canvas_w += 1

        padding_top = 40
        qr_button_gap = 30
        button_creator_gap = 20
        padding_bottom = 40

        button_h = 60 if bottom_text else 0
        creator_h = 0

        if creator_text:
            bbox = font_creator.getbbox(creator_text)
            creator_h = bbox[3] - bbox[1]

        total_h = (
            padding_top
            + img.height
            + (qr_button_gap if bottom_text else 0)
            + button_h
            + (button_creator_gap if creator_text else 0)
            + creator_h
            + padding_bottom
        )

        canvas = Image.new(
            "RGB",
            (canvas_w, total_h),
            "#F0F0F0",
        )
        draw = ImageDraw.Draw(canvas)

        canvas.paste(
            img,
            (
                (canvas_w - img.width) // 2,
                padding_top,
            ),
        )

        current_y = padding_top + img.height

        if bottom_text:
            current_y += qr_button_gap
            button_x = (canvas_w - button_w) / 2

            draw.rounded_rectangle(
                (
                    button_x,
                    current_y,
                    button_x + button_w,
                    current_y + button_h,
                ),
                radius=30,
                fill="#F0F0F0",
                outline="black",
                width=2,
            )

            draw.text(
                (
                    button_x + button_w / 2,
                    current_y + button_h / 2,
                ),
                bottom_text,
                font=font_button,
                fill="black",
                anchor="mm",
            )

            current_y += button_h

        if creator_text:
            current_y += button_creator_gap
            draw.text(
                (
                    (canvas_w - creator_w) / 2,
                    current_y,
                ),
                creator_text,
                font=font_creator,
                fill="black",
            )

        return self._image_to_bytes(canvas)

    async def generate(
        self,
        data: str,
        use_dots: bool = True,
        glow_background: bool = False,
        bottom_text: Optional[str] = None,
        creator_text: Optional[str] = None,
    ) -> bytes:
        return await asyncio.to_thread(
            self._sync_generate,
            data,
            use_dots,
            glow_background,
            bottom_text,
            creator_text,
        )

    def _sync_read(
        self,
        image_data: Union[str, bytes, io.BytesIO],
    ) -> Optional[str]:
        if isinstance(image_data, bytes):
            image_data = io.BytesIO(image_data)

        image = Image.open(image_data)
        decoded = pyzbar.decode(image)

        if not decoded:
            return None

        return decoded[0].data.decode("utf-8")

    async def read(
        self,
        image_data: Union[str, bytes, io.BytesIO],
    ) -> Optional[str]:
        return await asyncio.to_thread(
            self._sync_read,
            image_data,
        )
