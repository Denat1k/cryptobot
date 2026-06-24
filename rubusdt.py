import asyncio
import aiohttp
import time
import hmac
import hashlib
import os
import logging
import re
import json
import random
import string

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ====== Конфигурация ======
BOT_TOKEN     = "8906311793:AAFyen6qpsoKAKbtFTxmB47JxBSz8rMwCSY"
TS_API_KEY    = os.getenv("TS_API_KEY", "")
TS_API_SECRET = os.getenv("TS_API_SECRET", "")
TS_RECV_WINDOW = "50000"

PAIR_DISPLAY = "USDT/RUB"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ratebot")


# ====== HTTP helper ======
async def fetch(session, url, method="GET", headers=None, json_body=None, timeout=5):
    try:
        async with session.request(method, url, headers=headers, json=json_body,
                                   timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return await r.json(content_type=None)
    except Exception as e:
        return {"_error": str(e)}


# ====== Биржи ======
async def rapira(session):
    data = await fetch(session, "https://api.rapira.net/open/market/rates")
    if "_error" in data:
        return ("Rapira", None, data["_error"])
    for item in data.get("data", []):
        if item.get("symbol") == "USDT/RUB":
            return ("Rapira", float(item.get("close") or item.get("lastPrice")), None)
    return ("Rapira", None, "пара не найдена")


def _sockjs_session():
    """Случайный 8-символьный session-id для SockJS."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


async def investing(session):
    """
    Investing.com — стрим по SockJS WebSocket. Подключаемся, подписываемся
    на pair id (USDT/RUB = 1208082), ждём первый дата-кадр и закрываем сокет.
    """
    server = random.randint(100, 999)
    sess = _sockjs_session()
    url = f"wss://streaming.forexpros.com/echo/{server}/{sess}/websocket"
    headers = {
        "Origin": "https://www.investing.com",
        "User-Agent": "Mozilla/5.0",
    }

    try:
        async with session.ws_connect(url, headers=headers, timeout=5,
                                       heartbeat=20) as ws:
            # подписка на пару (Investing pid 1208082 = USDT/RUB)
            sub = json.dumps([json.dumps({
                "_event": "bulk-subscribe",
                "tzID": "8",
                "message": "pid-1208082:",
            })])
            await ws.send_str(sub)

            # ждём данные не дольше 5 сек
            deadline = time.time() + 5
            async for msg in ws:
                if time.time() > deadline:
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                raw = msg.data
                # SockJS обёртки: 'o' = open, 'h' = heartbeat, 'a[...]' = data
                if not raw.startswith("a"):
                    continue
                # внутри a[...] лежит массив строк, каждая — JSON
                try:
                    frames = json.loads(raw[1:])  # массив строк
                except json.JSONDecodeError:
                    continue
                for frame in frames:
                    # frame: '{"message":"pid-1208082::{\"pid\":...,\"last_numeric\":74.12,...}"}'
                    try:
                        outer = json.loads(frame)
                        m = outer.get("message", "")
                        # внутренний JSON после "pid-1208082::"
                        inner_json = m.split("::", 1)[-1]
                        inner = json.loads(inner_json)
                    except (json.JSONDecodeError, AttributeError):
                        continue
                    price = inner.get("last_numeric")
                    if price:
                        await ws.close()
                        return ("Investing", float(price), None)
            return ("Investing", None, "не получили last_numeric за 5с")
    except Exception as e:
        return ("Investing", None, str(e))
async def exmo(session):
    data = await fetch(session, "https://api.exmo.me/v1.1/ticker")
    if "_error" in data:
        return ("EXMO.me", None, data["_error"])
    t = data.get("USDT_RUB")
    if t:
        return ("EXMO.me", float(t["last_trade"]), None)
    return ("EXMO.me", None, "пара не найдена")


async def yobit(session):
    data = await fetch(session, "https://yobit.net/api/3/ticker/usdt_rur")
    if "_error" in data:
        return ("YoBit", None, data["_error"])
    t = data.get("usdt_rur")
    if t:
        return ("YoBit", float(t["last"]), None)
    return ("YoBit", None, "пара не найдена")


async def free2ex(session):
    url = "https://cryptottlivewebapi.free2ex.net:8443/api/v2/public/ticker/USDTRUB"
    data = await fetch(session, url)
    if "_error" in data:
        return ("Free2ex", None, data["_error"])
    if isinstance(data, list) and data:
        data = data[0]
    price = data.get("LastBuyPrice") or data.get("LastSellPrice") \
            or data.get("BestBid") or data.get("BestAsk")
    if price:
        return ("Free2ex", float(price), None)
    return ("Free2ex", None, f"нет цены: {str(data)[:80]}")


async def tokenspot(session):
    if not TS_API_KEY or not TS_API_SECRET:
        return ("TokenSpot", None, "нет API-ключа")
    endpoint = "/api/v1/spot/ticker"
    query = "symbol=usdtrub"
    ts = str(int(time.time() * 1000))
    string_to_sign = f"{ts}{TS_API_KEY}{TS_RECV_WINDOW}{query}"
    sign = hmac.new(TS_API_SECRET.encode(), string_to_sign.encode(),
                    hashlib.sha256).hexdigest()
    headers = {
        "TS-API-API-KEY": TS_API_KEY,
        "TS-API-TIMESTAMP": ts,
        "TS-API-RECV-WINDOW": TS_RECV_WINDOW,
        "TS-API-SIGN": sign,
        "Accept": "application/json",
    }
    url = f"https://api.tokenspot.com{endpoint}?{query}"
    data = await fetch(session, url, headers=headers)
    if "_error" in data:
        return ("TokenSpot", None, data["_error"])
    price = data.get("last") or data.get("lastPrice") or data.get("close")
    if price:
        return ("TokenSpot", float(price), None)
    return ("TokenSpot", None, f"нет цены: {str(data)[:80]}")


async def whitebird(session):
    url = "https://admin-service.whitebird.io/api/v1/exchange/calculation"
    amount_rub = 80000
    body = {
        "currencyPair": {"fromCurrency": "RUB", "toCurrency": "USDT_TRC"},
        "calculation": {"inputAsset": amount_rub},
        "paymentInfo": {"paymentToken": ""},
        "providerType": "ASSIST",
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://sdk.whitebird.io",
        "Referer": "https://sdk.whitebird.io/",
    }
    data = await fetch(session, url, method="POST", headers=headers, json_body=body)
    if "_error" in data:
        return ("Whitebird*", None, data["_error"])
    out = (data.get("calculation", {}).get("outputAsset")
           or data.get("outputAsset")
           or data.get("toAmount")
           or data.get("result", {}).get("outputAsset"))
    rate = (data.get("exchangeRate")
            or data.get("rate")
            or data.get("calculation", {}).get("exchangeRate"))
    if out:
        return ("Whitebird*", amount_rub / float(out), None)
    if rate:
        return ("Whitebird*", float(rate), None)
    return ("Whitebird*", None, f"структура: {str(data)[:120]}")


async def cifra_broker(session):
    url = "https://api.cifra-broker.by/api/site/ticker-calculator?key=1"
    raw = await fetch(session, url)
    if "_error" in raw:
        return ("Cifra*", None, raw["_error"])
    if not raw.get("success"):
        return ("Cifra*", None, f"success=false: {raw.get('message')}")
    data = raw.get("data") or {}
    real = data.get("currenciesReal", [])
    rate_rur = next((c["rate"]["value"] for c in real
                     if c.get("code") in ("RUR", "RUB") and c.get("rate")), None)
    rate_usd = next((c["rate"]["value"] for c in real
                     if c.get("code") == "USD" and c.get("rate")), None)
    if not rate_rur or not rate_usd:
        return ("Cifra*", None, f"нет RUR/USD (real={len(real)})")
    return ("Cifra*", float(rate_rur) / float(rate_usd), None)


async def bynex(session):
    url = "https://bynex.io/trading/ru/api/rate/USDT-RUB"
    data = await fetch(session, url)
    if "_error" in data:
        return ("Bynex", None, data["_error"])
    price = data.get("last") or data.get("price")
    if price:
        return ("Bynex", float(price), None)
    return ("Bynex", None, f"нет цены: {str(data)[:80]}")


# ====== Сбор всех курсов ======
async def get_all_rates():
    t0 = time.perf_counter()
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as s:
        results = await asyncio.gather(
            rapira(s), exmo(s), yobit(s), free2ex(s),
            tokenspot(s), whitebird(s), cifra_broker(s), bynex(s),    investing(s)

        )
    dt = (time.perf_counter() - t0) * 1000
    return results, dt


def _esc(s: str) -> str:
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def format_rates_message(results, dt):
    """Форматирует результаты в HTML-сообщение с выделением min/max и добавляет дату/время."""
    import datetime

    now = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    lines = [
        f"💱 <b>Курс {_esc(PAIR_DISPLAY)}</b>  <i>(собрано за {dt:.0f} мс)</i>",
        f"<i>Данные на</i>: <code>{now}</code>",
        ""
    ]
    prices = []
    errors = []

    for name, price, err in results:
        if price:
            prices.append((name, price))
        else:
            errors.append((name, err))

    market_prices = [(n, p) for n, p in prices if not n.endswith("*")]
    min_val = min(market_prices, key=lambda x: x[1])[1] if market_prices else None
    max_val = max(market_prices, key=lambda x: x[1])[1] if market_prices else None

    for name, price in prices:
        name_html = _esc(f"{name:<11}")
        price_html = f"{price:>9.4f} ₽"
        marker = ""
        if not name.endswith("*") and min_val is not None and max_val is not None:
            if abs(price - min_val) < 1e-4:
                marker = " 🟢"
            elif abs(price - max_val) < 1e-4:
                marker = " 🔴"
        lines.append(f"<code>{name_html}</code> <b>{price_html}</b>{marker}")

    if prices:
        if market_prices:
            avg = sum(p for _, p in market_prices) / len(market_prices)
            spread = (max_val - min_val) if (min_val is not None and max_val is not None) else 0
            lines.append("")
            lines.append(f"📊 <b>Биржевая средняя:</b> <code>{avg:.4f} ₽</code>")
            lines.append(
                f"📈 <b>Спред:</b> <code>{spread:.4f} ₽</code>  "
                f"<i>(min <code>{min_val:.4f}</code> / max <code>{max_val:.4f}</code>)</i>"
            )
        lines.append("\n<i>* — обменник/брокер, не биржа</i>")

    if errors:
        lines.append("\n⚠️ <i>Недоступны:</i>")
        for name, err in errors:
            short_err = _esc((err or "")[:50])
            lines.append(f"  • <code>{_esc(name)}</code> — {short_err}")

    return "\n".join(lines)


# ====== Хэндлеры бота ======
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 Привет! Я слежу за курсом *USDT/RUB* по биржам СНГ.\n\n"
        "Команды:\n"
        "/rate — получить актуальный курс\n"
        "/help — справка"
    )


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "*Поддерживаемые площадки:*\n"
        "🟢 *Биржи:* Rapira, EXMO\\.me, YoBit, Free2ex, TokenSpot, Bynex\n"
        "🟡 *Обменник/брокер:* Whitebird, Cifra\n\n"
        "Используй /rate чтобы увидеть курс по всем сразу\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


@dp.message(Command("rate"))
async def cmd_rate(message: types.Message):
    status = await message.answer("⏳ Опрашиваю биржи\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
    try:
        results, dt = await get_all_rates()
        text = format_rates_message(results, dt)
        await status.edit_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.exception("rate failed")
        await status.edit_text(f"❌ Ошибка: `{e}`")


# ====== Запуск ======
async def main():
    if BOT_TOKEN.startswith("ВСТАВЬ"):
        raise RuntimeError("Установи переменную окружения BOT_TOKEN")
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )
    # ставим команды в меню Telegram
    await bot.set_my_commands([
        types.BotCommand(command="rate", description="Курс USDT/RUB"),
        types.BotCommand(command="help", description="Справка"),
    ])
    log.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())