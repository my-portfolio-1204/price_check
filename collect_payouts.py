"""Збирач історії виплат (payout %) по не-OTC валютних парах Pocket Option.

Тільки читання: підключається без входу в акаунт до демо-сервера платформи
(той самий WebSocket, що й у веб-терміналі) і зберігає виплати в PostgreSQL.
Адреса бази береться зі змінної середовища DATABASE_URL.

Команди:
    python collect_payouts.py run                  # постійний збір
    python collect_payouts.py once                 # один знімок на екран, без запису
    python collect_payouts.py report [--date D]    # добовий звіт (типово за вчора, UTC)
"""

import argparse
import asyncio
import json
import os
import signal
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
import websockets
from psycopg.types.json import Jsonb

URL = "wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket"
ORIGIN = "https://pocketoption.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
AUTH = '42["auth",{"session":"anonymous","isDemo":1,"uid":0,"platform":1}]'

# Обов'язкові пари: якщо сервер пару не віддав, у знімок іде рядок з порожньою виплатою.
REQUIRED = ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF"]

RAW_DAYS = 5  # перші доби зберігаємо й сирий запис (для хвилинних знімків і змін)
SNAPSHOT_TIMEOUT = 8
LISTENER_MIN_INTERVAL = 5
# Стільки знімків поспіль з відмовою сервера чи незнайомим форматом — і збір зупиняється.
FATAL_AFTER = 10

# Позиції полів у масиві активу з повідомлення updateAssets.
IDX_SYMBOL, IDX_TYPE, IDX_PAYOUT, IDX_OTC, IDX_OPEN = 1, 3, 5, 9, 14

SCHEMA = """
CREATE TABLE IF NOT EXISTS payouts (
    ts_utc      TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    payout_pct  DOUBLE PRECISION,
    is_otc      SMALLINT NOT NULL,
    is_open     SMALLINT NOT NULL,
    source      TEXT NOT NULL,
    UNIQUE (ts_utc, symbol, source)
);
CREATE INDEX IF NOT EXISTS payouts_symbol_ts ON payouts (symbol, ts_utc);
CREATE TABLE IF NOT EXISTS raw_records (
    ts_utc TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL,
    raw    JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS collector_log (
    id     BIGSERIAL PRIMARY KEY,
    ts_utc TIMESTAMPTZ NOT NULL,
    event  TEXT NOT NULL,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS daily_reports (
    id         BIGSERIAL PRIMARY KEY,
    day        DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    body       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class Rejected(Exception):
    """Сервер відмовив у з'єднанні або надіслав дані в незнайомому форматі."""


class Fatal(Exception):
    """Сервер відмовляє надто довго: збір треба зупинити й розбиратися вручну."""


def utc(ts):
    return datetime.fromtimestamp(int(ts), timezone.utc)


def database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("Не задано DATABASE_URL, наприклад postgresql://user:pass@host:5432/db")
    return url


class Store:
    """Одне з'єднання з базою на обидва цикли збору; лок не дає їхнім транзакціям перемішатися."""

    def __init__(self, conn):
        self.conn = conn
        self.lock = asyncio.Lock()

    @classmethod
    async def open(cls):
        conn = await psycopg.AsyncConnection.connect(database_url(), autocommit=True)
        await conn.execute("SET TIME ZONE 'UTC'")
        await conn.execute(SCHEMA)
        return cls(conn)

    async def journal(self, event, detail=None):
        now = utc(time.time())
        print(f"[{now:%Y-%m-%dT%H:%M:%SZ}] {event}" + (f": {detail}" if detail else ""), flush=True)
        async with self.lock:
            await self.conn.execute(
                "INSERT INTO collector_log (ts_utc, event, detail) VALUES (%s, %s, %s)",
                (now, event, detail),
            )

    async def first_start(self):
        async with self.lock:
            await self.conn.execute(
                "INSERT INTO meta VALUES ('first_start', %s) ON CONFLICT DO NOTHING",
                (str(time.time()),),
            )
            cur = await self.conn.execute("SELECT value FROM meta WHERE key = 'first_start'")
            return float((await cur.fetchone())[0])

    async def store(self, ts, source, rows, keep_raw):
        """rows: список (symbol, payout або None, is_open, сирий масив або None)."""
        stamp = utc(ts)
        async with self.lock, self.conn.transaction(), self.conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO payouts VALUES (%s, %s, %s, 0, %s, %s) ON CONFLICT DO NOTHING",
                [(stamp, sym, payout, is_open, source) for sym, payout, is_open, _ in rows],
            )
            raw_rows = [(stamp, sym, source, Jsonb(raw)) for sym, _, _, raw in rows if raw is not None]
            if keep_raw and raw_rows:
                await cur.executemany("INSERT INTO raw_records VALUES (%s, %s, %s, %s)", raw_rows)


def tracked(assets):
    """Не-OTC валютні пари з повідомлення сервера: {symbol: сирий масив}."""
    result = {}
    for asset in assets:
        try:
            if asset[IDX_TYPE] == "currency" and not asset[IDX_OTC]:
                result[asset[IDX_SYMBOL]] = asset
        except (IndexError, TypeError):
            raise Rejected(f"незнайомий формат активу: {str(asset)[:200]}")
    return result


async def stream_assets():
    """Одне з'єднання: видає (час, список активів) на кожне повідомлення updateAssets."""
    try:
        ws = await websockets.connect(
            URL, origin=ORIGIN, user_agent_header=USER_AGENT, max_size=None, open_timeout=5
        )
    except websockets.InvalidStatus as e:
        raise Rejected(f"сервер відповів HTTP {e.response.status_code}")
    async with ws:
        pending = None
        async for message in ws:
            if isinstance(message, bytes):
                if pending != "updateAssets":
                    continue
                pending = None
                try:
                    data = json.loads(message)
                except ValueError:
                    raise Rejected("updateAssets: не JSON")
                if not (isinstance(data, list) and data and isinstance(data[0], list)):
                    raise Rejected(f"updateAssets: незнайомий формат: {str(data)[:200]}")
                yield time.time(), data
            elif message.startswith("0"):
                await ws.send("40")
            elif message == "2":
                await ws.send("3")
            elif message.startswith("40"):
                await ws.send(AUTH)
            elif message.startswith("451-"):
                pending = json.loads(message[4:])[0]
            elif message == "41":
                return


async def fetch_snapshot():
    """Нове з'єднання, перший повний список активів від сервера."""
    stream = stream_assets()
    try:
        async for ts, assets in stream:
            return ts, tracked(assets)
        raise ConnectionError("з'єднання закрито до отримання даних")
    finally:
        await stream.aclose()


def snapshot_rows(pairs):
    rows = [
        (sym, a[IDX_PAYOUT], int(bool(a[IDX_OPEN])), a) for sym, a in sorted(pairs.items())
    ]
    rows += [(sym, None, 0, None) for sym in REQUIRED if sym not in pairs]
    return rows


async def snapshot_loop(db, started):
    raw_until = started + RAW_DAYS * 86400
    failing = False
    rejected_in_row = 0
    while True:
        tick = (int(time.time()) // 60 + 1) * 60
        await asyncio.sleep(tick - time.time() + 0.3)
        try:
            ts, pairs = await asyncio.wait_for(fetch_snapshot(), SNAPSHOT_TIMEOUT)
        except Rejected as e:
            failing = True
            rejected_in_row += 1
            await db.journal("snapshot_rejected", f"{utc(tick):%H:%M:%S} {e}")
            if rejected_in_row >= FATAL_AFTER:
                raise Fatal("сервер відмовляє або змінив формат даних")
            continue
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as e:
            failing = True
            await db.journal("snapshot_failed", f"{utc(tick):%H:%M:%S} {e!r}")
            continue
        if failing:
            await db.journal("recovered")
        failing = False
        rejected_in_row = 0
        await db.store(ts, "snapshot", snapshot_rows(pairs), keep_raw=ts < raw_until)


async def change_loop(db, started):
    """Тримає з'єднання і пише рядок source=change, коли сервер надсилає нову виплату."""
    raw_until = started + RAW_DAYS * 86400
    last = {}
    failing = False
    while True:
        connected_at = time.time()
        try:
            async for ts, assets in stream_assets():
                rows = []
                for sym, a in tracked(assets).items():
                    state = (a[IDX_PAYOUT], int(bool(a[IDX_OPEN])))
                    if sym in last and last[sym] != state:
                        rows.append((sym, state[0], state[1], a))
                    last[sym] = state
                if rows:
                    await db.store(ts, "change", rows, keep_raw=ts < raw_until)
                if failing:
                    await db.journal("listener_recovered")
                    failing = False
        except (Rejected, OSError, asyncio.TimeoutError, websockets.WebSocketException) as e:
            if not failing:
                await db.journal("listener_lost", repr(e))
            failing = True
        await asyncio.sleep(max(1, connected_at + LISTENER_MIN_INTERVAL - time.time()))


async def run():
    db = await Store.open()
    started = await db.first_start()
    await db.journal("start", f"сирі записи до {utc(started + RAW_DAYS * 86400):%Y-%m-%dT%H:%M:%SZ}")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    collectors = [
        asyncio.ensure_future(snapshot_loop(db, started)),
        asyncio.ensure_future(change_loop(db, started)),
    ]
    waiter = asyncio.ensure_future(stop.wait())
    done, _ = await asyncio.wait(collectors + [waiter], return_when=asyncio.FIRST_COMPLETED)
    for task in collectors + [waiter]:
        task.cancel()
    await asyncio.gather(*collectors, waiter, return_exceptions=True)
    if stop.is_set():
        await db.journal("stop", "сигнал зупинки")
        return
    error = next(t.exception() for t in done if t is not waiter)
    if isinstance(error, Fatal):
        # Процес лишається живим без збору, щоб платформа не перезапускала його по колу.
        await db.journal("fatal", f"{error}; збір зупинено, потрібне ручне втручання")
        await stop.wait()
        await db.journal("stop", "сигнал зупинки")
        return
    await db.journal("crash", repr(error))
    raise error


async def once():
    ts, pairs = await asyncio.wait_for(fetch_snapshot(), SNAPSHOT_TIMEOUT)
    print(f"Знімок {utc(ts):%Y-%m-%dT%H:%M:%SZ}")
    for sym, payout, is_open, _ in snapshot_rows(pairs):
        mark = "*" if sym in REQUIRED else " "
        value = "немає в списку сервера" if payout is None else f"{payout}%"
        print(f" {mark} {sym:8} {value:>6}" + ("" if is_open else "  (закрита)"))
    print(" * — обов'язкові пари")


def report(date):
    start = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_day = start + timedelta(days=1)
    span = "ts_utc >= %s AND ts_utc < %s"
    out = [f"Звіт збирача за {date} (UTC)", ""]

    with psycopg.connect(database_url(), autocommit=True) as db:
        db.execute("SET TIME ZONE 'UTC'")
        db.execute(SCHEMA)

        def rows(sql, *params):
            return db.execute(sql, (start, end_day) + params).fetchall()

        symbols = [r[0] for r in rows(f"SELECT DISTINCT symbol FROM payouts WHERE {span} ORDER BY 1")]
        if not symbols:
            out.append("ЗА ЦЮ ДОБУ НЕМАЄ ЖОДНОГО ЗАПИСУ.")

        out.append("Хвилинні знімки з виплатою, по годинах (очікується 60):")
        out.append("год  " + " ".join(f"{s:>7}" for s in REQUIRED))
        counts = {
            (sym, hour): n
            for sym, hour, n in rows(
                f"""SELECT symbol, extract(hour FROM ts_utc)::int, count(*) FROM payouts
                    WHERE {span} AND source = 'snapshot' AND payout_pct IS NOT NULL
                    GROUP BY 1, 2"""
            )
        }
        for hour in range(24):
            out.append(f"{hour:02d}   " + " ".join(f"{counts.get((s, hour), 0):>7}" for s in REQUIRED))

        stamps = [
            r[0]
            for r in rows(
                f"SELECT DISTINCT ts_utc FROM payouts WHERE {span} AND source = 'snapshot' ORDER BY 1"
            )
        ]
        end = min(end_day, datetime.now(timezone.utc))
        out += ["", "Пропуски хвилинних знімків довші за 2 хвилини:"]
        gaps = [
            (a, b)
            for a, b in zip([start] + stamps, stamps + [end])
            if (b - a).total_seconds() > 120
        ]
        out += [
            f"  {a:%H:%M:%S} – {b:%H:%M:%S}  ({int((b - a).total_seconds() // 60)} хв)"
            for a, b in gaps
        ] or ["  немає"]

        have = {s.replace(second=0) for s in stamps}
        marks = [start + timedelta(minutes=5 * i) for i in range(288)]
        missed = [m for m in marks if m < end and m not in have]
        out += ["", f"Пропущені 5-хвилинні позначки: {len(missed)}"]
        if missed:
            out.append(
                "  " + " ".join(f"{m:%H:%M}" for m in missed[:60]) + (" …" if len(missed) > 60 else "")
            )

        out += [
            "",
            "Виплата за добу (лише поки пара відкрита):",
            "пара       мін  макс  змін  медіана|Δ|  макс|Δ|  години, коли закрита чи відсутня",
        ]
        fmt = lambda v: "-" if v is None else f"{v:g}"
        for sym in sorted(set(symbols) | set(REQUIRED)):
            lo, hi = rows(
                f"""SELECT min(payout_pct), max(payout_pct) FROM payouts
                    WHERE {span} AND symbol = %s AND is_open = 1""",
                sym,
            )[0]
            values = [
                r[0]
                for r in rows(
                    f"""SELECT payout_pct FROM payouts WHERE {span} AND symbol = %s
                        AND payout_pct IS NOT NULL AND is_open = 1 ORDER BY ts_utc""",
                    sym,
                )
            ]
            deltas = [abs(b - a) for a, b in zip(values, values[1:]) if a != b]
            closed = [
                f"{r[0]:02d}"
                for r in rows(
                    f"""SELECT DISTINCT extract(hour FROM ts_utc)::int FROM payouts
                        WHERE {span} AND symbol = %s AND is_open = 0 ORDER BY 1""",
                    sym,
                )
            ]
            out.append(
                f"{sym:9} {fmt(lo):>4} {fmt(hi):>5} {len(deltas):>5} "
                f"{fmt(statistics.median(deltas)) if deltas else '-':>10} "
                f"{fmt(max(deltas)) if deltas else '-':>8}  {' '.join(closed) or '-'}"
            )

        out += ["", "Журнал збирача:"]
        events = rows(
            f"SELECT ts_utc, event, coalesce(detail, '') FROM collector_log WHERE {span} ORDER BY id"
        )
        out += [f"  {ts:%H:%M:%S} {event} {detail}" for ts, event, detail in events[:200]] or [
            "  подій немає"
        ]
        if len(events) > 200:
            out.append(f"  … ще {len(events) - 200} подій")

        text = "\n".join(out) + "\n"
        db.execute("INSERT INTO daily_reports (day, body) VALUES (%s, %s)", (date, text))
    print(text, end="")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="постійний збір")
    sub.add_parser("once", help="один знімок на екран, без запису")
    rep = sub.add_parser("report", help="добовий звіт")
    rep.add_argument("--date", help="YYYY-MM-DD, типово вчора (UTC)")
    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(run())
    elif args.command == "once":
        asyncio.run(once())
    else:
        date = (
            datetime.strptime(args.date, "%Y-%m-%d").date()
            if args.date
            else (datetime.now(timezone.utc) - timedelta(days=1)).date()
        )
        report(date)


if __name__ == "__main__":
    main()
