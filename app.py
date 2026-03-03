#!/usr/bin/env python3
"""
PROXY API — Render Free Tier
Pool 100% vivant, retesté toutes les 45s
"""

import asyncio
import re
import json
import time
import sqlite3
from datetime import datetime
from typing import Optional, List, Tuple, Set
from contextlib import asynccontextmanager

import aiohttp
import aiosqlite
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import PlainTextResponse
import uvicorn
import os

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════

DB_PATH              = "/tmp/proxies.db"
API_HOST             = "0.0.0.0"
API_PORT             = int(os.environ.get("PORT", 8888))

SCRAPE_INTERVAL_S    = 300   # scrape nouvelles sources toutes les 5min
RETEST_INTERVAL_S    = 45    # retest le pool actif toutes les 45s
MAX_CONCURRENT_TESTS = 50    # max connexions simultanées (RAM safe)
TEST_TIMEOUT_S       = 6     # timeout par proxy

SCRAPE_SOURCES = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=elite",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=anonymous",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=all",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
]

PROXY_PATTERN = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})")


def detect_protocol(url: str) -> str:
    u = url.lower()
    if "socks5" in u: return "socks5"
    if "socks4" in u: return "socks4"
    return "http"


# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS proxies (
            ip               TEXT    NOT NULL,
            port             INTEGER NOT NULL,
            protocol         TEXT    NOT NULL DEFAULT 'http',
            anonymity        TEXT,
            response_time_ms INTEGER,
            last_checked     TEXT,
            last_success     TEXT,
            is_alive         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (ip, port, protocol)
        )
    """)
    # Table pour les candidats pas encore testés
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            ip       TEXT    NOT NULL,
            port     INTEGER NOT NULL,
            protocol TEXT    NOT NULL DEFAULT 'http',
            PRIMARY KEY (ip, port, protocol)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alive    ON proxies(is_alive)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_protocol ON proxies(protocol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ms       ON proxies(response_time_ms)")
    conn.commit()
    conn.close()
    print("[DB] Initialized")


async def db_conn():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def db_add_candidates(proxies: List[Tuple[str, int, str]]):
    """Ajoute les proxies bruts dans la file d'attente."""
    db = await db_conn()
    try:
        await db.executemany(
            "INSERT OR IGNORE INTO candidates (ip, port, protocol) VALUES (?,?,?)",
            proxies,
        )
        await db.commit()
    finally:
        await db.close()


async def db_pop_candidates(limit: int) -> List[Tuple[str, int, str]]:
    """Récupère et supprime un batch de candidats à tester."""
    db = await db_conn()
    try:
        cur = await db.execute(
            "SELECT ip, port, protocol FROM candidates LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        if rows:
            await db.executemany(
                "DELETE FROM candidates WHERE ip=? AND port=? AND protocol=?",
                [(r["ip"], r["port"], r["protocol"]) for r in rows],
            )
            await db.commit()
        return [(r["ip"], r["port"], r["protocol"]) for r in rows]
    finally:
        await db.close()


async def db_candidates_count() -> int:
    db = await db_conn()
    try:
        cur = await db.execute("SELECT COUNT(*) FROM candidates")
        return (await cur.fetchone())[0]
    finally:
        await db.close()


async def db_set_alive(results: list):
    """Upsert des proxies testés — ne garde que les vivants dans proxies."""
    if not results:
        return
    db = await db_conn()
    try:
        for r in results:
            if r["is_alive"]:
                await db.execute("""
                    INSERT INTO proxies
                        (ip, port, protocol, anonymity, response_time_ms, last_checked, last_success, is_alive)
                    VALUES (?,?,?,?,?,?,?,1)
                    ON CONFLICT(ip, port, protocol) DO UPDATE SET
                        anonymity        = excluded.anonymity,
                        response_time_ms = excluded.response_time_ms,
                        last_checked     = excluded.last_checked,
                        last_success     = excluded.last_success,
                        is_alive         = 1
                """, (
                    r["ip"], r["port"], r["protocol"],
                    r.get("anonymity"),
                    r.get("response_time_ms"),
                    r.get("last_checked"),
                    r.get("last_success"),
                ))
            else:
                # Mort → on le vire direct
                await db.execute(
                    "DELETE FROM proxies WHERE ip=? AND port=? AND protocol=?",
                    (r["ip"], r["port"], r["protocol"]),
                )
        await db.commit()
    finally:
        await db.close()


async def db_get_alive_pool() -> List[Tuple[str, int, str]]:
    """Retourne tous les proxies vivants pour le retest."""
    db = await db_conn()
    try:
        cur = await db.execute(
            "SELECT ip, port, protocol FROM proxies WHERE is_alive=1"
        )
        return [(r["ip"], r["port"], r["protocol"]) for r in await cur.fetchall()]
    finally:
        await db.close()


async def db_get_proxies(
    protocol=None, max_ms=None, min_rel=None,
    alive_only=True, limit=50, random_order=False,
):
    db = await db_conn()
    try:
        conds, params = [], []
        if alive_only:
            conds.append("is_alive = 1")
        if protocol:
            conds.append("protocol = ?")
            params.append(protocol)
        if max_ms:
            conds.append("response_time_ms <= ?")
            params.append(max_ms)
        where = " AND ".join(conds) if conds else "1=1"
        order = "RANDOM()" if random_order else "response_time_ms ASC"
        params.append(limit)
        cur = await db.execute(
            f"SELECT * FROM proxies WHERE {where} ORDER BY {order} LIMIT ?",
            params,
        )
        return [dict(r) for r in await cur.fetchall()]
    finally:
        await db.close()


async def db_stats() -> dict:
    db = await db_conn()
    try:
        s = {}
        cur = await db.execute("SELECT COUNT(*) FROM proxies WHERE is_alive=1")
        s["alive"] = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM candidates")
        s["candidates_pending"] = (await cur.fetchone())[0]
        cur = await db.execute("""
            SELECT protocol, COUNT(*) as count
            FROM proxies WHERE is_alive=1 GROUP BY protocol
        """)
        s["by_protocol"] = [dict(r) for r in await cur.fetchall()]
        cur = await db.execute("""
            SELECT AVG(response_time_ms) as avg_ms,
                   MIN(response_time_ms) as min_ms,
                   MAX(response_time_ms) as max_ms
            FROM proxies WHERE is_alive=1 AND response_time_ms IS NOT NULL
        """)
        row = await cur.fetchone()
        s["response_times"] = {
            "avg_ms": round(row[0]) if row[0] else None,
            "min_ms": row[1],
            "max_ms": row[2],
        }
        return s
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# SCRAPER
# ══════════════════════════════════════════════════════════════

async def fetch_source(
    session: aiohttp.ClientSession, url: str
) -> List[Tuple[str, int, str]]:
    protocol = detect_protocol(url)
    found = []
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                text = await resp.text()
                for ip, port in PROXY_PATTERN.findall(text):
                    p = int(port)
                    if 1 <= p <= 65535:
                        found.append((ip, p, protocol))
                print(f"  [SCRAPE] {len(found):>5} <- {url[:70]}")
    except Exception as e:
        print(f"  [SCRAPE] FAIL {url[:50]} | {e}")
    return found


async def scrape_all() -> int:
    print("[SCRAPE] Start...")
    seen: Set[Tuple[str, int, str]] = set()
    result = []
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [fetch_source(session, url) for url in SCRAPE_SOURCES]
        for batch in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(batch, list):
                for p in batch:
                    if p not in seen:
                        seen.add(p)
                        result.append(p)
    await db_add_candidates(result)
    print(f"[SCRAPE] Done — {len(result)} candidats ajoutés")
    return len(result)


# ══════════════════════════════════════════════════════════════
# TESTER
# ══════════════════════════════════════════════════════════════

async def get_my_ip(session: aiohttp.ClientSession) -> Optional[str]:
    try:
        async with session.get(
            "https://api.ipify.org?format=json",
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            return (await resp.json()).get("ip")
    except:
        return None


async def test_one(
    session: aiohttp.ClientSession,
    ip: str, port: int, protocol: str,
    my_ip: Optional[str],
    sem: asyncio.Semaphore,
) -> dict:
    result = {
        "ip": ip, "port": port, "protocol": protocol,
        "last_checked": datetime.utcnow().isoformat(),
        "is_alive": False,
        "anonymity": None,
        "response_time_ms": None,
        "last_success": None,
    }
    async with sem:
        start = time.monotonic()
        try:
            async with session.get(
                "http://httpbin.org/ip",
                proxy=f"{protocol}://{ip}:{port}",
                timeout=aiohttp.ClientTimeout(total=TEST_TIMEOUT_S),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    elapsed = int((time.monotonic() - start) * 1000)
                    body = await resp.text()
                    result["is_alive"] = True
                    result["response_time_ms"] = elapsed
                    result["last_success"] = datetime.utcnow().isoformat()
                    try:
                        origin = json.loads(body).get("origin", "")
                        if my_ip and my_ip in origin:
                            result["anonymity"] = "transparent"
                        elif "," in origin:
                            result["anonymity"] = "anonymous"
                        else:
                            result["anonymity"] = "elite"
                    except:
                        result["anonymity"] = "unknown"
        except:
            pass
    return result


async def run_tests(
    proxies: List[Tuple[str, int, str]],
    label: str = "TEST",
) -> Tuple[int, int]:
    if not proxies:
        return 0, 0

    print(f"[{label}] Testing {len(proxies)} proxies...")
    sem = asyncio.Semaphore(MAX_CONCURRENT_TESTS)
    alive = dead = done = 0
    total = len(proxies)

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_TESTS,
        force_close=True,
        ssl=False,
        ttl_dns_cache=300,
    )
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as session:
        my_ip = await get_my_ip(session)
        tasks = [
            test_one(session, ip, port, proto, my_ip, sem)
            for ip, port, proto in proxies
        ]
        buffer = []
        for coro in asyncio.as_completed(tasks):
            r = await coro
            buffer.append(r)
            done += 1
            if r["is_alive"]:
                alive += 1
            else:
                dead += 1
            if len(buffer) >= 100:
                await db_set_alive(buffer)
                buffer.clear()
        if buffer:
            await db_set_alive(buffer)

    print(f"[{label}] Done — alive={alive} dead={dead}")
    return alive, dead


# ══════════════════════════════════════════════════════════════
# BACKGROUND LOOPS
# ══════════════════════════════════════════════════════════════

_scrape_lock = asyncio.Lock()
_retest_lock = asyncio.Lock()


async def loop_scrape():
    """Scrape les sources et teste les nouveaux candidats en continu."""
    await asyncio.sleep(2)  # laisser le temps au serveur de démarrer
    while True:
        async with _scrape_lock:
            try:
                await scrape_all()
                # Tester tous les candidats par batch
                while True:
                    batch = await db_pop_candidates(200)
                    if not batch:
                        break
                    await run_tests(batch, label="NEW")
            except Exception as e:
                print(f"[SCRAPE LOOP] Error: {e}")
        await asyncio.sleep(SCRAPE_INTERVAL_S)


async def loop_retest():
    """Reteste le pool actif toutes les 45s — vire les morts immédiatement."""
    await asyncio.sleep(10)  # laisser le premier scrape démarrer
    while True:
        async with _retest_lock:
            try:
                pool = await db_get_alive_pool()
                if pool:
                    await run_tests(pool, label="RETEST")
                else:
                    print("[RETEST] Pool vide, on attend...")
            except Exception as e:
                print(f"[RETEST LOOP] Error: {e}")
        await asyncio.sleep(RETEST_INTERVAL_S)


# ══════════════════════════════════════════════════════════════
# FASTAPI
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("\n=== PROXY API — Starting ===")
    init_db()
    asyncio.create_task(loop_scrape())
    asyncio.create_task(loop_retest())
    print("[APP] Background loops started\n")
    yield
    print("[APP] Shutting down")


app = FastAPI(title="Proxy API", version="3.0.0", lifespan=lifespan)


@app.get("/")
async def root():
    s = await db_stats()
    return {
        "alive_proxies":    s.get("alive", 0),
        "pending_candidates": s.get("candidates_pending", 0),
        "by_protocol":      s.get("by_protocol", []),
        "endpoints": {
            "GET  /proxies":        "Liste JSON — params: protocol, max_response_time, limit, random",
            "GET  /proxies/raw":    "Texte brut ip:port — params: protocol, limit, format",
            "GET  /proxies/random": "Un proxy aléatoire vivant",
            "GET  /stats":          "Stats complètes",
            "GET  /health":         "Health check Render",
        },
    }


@app.get("/proxies")
async def list_proxies(
    protocol:          Optional[str] = Query(None, description="http | socks4 | socks5"),
    max_response_time: Optional[int] = Query(None, description="ex: 2000 (ms)"),
    alive_only:        bool          = Query(True),
    limit:             int           = Query(50, le=1000),
    random_order:      bool          = Query(False),
):
    proxies = await db_get_proxies(
        protocol=protocol,
        max_ms=max_response_time,
        alive_only=alive_only,
        limit=limit,
        random_order=random_order,
    )
    return {"count": len(proxies), "proxies": proxies}


@app.get("/proxies/raw", response_class=PlainTextResponse)
async def raw_proxies(
    protocol: Optional[str] = Query(None),
    limit:    int           = Query(500, le=5000),
    format:   str           = Query("ip:port", description="ip:port | url"),
):
    proxies = await db_get_proxies(protocol=protocol, limit=limit, alive_only=True)
    if format == "url":
        lines = [f"{p['protocol']}://{p['ip']}:{p['port']}" for p in proxies]
    else:
        lines = [f"{p['ip']}:{p['port']}" for p in proxies]
    return "\n".join(lines)


@app.get("/proxies/random")
async def random_proxy(
    protocol:          Optional[str] = Query(None),
    max_response_time: Optional[int] = Query(None),
):
    proxies = await db_get_proxies(
        protocol=protocol,
        max_ms=max_response_time,
        alive_only=True,
        limit=100,
        random_order=True,
    )
    if not proxies:
        raise HTTPException(404, "Aucun proxy vivant disponible")
    p = proxies[0]
    return {
        "proxy":            f"{p['protocol']}://{p['ip']}:{p['port']}",
        "ip":               p["ip"],
        "port":             p["port"],
        "protocol":         p["protocol"],
        "anonymity":        p.get("anonymity"),
        "response_time_ms": p.get("response_time_ms"),
    }


@app.get("/stats")
async def stats():
    return await db_stats()


@app.get("/health")
async def health():
    """Render Health Check — répondre vite."""
    s = await db_stats()
    return {"status": "ok", "alive": s.get("alive", 0)}


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="info")
