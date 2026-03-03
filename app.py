#!/usr/bin/env python3
"""
PROXY API — Render Free Tier
Pool 100% propre :
- Proxy doit répondre 200
- Proxy doit cacher notre vraie IP (pas transparent)
- Retesté toutes les 45s, mort = viré immédiatement
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

SCRAPE_INTERVAL_S    = 300
RETEST_INTERVAL_S    = 45
MAX_CONCURRENT_TESTS = 50
TEST_TIMEOUT_S       = 6

# Notre vraie IP — récupérée au démarrage
MY_IP: Optional[str] = None

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


async def db_set_results(results: list):
    """
    Vivant + pas transparent = on garde.
    Tout le reste = on vire.
    """
    if not results:
        return
    db = await db_conn()
    try:
        for r in results:
            if r["is_alive"] and r["anonymity"] in ("anonymous", "elite"):
                await db.execute("""
                    INSERT INTO proxies
                        (ip, port, protocol, anonymity, response_time_ms,
                         last_checked, last_success, is_alive)
                    VALUES (?,?,?,?,?,?,?,1)
                    ON CONFLICT(ip, port, protocol) DO UPDATE SET
                        anonymity        = excluded.anonymity,
                        response_time_ms = excluded.response_time_ms,
                        last_checked     = excluded.last_checked,
                        last_success     = excluded.last_success,
                        is_alive         = 1
                """, (
                    r["ip"], r["port"], r["protocol"],
                    r["anonymity"],
                    r["response_time_ms"],
                    r["last_checked"],
                    r["last_success"],
                ))
            else:
                # Transparent, mort, unknown → poubelle
                await db.execute(
                    "DELETE FROM proxies WHERE ip=? AND port=? AND protocol=?",
                    (r["ip"], r["port"], r["protocol"]),
                )
        await db.commit()
    finally:
        await db.close()


async def db_get_alive_pool() -> List[Tuple[str, int, str]]:
    db = await db_conn()
    try:
        cur = await db.execute(
            "SELECT ip, port, protocol FROM proxies WHERE is_alive=1"
        )
        return [(r["ip"], r["port"], r["protocol"]) for r in await cur.fetchall()]
    finally:
        await db.close()


async def db_get_proxies(
    protocol=None, max_ms=None,
    anonymity=None, limit=50, random_order=False,
):
    db = await db_conn()
    try:
        conds = ["is_alive = 1"]
        params = []
        if protocol:
            conds.append("protocol = ?")
            params.append(protocol)
        if max_ms:
            conds.append("response_time_ms <= ?")
            params.append(max_ms)
        if anonymity:
            conds.append("anonymity = ?")
            params.append(anonymity)
        where = " AND ".join(conds)
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
        cur = await db.execute(
            "SELECT COUNT(*) FROM proxies WHERE is_alive=1 AND anonymity='elite'"
        )
        s["elite"] = (await cur.fetchone())[0]
        cur = await db.execute(
            "SELECT COUNT(*) FROM proxies WHERE is_alive=1 AND anonymity='anonymous'"
        )
        s["anonymous"] = (await cur.fetchone())[0]
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

async def fetch_my_ip() -> Optional[str]:
    """Récupère notre vraie IP au démarrage."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.ipify.org?format=json",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                ip = data.get("ip")
                print(f"[INIT] Notre IP publique: {ip}")
                return ip
    except Exception as e:
        print(f"[INIT] Impossible de récupérer notre IP: {e}")
        return None


async def test_one(
    session: aiohttp.ClientSession,
    ip: str, port: int, protocol: str,
    sem: asyncio.Semaphore,
) -> dict:
    """
    Teste un proxy.
    Règles strictes :
      - Doit répondre 200
      - Doit masquer notre vraie IP (pas transparent, pas unknown)
      - Doit retourner un JSON valide avec 'origin'
    """
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
                if resp.status != 200:
                    return result  # mort

                elapsed = int((time.monotonic() - start) * 1000)
                body = await resp.text()

                # Doit être un JSON valide
                try:
                    data = json.loads(body)
                except Exception:
                    return result  # réponse invalide → poubelle

                origin = data.get("origin", "")

                # Doit avoir un champ origin
                if not origin:
                    return result

                # Notre vraie IP ne doit PAS apparaître
                if MY_IP and MY_IP in origin:
                    result["anonymity"] = "transparent"
                    result["is_alive"] = False  # transparent = poubelle
                    return result

                # Plusieurs IPs dans origin = anonymous
                # Une seule IP différente de la nôtre = elite
                if "," in origin:
                    anonymity = "anonymous"
                else:
                    anonymity = "elite"

                result["is_alive"] = True
                result["response_time_ms"] = elapsed
                result["last_success"] = datetime.utcnow().isoformat()
                result["anonymity"] = anonymity

        except Exception:
            pass  # timeout, refused, etc. → mort

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
        tasks = [
            test_one(session, ip, port, proto, sem)
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
            if done % 100 == 0 or done == total:
                print(f"  [{int(done/total*100):3d}%] {done}/{total} | alive={alive} dead={dead}")
            if len(buffer) >= 100:
                await db_set_results(buffer)
                buffer.clear()
        if buffer:
            await db_set_results(buffer)

    print(f"[{label}] Done — alive={alive} dead={dead}")
    return alive, dead


# ══════════════════════════════════════════════════════════════
# BACKGROUND LOOPS
# ══════════════════════════════════════════════════════════════

_scrape_lock = asyncio.Lock()
_retest_lock = asyncio.Lock()


async def loop_scrape():
    """Scrape + teste les nouveaux candidats en boucle."""
    await asyncio.sleep(2)
    while True:
        if not _scrape_lock.locked():
            async with _scrape_lock:
                try:
                    await scrape_all()
                    while True:
                        batch = await db_pop_candidates(200)
                        if not batch:
                            break
                        await run_tests(batch, label="NEW")
                except Exception as e:
                    print(f"[SCRAPE LOOP] Error: {e}")
        await asyncio.sleep(SCRAPE_INTERVAL_S)


async def loop_retest():
    """Retest le pool actif toutes les 45s."""
    await asyncio.sleep(15)
    while True:
        if not _retest_lock.locked():
            async with _retest_lock:
                try:
                    pool = await db_get_alive_pool()
                    if pool:
                        await run_tests(pool, label="RETEST")
                    else:
                        print("[RETEST] Pool vide...")
                except Exception as e:
                    print(f"[RETEST LOOP] Error: {e}")
        await asyncio.sleep(RETEST_INTERVAL_S)


# ══════════════════════════════════════════════════════════════
# FASTAPI
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    global MY_IP
    print("\n=== PROXY API v4 — Starting ===")
    init_db()
    MY_IP = await fetch_my_ip()
    asyncio.create_task(loop_scrape())
    asyncio.create_task(loop_retest())
    print("[APP] Ready\n")
    yield
    print("[APP] Shutting down")


app = FastAPI(title="Proxy API", version="4.0.0", lifespan=lifespan)


@app.get("/")
async def root():
    s = await db_stats()
    return {
        "alive":             s.get("alive", 0),
        "elite":             s.get("elite", 0),
        "anonymous":         s.get("anonymous", 0),
        "candidates_pending":s.get("candidates_pending", 0),
        "by_protocol":       s.get("by_protocol", []),
        "note":              "Pool 100% propre — transparent et unknown exclus",
    }


@app.get("/proxies")
async def list_proxies(
    protocol:          Optional[str] = Query(None, description="http | socks4 | socks5"),
    max_response_time: Optional[int] = Query(None, description="ex: 2000 (ms)"),
    anonymity:         Optional[str] = Query(None, description="elite | anonymous"),
    limit:             int           = Query(50, le=1000),
    random_order:      bool          = Query(False),
):
    proxies = await db_get_proxies(
        protocol=protocol,
        max_ms=max_response_time,
        anonymity=anonymity,
        limit=limit,
        random_order=random_order,
    )
    return {"count": len(proxies), "proxies": proxies}


@app.get("/proxies/raw", response_class=PlainTextResponse)
async def raw_proxies(
    protocol:  Optional[str] = Query(None),
    anonymity: Optional[str] = Query(None, description="elite | anonymous"),
    limit:     int           = Query(500, le=5000),
    format:    str           = Query("ip:port", description="ip:port | url"),
):
    proxies = await db_get_proxies(
        protocol=protocol, anonymity=anonymity, limit=limit
    )
    if format == "url":
        lines = [f"{p['protocol']}://{p['ip']}:{p['port']}" for p in proxies]
    else:
        lines = [f"{p['ip']}:{p['port']}" for p in proxies]
    return "\n".join(lines)


@app.get("/proxies/random")
async def random_proxy(
    protocol:          Optional[str] = Query(None),
    anonymity:         Optional[str] = Query(None, description="elite | anonymous"),
    max_response_time: Optional[int] = Query(None),
):
    proxies = await db_get_proxies(
        protocol=protocol,
        anonymity=anonymity,
        max_ms=max_response_time,
        limit=100,
        random_order=True,
    )
    if not proxies:
        raise HTTPException(404, "Aucun proxy disponible")
    p = proxies[0]
    return {
        "proxy":            f"{p['protocol']}://{p['ip']}:{p['port']}",
        "ip":               p["ip"],
        "port":             p["port"],
        "protocol":         p["protocol"],
        "anonymity":        p["anonymity"],
        "response_time_ms": p["response_time_ms"],
    }


@app.get("/stats")
async def stats():
    return await db_stats()


@app.get("/health")
async def health():
    s = await db_stats()
    return {"status": "ok", "alive": s.get("alive", 0)}


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="info")
