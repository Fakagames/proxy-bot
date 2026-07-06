"""
Proxy Bot — Autonomous cloud-ready proxy checker.
Reads raw URLs from sources.txt, fetches + tests against Discord, loops forever.
Just add URLs to sources.txt and the bot picks them up next cycle.

Usage:
    python bot.py                  # run forever (cycle every 30 min)
    python bot.py --once           # single run then exit
    python bot.py --interval 60    # cycle every 60 min
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import httpx

# ── Config ──────────────────────────────────────────────────────────────────

CONCURRENCY = 150
TIMEOUT = 8.0
SAVE_EVERY = 30
SOURCES_FILE = "sources.txt"
OUTPUT_DIR = Path("output")
DEFAULT_INTERVAL = 30  # minutes

SOCKS_OK = False
try:
    import socksio  # noqa: F401
    SOCKS_OK = True
except ImportError:
    pass

ALLOWED = {"http", "https"} | ({"socks4", "socks5"} if SOCKS_OK else set())

# ── Proxy ───────────────────────────────────────────────────────────────────

@dataclass
class Proxy:
    url: str
    protocol: str
    host: str
    port: int
    latency_ms: float = 0.0
    alive: bool = False
    error: str = ""

_PROXY_RE = re.compile(
    r"^(?:(?P<proto>https?|socks[45]h?)://)?"
    r"(?:(?P<user>[^:@/]+):(?P<pw>[^@/]+)@)?"
    r"(?P<host>\[[0-9a-fA-F:]+\]|[^:@/\s]+)"
    r":(?P<port>\d{1,5})"
    r"(?:/.*)?$"
)
_PROTO_NORM = {"socks5h": "socks5", "socks4a": "socks4"}

def parse_line(raw: str, default_proto: str = "http") -> Proxy | None:
    line = raw.strip()
    if not line or line.startswith("#"):
        return None
    if "  #" in line:
        line = line[:line.index("  #")].strip()
    m = _PROXY_RE.match(line)
    if not m:
        return None
    proto = _PROTO_NORM.get(
        (m.group("proto") or default_proto).lower(),
        (m.group("proto") or default_proto).lower(),
    )
    host = m.group("host").strip("[]")
    try:
        port = int(m.group("port"))
    except ValueError:
        return None
    if not (1 <= port <= 65535) or proto not in ALLOWED:
        return None
    user, pw = m.group("user"), m.group("pw")
    auth = f"{user}:{pw}@" if user and pw else ""
    url_host = f"[{host}]" if ":" in host else host
    return Proxy(url=f"{proto}://{auth}{url_host}:{port}", protocol=proto, host=host, port=port)

def infer_proto(url: str) -> str:
    low = url.lower()
    for kw in ("socks5", "socks4", "https"):
        if kw in low:
            return kw
    return "http"

# ── Sources ─────────────────────────────────────────────────────────────────

def load_sources(path: str = SOURCES_FILE) -> tuple[list[str], bool]:
    """Returns (urls, has_ruproxy)."""
    p = Path(path)
    if not p.is_file():
        return [], False
    urls = []
    has_ruproxy = False
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            if line == "#RUPROXY":
                has_ruproxy = True
            continue
        if line.startswith("http"):
            urls.append(line)
    return urls, has_ruproxy

# ── Fetch ───────────────────────────────────────────────────────────────────

async def fetch_url(client: httpx.AsyncClient, url: str) -> str:
    for attempt in range(2):
        try:
            resp = await client.get(url, timeout=15.0)
            resp.raise_for_status()
            return resp.text
        except Exception:
            if attempt == 0:
                await asyncio.sleep(1.0)
    return ""

async def fetch_ruproxy(client: httpx.AsyncClient) -> list[Proxy]:
    proxies = []
    for params in [
        {"residential": "true", "maxSpeed": 1000},
        {"residential": "false", "maxSpeed": 500, "maxFraudScore": 50},
    ]:
        try:
            resp = await client.get("https://ruproxy.to/api/v2/proxies", params=params, timeout=15.0)
            if resp.status_code != 200:
                continue
            for item in resp.json():
                if item.get("status") != 1:
                    continue
                addr = item.get("address", "")
                port = item.get("port", 0)
                if not addr or not port:
                    continue
                p = Proxy(url=f"http://{addr}:{port}", protocol="http", host=addr, port=int(port))
                ping = item.get("ping", 0)
                if ping:
                    p.latency_ms = float(ping)
                proxies.append(p)
        except Exception:
            pass
    return proxies

async def gather_all(sources_file: str = SOURCES_FILE) -> list[Proxy]:
    seen: set[str] = set()
    proxies: list[Proxy] = []

    def add(p: Proxy) -> None:
        key = f"{p.host}:{p.port}"
        if key not in seen:
            seen.add(key)
            proxies.append(p)

    urls, has_ruproxy = load_sources(sources_file)
    if not urls and not has_ruproxy:
        log("No sources found in sources.txt!")
        return []

    log(f"Fetching {len(urls)} sources...")

    async with httpx.AsyncClient(follow_redirects=True, verify=False, timeout=20.0) as client:
        results = await asyncio.gather(
            *[fetch_url(client, url) for url in urls],
            return_exceptions=True,
        )

        fetched = 0
        for url, result in zip(urls, results):
            if isinstance(result, BaseException) or not result:
                continue
            fetched += 1

            # JSON source
            if url.endswith(".json"):
                try:
                    data = json.loads(result)
                    items = data if isinstance(data, list) else data.get("proxies", data.get("data", []))
                    if not isinstance(items, list):
                        items = []
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        try:
                            host = str(
                                item.get("ip") or item.get("host")
                                or item.get("address") or item.get("proxy", "").split(":")[0]
                            ).strip()
                            port_raw = (
                                item.get("port")
                                or (item.get("proxy", ":").split(":")[1] if ":" in item.get("proxy", "") else 0)
                            )
                            port = int(port_raw)
                            raw_proto = str(
                                item.get("protocol") or item.get("type")
                                or item.get("proto") or "http"
                            ).lower().replace("https", "http")
                            proto = _PROTO_NORM.get(raw_proto, raw_proto)
                            if proto not in ALLOWED or not host or not (1 <= port <= 65535):
                                continue
                            url_host = f"[{host}]" if ":" in host else host
                            p = Proxy(url=f"{proto}://{url_host}:{port}", protocol=proto, host=host, port=port)
                            lat = item.get("latency") or item.get("response_time")
                            if lat is not None:
                                p.latency_ms = float(lat)
                            add(p)
                        except Exception:
                            continue
                except Exception:
                    pass
                continue

            proto = infer_proto(url)
            for raw_line in result.splitlines():
                p = parse_line(raw_line.strip(), default_proto=proto)
                if p:
                    add(p)

        # ruproxy
        if has_ruproxy:
            ru_proxies = await fetch_ruproxy(client)
            for p in ru_proxies:
                add(p)
            if ru_proxies:
                log(f"  ruproxy: +{len(ru_proxies):,}")

    log(f"Fetched {fetched}/{len(urls)} sources | {len(proxies):,} unique proxies")
    return proxies

# ── Test ────────────────────────────────────────────────────────────────────

async def test_proxy(proxy: Proxy) -> None:
    kw: dict = {
        "proxy": proxy.url,
        "timeout": TIMEOUT,
        "verify": False,
        "follow_redirects": True,
    }
    if proxy.protocol in ("socks4", "socks5"):
        kw["http2"] = False

    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(**kw) as client:
            resp = await client.head("https://discord.com/", timeout=TIMEOUT)
            if resp.status_code >= 500:
                proxy.error = f"http_{resp.status_code}"
                return
            if resp.status_code == 429:
                proxy.alive = True
                proxy.latency_ms = (time.monotonic() - t0) * 1000
                return

            resp2 = await client.get(
                "https://discord.com/api/v9/experiments",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Content-Type": "application/json",
                },
                timeout=TIMEOUT,
            )
            latency = (time.monotonic() - t0) * 1000

            if resp2.status_code == 429:
                proxy.alive = True
                proxy.latency_ms = latency
                return
            if resp2.status_code < 500:
                proxy.alive = True
                proxy.latency_ms = latency
                return
            proxy.error = f"api_{resp2.status_code}"

    except httpx.TimeoutException:
        proxy.error = "timeout"
    except httpx.ConnectError:
        proxy.error = "connect"
    except httpx.ProxyError:
        proxy.error = "proxy_error"
    except Exception as e:
        proxy.error = type(e).__name__

# ── Save ────────────────────────────────────────────────────────────────────

def save_results(proxies: list[Proxy]) -> int:
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    alive = sorted([p for p in proxies if p.alive], key=lambda p: p.latency_ms)

    # Main working file (always overwritten with latest)
    working = OUTPUT_DIR / "working_proxies.txt"
    with open(working, "w", encoding="utf-8") as f:
        f.write(f"# {len(alive)} working proxies — sorted by latency\n")
        f.write(f"# Generated: {ts}\n\n")
        for p in alive:
            f.write(f"{p.url}  # {p.latency_ms:.0f}ms\n")

    # Timestamped snapshot
    snapshot = OUTPUT_DIR / f"working_{ts}.txt"
    with open(snapshot, "w", encoding="utf-8") as f:
        for p in alive:
            f.write(f"{p.url}  # {p.latency_ms:.0f}ms\n")

    # By protocol
    by_proto: dict[str, list[Proxy]] = {}
    for p in alive:
        by_proto.setdefault(p.protocol, []).append(p)
    for proto, plist in by_proto.items():
        with open(OUTPUT_DIR / f"working_{proto}.txt", "w", encoding="utf-8") as f:
            for p in plist:
                f.write(f"{p.url}  # {p.latency_ms:.0f}ms\n")

    # Copy to discord checker if exists
    checker_dir = Path.home() / "OneDrive" / "Desktop" / "discord_checker_improved_v2"
    if checker_dir.is_dir():
        import shutil
        shutil.copy2(str(working), str(checker_dir / "working_proxies.txt"))

    # Cleanup old snapshots (keep last 10)
    snapshots = sorted(OUTPUT_DIR.glob("working_20*.txt"), reverse=True)
    for old in snapshots[10:]:
        old.unlink(missing_ok=True)

    return len(alive)

# ── Display ─────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def print_progress(tested: int, total: int, alive: int, dead: int, started: float) -> None:
    elapsed = time.monotonic() - started
    rate = tested / elapsed if elapsed > 0 else 0
    pct = (tested / total * 100) if total > 0 else 0
    remaining = (total - tested) / rate if rate > 0 else 0

    bar_w = 30
    filled = int(bar_w * pct / 100)
    bar = "#" * filled + "-" * (bar_w - filled)

    line = (
        f"\r  [{bar}] {pct:5.1f}%  "
        f"{alive} alive / {dead} dead  "
        f"({tested:,}/{total:,})  "
        f"{rate:.0f}/s  "
        f"ETA {int(remaining)}s   "
    )
    sys.stdout.write(line)
    sys.stdout.flush()

# ── Main cycle ──────────────────────────────────────────────────────────────

async def run_cycle() -> int:
    log("=" * 50)
    log("PROXY BOT — Starting cycle")
    log("=" * 50)

    proxies = await gather_all()
    if not proxies:
        log("No proxies to test.")
        return 0

    log(f"Testing {len(proxies):,} proxies ({CONCURRENCY} concurrent)...")

    total = len(proxies)
    tested = 0
    alive_count = 0
    dead_count = 0
    started = time.monotonic()
    last_save = started
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _test(p: Proxy) -> None:
        nonlocal tested, alive_count, dead_count, last_save
        async with sem:
            try:
                await asyncio.wait_for(test_proxy(p), timeout=TIMEOUT + 5)
            except (asyncio.TimeoutError, Exception):
                p.error = "timeout_outer"

            tested += 1
            if p.alive:
                alive_count += 1
            else:
                dead_count += 1

            if tested % 100 == 0 or tested == total:
                print_progress(tested, total, alive_count, dead_count, started)

            if time.monotonic() - last_save > SAVE_EVERY and alive_count > 0:
                last_save = time.monotonic()
                save_results(proxies)

    await asyncio.gather(*[_test(p) for p in proxies], return_exceptions=True)

    print()
    n = save_results(proxies)
    elapsed = time.monotonic() - started

    alive_list = sorted([p for p in proxies if p.alive], key=lambda p: p.latency_ms)

    log(f"DONE in {elapsed:.0f}s")
    log(f"Tested: {total:,} | Alive: {alive_count:,} | Dead: {dead_count:,}")

    if alive_list:
        avg_lat = sum(p.latency_ms for p in alive_list) / len(alive_list)
        fast = sum(1 for p in alive_list if p.latency_ms < 2000)
        log(f"Avg latency: {avg_lat:.0f}ms | Fast (<2s): {fast}")

        proto_counts: dict[str, int] = {}
        for p in alive_list:
            proto_counts[p.protocol] = proto_counts.get(p.protocol, 0) + 1
        parts = [f"{proto}: {c}" for proto, c in sorted(proto_counts.items())]
        log(f"By type: {' | '.join(parts)}")

        log(f"Top 5 fastest:")
        for i, p in enumerate(alive_list[:5], 1):
            log(f"  {i}. {p.url:<45s} {p.latency_ms:>6.0f}ms")

    log(f"Saved to: output/working_proxies.txt ({n} proxies)")
    log("=" * 50)
    return n

async def main() -> None:
    once = "--once" in sys.argv
    interval = DEFAULT_INTERVAL
    if "--interval" in sys.argv:
        idx = sys.argv.index("--interval")
        if idx + 1 < len(sys.argv):
            interval = int(sys.argv[idx + 1])

    cycle = 0
    while True:
        cycle += 1
        log(f"--- Cycle #{cycle} ---")
        try:
            n = await run_cycle()
            log(f"Cycle #{cycle} complete: {n} working proxies")
        except KeyboardInterrupt:
            log("Interrupted by user.")
            break
        except Exception as e:
            log(f"Cycle error: {e}")

        if once:
            break

        log(f"Next cycle in {interval} minutes. Press Ctrl+C to stop.")
        try:
            await asyncio.sleep(interval * 60)
        except KeyboardInterrupt:
            log("Stopped.")
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBye.")
