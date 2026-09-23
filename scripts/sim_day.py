#!/usr/bin/env python3
"""sim_day.py — paced "N links/day" live simulation against a running shrt.

Pure HTTP + stdlib — works against every shrt implementation (the API is
identical). The point is *sustained pacing*, not max throughput: the server
is kept at a realistic rate for a real duration so cache behaviour, hit
flushes, sweeps and corpus growth all behave like a day of traffic.

Examples
  # 100M/day pacing (~1157 links/s) for 10 min against :8080
  python3 scripts/sim_day.py --rate 1157 --duration 600

  # compress a whole day: write 100M via bulk x1000 as fast as it goes
  python3 scripts/sim_day.py --count 100000000 --bulk 1000 --rate 50000

  # add redirect traffic: 5000 GET/s on previously written codes
  python3 scripts/sim_day.py --rate 1157 --read-rate 5000 --duration 600

  # many distinct client IPs (needs server TRUST_PROXY=1, or it rate-limits)
  python3 scripts/sim_day.py --rate 1157 --xff --duration 600

Exit: 0 on clean run (errors <1%, verify sample resolves), 1 otherwise.
"""

import argparse
import http.client
import json
import queue
import random
import string
import subprocess
import sys
import threading
import time
import urllib.parse

CODES_CAP = 250_000  # ring of recently written codes for read traffic


def host_port(url):
    p = urllib.parse.urlparse(url)
    return p.hostname, p.port or 80


def fmt_lat(us):
    return f"{us / 1000:.1f}ms" if us >= 1000 else f"{us}us"


def rss_kb(pid):
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                             capture_output=True, text=True)
        return int(out.stdout.strip() or 0)
    except Exception:
        return 0


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ok = 0
        self.err = 0
        self.lat = []  # us, all samples

    def add(self, ok, us):
        with self.lock:
            if ok:
                self.ok += 1
            else:
                self.err += 1
            self.lat.append(us)

    def snapshot(self, since):
        with self.lock:
            return self.ok, self.err, self.lat[since:], len(self.lat)


def pct(sorted_lat, q):
    if not sorted_lat:
        return 0
    return sorted_lat[min(len(sorted_lat) - 1, int(len(sorted_lat) * q))]


def url_for(i):
    """Deterministic unique URLs — realistic ~40-80B targets."""
    return (f"https://sim{i % 977}.example.com/"
            f"{''.join(random.choices(string.ascii_lowercase, k=8))}/{i}")


class SimWorker(threading.Thread):
    """One keep-alive connection executing (kind, payload) work items."""

    def __init__(self, args, q, stats, codes, codes_lock, want_codes):
        super().__init__(daemon=True)
        self.args, self.q, self.stats = args, q, stats
        self.codes, self.codes_lock = codes, codes_lock
        self.want_codes = want_codes
        self.host, self.port = host_port(args.url)
        self.conn = None

    def _conn(self):
        if self.conn is None:
            self.conn = http.client.HTTPConnection(self.host, self.port,
                                                   timeout=30)
        return self.conn

    def _do(self, method, path, body, headers):
        last = None
        for _ in range(2):  # one reconnect retry
            try:
                c = self._conn()
                c.request(method, path, body=body, headers=headers)
                r = c.getresponse()
                return r.status, r.read()
            except Exception as e:
                last = e
                self.conn = None
        raise last

    def run(self):
        while True:
            item = self.q.get()
            if item is None:
                self.q.task_done()
                return
            kind, payload = item
            headers = {}
            if self.args.xff:
                headers["x-forwarded-for"] = ".".join(
                    str(random.randint(1, 254)) for _ in range(4))
            t0 = time.perf_counter_ns()
            try:
                if kind == "read":
                    st, _ = self._do("GET", f"/{payload}", None, headers)
                    self.stats.add(st in (301, 302, 307, 308),
                                   (time.perf_counter_ns() - t0) // 1000)
                else:
                    headers["content-type"] = "application/json"
                    if kind == "write1":
                        st, b = self._do("POST", "/api/shorten",
                                         json.dumps({"url": payload}), headers)
                    else:
                        st, b = self._do("POST", "/api/shorten/bulk",
                                         json.dumps({"urls": payload}), headers)
                    ok = st in (200, 201)
                    if ok and self.want_codes:
                        try:
                            got = ([json.loads(b)["code"]] if kind == "write1"
                                   else json.loads(b)["codes"])
                            with self.codes_lock:
                                self.codes.extend(got)
                        except Exception:
                            pass
                    self.stats.add(ok, (time.perf_counter_ns() - t0) // 1000)
            except Exception:
                self.conn = None
                self.stats.add(False, (time.perf_counter_ns() - t0) // 1000)
            finally:
                self.q.task_done()


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--rate", type=float, default=1157,
                    help="links written per second (default ~100M/day)")
    ap.add_argument("--duration", type=float, default=600, help="seconds")
    ap.add_argument("--count", type=int, default=0,
                    help="total links to write (overrides duration)")
    ap.add_argument("--bulk", type=int, default=1,
                    help="urls per /api/shorten/bulk request")
    ap.add_argument("--read-rate", type=float, default=0,
                    help="redirects/s against written codes")
    ap.add_argument("--write-workers", type=int, default=16)
    ap.add_argument("--read-workers", type=int, default=16)
    ap.add_argument("--xff", action="store_true",
                    help="spoof random x-forwarded-for (server: TRUST_PROXY=1)")
    ap.add_argument("--verify", type=int, default=100,
                    help="codes sample-resolved after the run")
    ap.add_argument("--pid", type=int, default=0, help="server pid for RSS samples")
    ap.add_argument("--interval", type=float, default=10)
    args = ap.parse_args()

    wq, rq = queue.Queue(maxsize=10_000), queue.Queue(maxsize=10_000)
    wstats, rstats = Stats(), Stats()
    codes, codes_lock = [], threading.Lock()

    for _ in range(args.write_workers):
        SimWorker(args, wq, wstats, codes, codes_lock, True).start()
    if args.read_rate > 0:
        for _ in range(args.read_workers):
            SimWorker(args, rq, rstats, codes, codes_lock, False).start()

    total_links = args.count or int(args.rate * args.duration)
    per_req = max(1, args.bulk)
    req_rate = args.rate / per_req
    n_req = (total_links + per_req - 1) // per_req

    print(f"target  {args.url}")
    print(f"plan    {total_links:,} links @ {args.rate:,.0f}/s"
          f" ({args.rate * 86400 / 1e6:.1f}M/day)"
          + (f" via bulk x{per_req} -> {req_rate:,.1f} req/s" if per_req > 1 else "")
          + (f" + {args.read_rate:,.0f} redirects/s" if args.read_rate else ""))
    hdr = (f"{'t':>5} {'links':>12} {'w/s':>8} {'p50':>8} {'p99':>9} {'err':>5}"
           + (f" {'r/s':>8} {'rp99':>9} {'rerr':>5}" if args.read_rate else "")
           + (f" {'rss_mb':>8}" if args.pid else ""))
    print(hdr)

    t0 = time.monotonic()
    lag_warned = False
    rss0 = rss_kb(args.pid) if args.pid else 0
    mark = rmark = 0
    next_rep = args.interval
    next_read = t0 + 1.0 / args.read_rate if args.read_rate else 0

    i = 0
    while i < n_req:
        due = t0 + i / req_rate
        now = time.monotonic()
        if now < due:
            time.sleep(min(due - now, 0.05))
            continue
        if now - due > 5 and not lag_warned:
            print(f"  !! writer backlog {now - due:.1f}s — server can't keep "
                  f"up at {args.rate:,.0f}/s", file=sys.stderr)
            lag_warned = True
        urls = [url_for(i * per_req + j) for j in range(per_req)]
        wq.put(("write_bulk" if per_req > 1 else "write1",
                urls if per_req > 1 else urls[0]))
        i += 1

        while args.read_rate and now >= next_read:
            with codes_lock:
                if codes:
                    try:
                        rq.put_nowait(("read", random.choice(codes)))
                    except queue.Full:
                        pass  # readers lagging — drop rather than stall writes
                    if len(codes) > CODES_CAP:
                        del codes[: len(codes) - CODES_CAP]
            next_read += 1.0 / args.read_rate

        if now - t0 >= next_rep:
            ok, err, lat, mark = wstats.snapshot(mark)
            lat.sort()
            line = (f"{now - t0:>5.0f} {ok:>12,} {len(lat) / args.interval:>8,.0f}"
                    f" {fmt_lat(pct(lat, 0.5)):>8} {fmt_lat(pct(lat, 0.99)):>9}"
                    f" {err:>5}")
            if args.read_rate:
                rok, rerr, rlat, rmark = rstats.snapshot(rmark)
                rlat.sort()
                line += (f" {len(rlat) / args.interval:>8,.0f}"
                         f" {fmt_lat(pct(rlat, 0.99)):>9} {rerr:>5}")
            if args.pid:
                line += f" {rss_kb(args.pid) // 1024:>8}"
            print(line, flush=True)
            next_rep += args.interval

    wq.join()
    wall = time.monotonic() - t0

    for _ in range(args.write_workers):
        wq.put(None)
    if args.read_rate:
        rq.join()
        for _ in range(args.read_workers):
            rq.put(None)

    wok, werr, wlat, _ = wstats.snapshot(0)
    wlat.sort()
    got = wok * per_req
    rate = got / wall if wall else 0
    print()
    print(f"wrote   {got:,} links in {wall:.0f}s = {rate:,.0f}/s"
          f"  ({rate * 86400 / 1e6:.1f}M/day equivalent)")
    print(f"latency p50 {fmt_lat(pct(wlat, 0.5))}"
          f"  p99 {fmt_lat(pct(wlat, 0.99))}"
          f"  max {fmt_lat(pct(wlat, 0.999))}   errors {werr}")
    if args.read_rate:
        rok, rerr, rlat, _ = rstats.snapshot(0)
        rlat.sort()
        print(f"reads   {rok:,} ok, {rerr} err"
              f"  p50 {fmt_lat(pct(rlat, 0.5))}  p99 {fmt_lat(pct(rlat, 0.99))}")
    if args.pid:
        rss1 = rss_kb(args.pid)
        print(f"rss     {rss0 // 1024}MB -> {rss1 // 1024}MB"
              f"  (delta {(rss1 - rss0) // 1024:+}MB)")

    # verify: sampled codes must still resolve
    with codes_lock:
        sample = (random.sample(codes, min(args.verify, len(codes)))
                  if codes else [])
    bad = 0
    if sample:
        host, port = host_port(args.url)
        c = http.client.HTTPConnection(host, port, timeout=30)
        for code in sample:
            c.request("GET", f"/{code}")
            r = c.getresponse()
            r.read()
            if r.status not in (301, 302, 307, 308):
                bad += 1
        print(f"verify  {len(sample) - bad}/{len(sample)} sampled codes resolve")
    err_rate = werr / max(1, wok + werr)
    ok_run = err_rate < 0.01 and bad == 0
    print("result  " + ("PASS" if ok_run else
          f"FAIL (err_rate {err_rate:.1%}, bad_resolves {bad})"))
    sys.exit(0 if ok_run else 1)


if __name__ == "__main__":
    main()
