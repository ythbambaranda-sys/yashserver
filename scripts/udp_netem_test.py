"""UDP reliability against real kernel packet impairment (``tc netem``).

Run it inside an unprivileged network namespace, which needs no root and
cannot touch the host's networking::

    unshare -r -n -- python3 scripts/udp_netem_test.py

``unshare -r`` maps the caller to root *inside a new user namespace*, which
grants ``CAP_NET_ADMIN`` there; ``-n`` gives that namespace its own network
stack whose ``lo`` is ours to damage. Nothing outside is affected, and the
qdisc disappears with the namespace even if this process dies badly.

Why this is worth having on top of ``tests/test_udp_impairment.py``: that test
impairs traffic in userspace, which proves the protocol logic copes. This one
impairs it in the kernel, below the socket layer, which proves it copes with a
real network stack -- genuine queueing, genuine timing, genuine reordering.

One property of testing on loopback is worth knowing: both directions traverse
``lo``, so impairment is applied to the data *and* to the acknowledgement. A
"10% loss" scenario therefore loses roughly 19% of round trips, which makes
these numbers pessimistic rather than flattering.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise SystemExit(
            f"command failed: {' '.join(command)}\n{result.stdout}{result.stderr}"
        )
    return result


def have_net_admin() -> bool:
    probe = subprocess.run(
        ["tc", "qdisc", "show", "dev", "lo"], capture_output=True, text=True
    )
    if probe.returncode != 0:
        return False
    trial = subprocess.run(
        ["tc", "qdisc", "replace", "dev", "lo", "root", "netem", "loss", "0%"],
        capture_output=True,
        text=True,
    )
    if trial.returncode != 0:
        return False
    subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)
    return True


def apply_netem(spec: str | None) -> None:
    if spec is None:
        subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)
        return
    run(["tc", "qdisc", "replace", "dev", "lo", "root", "netem", *spec.split()])


def current_qdisc() -> str:
    return run(["tc", "qdisc", "show", "dev", "lo"]).stdout.strip()


# ---------------------------------------------------------------------------
# the actual transfer
# ---------------------------------------------------------------------------


async def exchange(
    count: int, *, ordered: bool, retry_interval: float, max_retries: int, timeout: float
) -> dict:
    """Send ``count`` messages over a reliable channel and see what arrives."""

    server = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
    received: list[bytes] = []
    server_channel = yashserver.ReliableUdpChannel(
        server,
        retry_interval_seconds=retry_interval,
        max_retries=max_retries,
        ordered=ordered,
        reorder_window=256,
        reorder_timeout_seconds=5.0,
    )

    @server_channel.on_message
    async def _on_message(payload, _endpoint):
        received.append(payload)

    server.on_datagram(server_channel.handle_datagram)
    await server.start()

    client = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
    client_channel = yashserver.ReliableUdpChannel(
        client,
        retry_interval_seconds=retry_interval,
        max_retries=max_retries,
        ordered=ordered,
    )
    client.on_datagram(client_channel.handle_datagram)
    await client.start()
    failures: list = []
    client_channel.on_delivery_failed(lambda *args: failures.append(args))

    endpoint = yashserver.UdpEndpoint("127.0.0.1", server.bound_port)
    expected = [f"m-{i:04d}".encode() for i in range(count)]

    wanted = set(expected)
    started = time.time()
    try:
        for payload in expected:
            await client_channel.send(endpoint, payload)
        deadline = time.time() + timeout
        while time.time() < deadline:
            # Wait on the *expected* payloads. Counting unique arrivals would
            # let a corrupted payload stand in for a missing one and end the
            # wait early.
            if wanted.issubset(received) or failures:
                break
            await asyncio.sleep(0.02)
        elapsed = time.time() - started
    finally:
        await client.stop()
        await server.stop()

    unique = set(received)
    # Compare against what was actually sent, not merely count what arrived:
    # netem's `corrupt` flips bits, and loopback does not always verify UDP
    # checksums, so a mangled payload can be delivered rather than dropped.
    # Counting unique arrivals would score that as a success.
    corrupted = unique - wanted
    return {
        "sent": count,
        "delivered": len(unique & wanted),
        "corrupted": len(corrupted),
        "duplicates_leaked": len(received) - len(unique),
        "in_order": received == expected if ordered else None,
        "failures": len(failures),
        "elapsed": elapsed,
        "complete": unique >= wanted,
    }


SCENARIOS: list[tuple[str, str | None]] = [
    ("baseline (no impairment)", None),
    ("loss 10%", "loss 10%"),
    ("loss 30%", "loss 30%"),
    ("delay 50ms +/-20ms normal", "delay 50ms 20ms distribution normal"),
    ("duplicate 10%", "duplicate 10%"),
    ("reorder 25% (needs delay)", "delay 10ms reorder 25% 50%"),
    ("corrupt 5%", "corrupt 5%"),
    ("loss 15% + dup 10% + reorder 20%", "delay 20ms 10ms loss 15% duplicate 10% reorder 20% 50%"),
]


async def main_async(args) -> int:
    print("=" * 74)
    print(" UDP reliability under kernel packet impairment (tc netem)")
    try:
        with open("/etc/os-release") as handle:
            pretty = [l for l in handle if l.startswith("PRETTY_NAME")][0]
            print(" " + pretty.split("=", 1)[1].strip().strip('"'))
    except (OSError, IndexError):
        pass
    print(f" kernel: {os.uname().release}")
    print("=" * 74)
    print(
        "\nNote: on loopback both directions cross this qdisc, so data *and*\n"
        "acknowledgements are impaired. Effective round-trip loss is roughly\n"
        "double the configured figure.\n"
    )

    header = (
        f"{'scenario':<36}{'deliv':>8}{'corrupt':>9}{'dupes':>7}"
        f"{'order':>7}{'fail':>6}{'time':>8}"
    )
    print(header)
    print("-" * len(header))

    ok = True
    for label, spec in SCENARIOS:
        apply_netem(spec)
        try:
            result = await exchange(
                args.count,
                ordered=args.ordered,
                retry_interval=args.retry_interval,
                max_retries=args.max_retries,
                timeout=args.timeout,
            )
        finally:
            apply_netem(None)

        order = "-" if result["in_order"] is None else ("yes" if result["in_order"] else "NO")
        status = "" if result["complete"] else "  <-- INCOMPLETE"
        if not result["complete"]:
            ok = False
        if result["in_order"] is False:
            ok = False
        if result["duplicates_leaked"]:
            ok = False
        if result["corrupted"]:
            ok = False
            status = "  <-- CORRUPT PAYLOADS DELIVERED"
        print(
            f"{label:<36}{result['delivered']:>4}/{result['sent']:<3}"
            f"{result['corrupted']:>9}{result['duplicates_leaked']:>7}"
            f"{order:>7}{result['failures']:>6}"
            f"{result['elapsed']:>7.1f}s{status}"
        )

    print("-" * len(header))
    print("delivered = unique messages that arrived matching what was sent")
    print("corrupt   = payloads delivered that were NOT sent (must be 0)")
    print("dupes     = duplicates that leaked past suppression (must be 0)")
    print("order     = in-order delivery held (ordered mode only)")
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=60)
    parser.add_argument("--ordered", action="store_true", help="require in-order delivery")
    parser.add_argument("--retry-interval", type=float, default=0.1)
    parser.add_argument("--max-retries", type=int, default=40)
    parser.add_argument("--timeout", type=float, default=90.0)
    args = parser.parse_args()

    if sys.platform != "linux":
        raise SystemExit("tc netem is Linux-only")

    # lo starts down in a fresh namespace.
    subprocess.run(["ip", "link", "set", "lo", "up"], capture_output=True)

    if not have_net_admin():
        raise SystemExit(
            "no CAP_NET_ADMIN for tc. Re-run inside an unprivileged network\n"
            "namespace, which needs no root and cannot affect the host:\n\n"
            "    unshare -r -n -- python3 scripts/udp_netem_test.py\n"
        )

    try:
        return asyncio.run(main_async(args))
    finally:
        apply_netem(None)


if __name__ == "__main__":
    raise SystemExit(main())
