import asyncio
import csv
import os
import platform
import subprocess
import sys
import time
from typing import Dict, List

import aiohttp

# Configuration
SERVER_HOST = os.getenv("SERVER_HOST", "10.0.0.1")
HTTP_PORT = os.getenv("HTTP_PORT", "80")
HTTPS_PORT = os.getenv("HTTPS_PORT", "443")
XROOTD_PORT = os.getenv("XROOTD_PORT", "1094")

FILE_SIZES = ["1MB", "10MB", "100MB", "1GB", "10GB"]
CONCURRENCIES = [1, 5, 10]
RUNS = 10
PROTOCOLS = ["http", "https", "xrootd"]

OUTPUT_CSV = "performance_results.csv"
SYS_INFO_CSV = "system_metadata.csv"


def collect_system_metadata() -> None:
    """Collect hardware, OS, and software environment metadata."""
    print("Collecting system metadata...")

    def run_cmd(cmd: str) -> str:
        try:
            return subprocess.check_output(cmd, shell=True, text=True).strip()
        except Exception:
            return "N/A"

    cpu_cores = os.cpu_count() or "N/A"
    cpu_model = run_cmd("lscpu | grep 'Model name:' | sed 's/Model name:\\s*//'")
    cpu_freq = run_cmd("lscpu | grep 'CPU max MHz:' | awk '{print $4}'")
    xrootd_ver = run_cmd("xrootd -v 2>&1 | head -n1")

    metadata = [
        ("OS_System", platform.system()),
        ("OS_Release", platform.release()),
        ("OS_Version", platform.version()),
        ("Architecture", platform.machine()),
        ("Python_Version", platform.python_version()),
        ("CPU_Model", cpu_model),
        ("CPU_Cores", cpu_cores),
        ("CPU_Max_MHz", cpu_freq),
        ("XRootD_Version", xrootd_ver),
    ]

    with open(SYS_INFO_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerows(metadata)


async def fetch_http(session: aiohttp.ClientSession, url: str) -> None:
    """Fetch HTTP/HTTPS payload asynchronously."""
    async with session.get(url, ssl=False) as response:
        await response.read()


def fetch_xrootd(url: str) -> None:
    """Execute XRootD file fetch via xrdcp subprocess."""
    subprocess.run(
        ["xrdcp", "-f", url, "/dev/null"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )


async def execute_transfer(
    session: aiohttp.ClientSession, protocol: str, url: str, concurrency: int
) -> float:
    """Execute transfers concurrently and return elapsed duration in seconds."""
    start_time = time.perf_counter()

    if protocol in ("http", "https"):
        tasks = [fetch_http(session, url) for _ in range(concurrency)]
        await asyncio.gather(*tasks)
    elif protocol == "xrootd":
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(None, fetch_xrootd, url)
            for _ in range(concurrency)
        ]
        await asyncio.gather(*tasks)

    return time.perf_counter() - start_time


async def main() -> None:
    collect_system_metadata()

    # Initialize CSV Headers
    with open(OUTPUT_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Protocol", "FileSize", "ParallelRequests", "RunIndex", "DurationSeconds"]
        )

    print("Starting performance test suite...")
    connector = aiohttp.TCPConnector(ssl=False, limit=0)

    async with aiohttp.ClientSession(connector=connector) as session:
        for proto in PROTOCOLS:
            for size in FILE_SIZES:
                for concurrency in CONCURRENCIES:
                    for run_idx in range(1, RUNS + 1):
                        if proto == "http":
                            url = f"http://{SERVER_HOST}:{HTTP_PORT}/test_{size}.dat"
                        elif proto == "https":
                            url = f"https://{SERVER_HOST}:{HTTPS_PORT}/test_{size}.dat"
                        elif proto == "xrootd":
                            url = f"root://{SERVER_HOST}:{XROOTD_PORT}//test_{size}.dat"

                        print(
                            f"Running: Protocol={proto:<6} | Size={size:<5} | "
                            f"Concurrency={concurrency:<2} | Iteration={run_idx}/{RUNS}"
                        )

                        try:
                            duration = await execute_transfer(
                                session, proto, url, concurrency
                            )
                        except Exception as err:
                            print(f"  -> Execution error: {err}")
                            duration = -1.0

                        with open(OUTPUT_CSV, mode="a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow(
                                [proto, size, concurrency, run_idx, f"{duration:.6f}"]
                            )

    print(f"\nBenchmarking completed. Saved outputs to {OUTPUT_CSV} and {SYS_INFO_CSV}.")


if __name__ == "__main__":
    asyncio.run(main())
