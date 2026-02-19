#!/usr/bin/env python3
"""
makefile.py - Build system for StoreMy Go project.

Usage:
    python makefile.py <target>
    make <target>           (delegates here via Makefile)

All targets are identical to the original Makefile targets.
Requires: Python 3.7+
Optional: pip install colorama   (for colored terminal output)
"""

import os
import platform
import shutil
import subprocess
import sys

try:
    from colorama import Fore, Style
    from colorama import init as _colorama_init

    _colorama_init(autoreset=True)
    HAS_COLOR = True
except ImportError:

    class _Stub:
        def __getattr__(self, _):
            return ""

    Fore = _Stub()
    Style = _Stub()
    HAS_COLOR = False

if not HAS_COLOR:
    print(
        "[INFO] colorama not installed. Run: pip install colorama  (optional, for colors)"
    )


def print_header(title):
    bar = Fore.CYAN + Style.BRIGHT + "=" * 52 + Style.RESET_ALL
    label = Fore.CYAN + Style.BRIGHT + f"  {title}" + Style.RESET_ALL
    print(f"\n{bar}\n{label}\n{bar}")


def print_step(msg):
    print(f"{Fore.YELLOW}-->{Style.RESET_ALL} {msg}")


def print_success(msg):
    print(f"{Fore.GREEN}[OK]{Style.RESET_ALL} {msg}")


def print_warn(msg):
    print(f"{Fore.YELLOW}[WARN]{Style.RESET_ALL} {msg}")


def run_cmd(args, allow_failure=False):
    """
    Run a command as a subprocess, streaming output directly to the terminal.
    Exits with the subprocess exit code on failure unless allow_failure=True.
    stdout/stderr are NOT captured — Go and Docker color output passes through unmodified.
    """
    try:
        result = subprocess.run(args)
    except FileNotFoundError:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Command not found: '{args[0]}'")
        print(f"        Ensure '{args[0]}' is installed and on your PATH.")
        if not allow_failure:
            sys.exit(127)
        return 127
    if result.returncode != 0 and not allow_failure:
        sys.exit(result.returncode)
    return result.returncode


def target_test():
    print_header("Running All Tests")
    run_cmd(["go", "test", "./...", "-v"])


def target_test_tables():
    print_header("Running Table Tests")
    run_cmd(["go", "test", "./pkg/tables/...", "-v"])


def target_test_coverage():
    print_header("Running Tests with Coverage")
    run_cmd(["go", "test", "./...", "-cover"])


def target_test_watch():
    print_header("Test Watch Mode (all packages)")
    print_step("Using 'gow' for cross-platform file watching — press Ctrl+C to stop")
    print_step("Install gow first if needed:  make install-tools")
    run_cmd(["gow", "test", "./...", "-v"])


def target_test_watch_tables():
    print_header("Test Watch Mode (tables package)")
    print_step("Using 'gow' for cross-platform file watching — press Ctrl+C to stop")
    print_step("Install gow first if needed:  make install-tools")
    run_cmd(["gow", "test", "./pkg/tables/...", "-v"])


def target_install_tools():
    print_header("Installing Development Tools")
    system = platform.system()
    if system == "Linux":
        print_step("Installing inotify-tools (Linux)...")
        run_cmd(["sudo", "apt-get", "update"], allow_failure=True)
        run_cmd(
            ["sudo", "apt-get", "install", "-y", "inotify-tools"], allow_failure=True
        )
    else:
        print_step(
            f"Skipping inotify-tools ({system}) — 'gow' is used instead on this platform"
        )
    print_step("Installing gow (cross-platform Go file watcher)...")
    run_cmd(["go", "install", "github.com/mitranim/gow@latest"], allow_failure=True)
    print_step("Installing govulncheck (Go vulnerability scanner)...")
    run_cmd(
        ["go", "install", "golang.org/x/vuln/cmd/govulncheck@latest"],
        allow_failure=True,
    )
    print_step("Installing gosec (Go security analyser)...")
    run_cmd(
        ["go", "install", "github.com/securego/gosec/v2/cmd/gosec@latest"],
        allow_failure=True,
    )
    print_success("Tools installation complete!")


def target_clean():
    print_header("Cleaning Test Cache")
    run_cmd(["go", "clean", "-testcache"])


def target_build():
    print_header("Building StoreMy")
    os.makedirs("bin", exist_ok=True)
    run_cmd(["go", "build", "-o", "bin/storemy", "./"])
    print_success("Binary written to bin/storemy")


def target_fmt():
    print_header("Formatting Code")
    run_cmd(["go", "fmt", "./..."])


def target_vet():
    print_header("Vetting Code")
    run_cmd(["go", "vet", "./..."])


def target_lint():
    print_header("Running Linter")
    run_cmd(["golangci-lint", "run"], allow_failure=True)


def target_lint_fix():
    print_header("Running Linter with Auto-fix")
    run_cmd(["golangci-lint", "run", "--fix"])


def target_tidy():
    print_header("Tidying Go Modules")
    run_cmd(["go", "mod", "tidy"])
    run_cmd(["go", "mod", "verify"])


def target_coverage_html():
    print_header("Generating HTML Coverage Report")
    run_cmd(["go", "test", "./...", "-coverprofile=coverage.out"])
    run_cmd(["go", "tool", "cover", "-html=coverage.out", "-o", "coverage.html"])
    print_success("Coverage report written to coverage.html")


def target_bench():
    print_header("Running Benchmarks")
    run_cmd(["go", "test", "-v", "-run", "TestBenchmarkAndSaveResults", "./..."])


def target_check():
    print_header("Full Check: fmt + vet + test")
    target_fmt()
    target_vet()
    target_test()


def target_run():
    print_header("Running StoreMy (db: mydb)")
    run_cmd(["go", "run", ".", "-db", "mydb", "-data", "./data"])


def target_run_fresh():
    print_header("Running StoreMy on Fresh Database")
    fresh_path = os.path.join("tmp", "data", "testdb")
    if os.path.exists(fresh_path):
        try:
            shutil.rmtree(fresh_path)
            print_step(f"Removed {fresh_path}")
        except OSError as exc:
            print_warn(f"Could not remove {fresh_path}: {exc}")
    run_cmd(["go", "run", ".", "-db", "testdb", "-data", "./tmp/data"])


def target_examples():
    print_header("Running WAL Examples")
    run_cmd(["go", "run", "pkg/examples/main.go"])



def target_docker_demo():
    print_header("Docker Demo (with demo data)")
    print_step("Use Ctrl+E to execute queries, Ctrl+H for help, Ctrl+Q to quit")
    run_cmd(["docker-compose", "up", "storemy-demo"])


def target_docker_import():
    print_header("Docker Import (sample SQL)")
    run_cmd(["docker-compose", "up", "storemy-import"])


def target_docker_fresh():
    print_header("Docker Fresh (empty database)")
    run_cmd(["docker-compose", "up", "storemy-fresh"])


def target_docker_test():
    print_header("Docker Automated CRUD Tests")
    run_cmd(["docker-compose", "run", "--rm", "storemy-test"])


def target_docker_build():
    print_header("Building Docker Image")
    run_cmd(["docker-compose", "build"])


def target_docker_clean():
    print_header("Docker Clean (containers + volumes)")
    run_cmd(["docker-compose", "down", "-v"])


def target_docker_stop():
    print_header("Stopping Docker Containers")
    run_cmd(["docker-compose", "down"])


def target_quickstart():
    print_header("Quickstart")
    target_docker_build()
    target_docker_demo()


# ── Security ────────────────────────────────────────────────────────────────────

def target_security_vuln():
    print_header("Go Vulnerability Check (govulncheck)")
    run_cmd(["govulncheck", "./..."])


def target_security_scan():
    print_header("Go Security Analysis (gosec)")
    run_cmd(
        [
            "gosec",
            "-severity", "medium",
            "-confidence", "medium",
            "-exclude-dir=pkg/examples",
            "./...",
        ],
        allow_failure=True,
    )


def target_security():
    print_header("Full Security Check: govulncheck + gosec")
    target_security_vuln()
    target_security_scan()


TARGETS = {
    "test": (target_test, "Run all tests", "Testing"),
    "test-tables": (target_test_tables, "Run table tests only", "Testing"),
    "test-coverage": (target_test_coverage, "Run tests with coverage", "Testing"),
    "test-watch": (
        target_test_watch,
        "Watch and re-run all tests [uses gow]",
        "Testing",
    ),
    "test-watch-tables": (
        target_test_watch_tables,
        "Watch and re-run table tests [uses gow]",
        "Testing",
    ),
    "install-tools": (
        target_install_tools,
        "Install dev tools (gow, inotify-tools)",
        "Tools",
    ),
    "clean": (target_clean, "Clean test cache", "Tools"),
    "build": (target_build, "Build binary (bin/storemy)", "Build"),
    "fmt": (target_fmt, "Format all Go code", "Build"),
    "vet": (target_vet, "Vet all Go code", "Build"),
    "lint": (target_lint, "Run golangci-lint", "Build"),
    "lint-fix": (target_lint_fix, "Run golangci-lint with auto-fix", "Build"),
    "tidy": (target_tidy, "go mod tidy + go mod verify", "Build"),
    "coverage-html": (target_coverage_html, "Generate coverage.html report", "Build"),
    "bench": (target_bench, "Run TestBenchmarkAndSaveResults", "Build"),
    "check": (target_check, "fmt + vet + test", "Build"),
    "run": (target_run, "Run StoreMy (db: mydb, data: ./data)", "Run"),
    "run-fresh": (target_run_fresh, "Wipe testdb and run StoreMy fresh", "Run"),
    "examples": (target_examples, "Run WAL transaction examples", "Run"),
    "docker-demo": (
        target_docker_demo,
        "Start with demo data [recruiter-friendly]",
        "Docker",
    ),
    "docker-import": (target_docker_import, "Start with sample SQL import", "Docker"),
    "docker-fresh": (target_docker_fresh, "Start fresh empty database", "Docker"),
    "docker-test": (target_docker_test, "Run automated CRUD tests in Docker", "Docker"),
    "docker-build": (target_docker_build, "Build Docker image", "Docker"),
    "docker-clean": (
        target_docker_clean,
        "Stop containers and remove volumes",
        "Docker",
    ),
    "docker-stop": (target_docker_stop, "Stop Docker containers", "Docker"),
    "quickstart": (
        target_quickstart,
        "docker-build + docker-demo (one command)",
        "Docker",
    ),
    "security": (target_security, "govulncheck + gosec (full security check)", "Security"),
    "security-vuln": (target_security_vuln, "Run govulncheck (known CVEs)", "Security"),
    "security-scan": (target_security_scan, "Run gosec (static security analysis)", "Security"),
    "help": (None, "Show this help message", "Meta"),
}



def target_help():
    from collections import defaultdict

    title = (
        Fore.CYAN
        + Style.BRIGHT
        + "StoreMy Database - Available Commands"
        + Style.RESET_ALL
    )
    print(f"\n{title}\n")
    groups = defaultdict(list)
    for name, (_, desc, group) in TARGETS.items():
        groups[group].append((name, desc))
    group_order = ["Docker", "Testing", "Build", "Run", "Security", "Tools", "Meta"]
    for group in group_order:
        if group not in groups:
            continue
        header = Fore.YELLOW + Style.BRIGHT + f"{group}:" + Style.RESET_ALL
        print(header)
        for name, desc in groups[group]:
            padded = name.ljust(24)
            print(f"  {Fore.GREEN}{padded}{Style.RESET_ALL}  {desc}")
        print()
    if not HAS_COLOR:
        print("  Tip: pip install colorama  to enable colored output\n")


TARGETS["help"] = (target_help, "Show this help message", "Meta")


def main():
    if len(sys.argv) < 2:
        target_help()
        sys.exit(0)

    name = sys.argv[1]

    if name not in TARGETS:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Unknown target: '{name}'")
        print("  Run:  python makefile.py help  to list all available targets.")
        sys.exit(1)

    func, _, _ = TARGETS[name]
    func()


if __name__ == "__main__":
    main()
