#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import re
import sys


COMPOSE_RE = re.compile(r"^(?P<service>[A-Za-z0-9_.-]+)\s+\|\s?(?P<message>.*)$")
DOCKER_TS_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T(?P<time>\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\s+(?P<body>.*)$"
)
APP_LOG_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}) (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+(?P<body>.*)$"
)
PYTHON_LOG_RE = re.compile(r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL):[^:]+:(?P<body>.*)$")
KV_RE = re.compile(r"\b(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)=(?P<value>\"[^\"]*\"|'[^']*'|[^|\s]+)")

RESET = "\033[0m"
BOLD = "1"
DIM = "2"


SERVICE_COLORS = (
    ("rabbitmq", 244),
    ("gateway", 201),
    ("client_", 83),
    ("file_splitter_", 50),
    ("file_ingestor_", 45),
    ("filter_usd_", 214),
    ("filter_q1_", 226),
    ("filter_date_", 208),
    ("filter_q5_format_", 202),
    ("filter_q5_usd_", 166),
    ("sum_q2_", 39),
    ("sum_q3_", 75),
    ("aggregation_q2_", 141),
    ("aggregation_q3_", 177),
    ("aggregation_q5_", 133),
    ("join_q2", 51),
    ("join_q3", 87),
    ("join_q5", 129),
    ("q2_bank_name_joiner", 87),
    ("q3_barrier_", 207),
    ("q4_filter_", 118),
    ("q4_sum_", 44),
    ("q4_joiner_", 196),
    ("q4_aggregator_", 203),
    ("q4_deduper_", 111),
    ("scatter_gather_mapper_", 118),
    ("scatter_gather_linker_", 44),
    ("scatter_gather_detector_", 196),
    ("rates_service", 172),
    ("monitor_", 81),
)

FALLBACK_COLORS = (33, 35, 37, 41, 43, 47, 49, 69, 111, 147, 203, 209)
LEVEL_COLORS = {
    "DEBUG": 244,
    "INFO": 34,
    "WARNING": 220,
    "ERROR": 196,
    "CRITICAL": 196,
}
LEVEL_LABELS = {
    "DEBUG": "DEBUG",
    "INFO": "INFO ",
    "WARNING": "WARN ",
    "ERROR": "ERROR",
    "CRITICAL": "CRIT ",
}


def main() -> int:
    args = parse_args()
    color = should_color(args.color)
    tee_file = open(args.tee_file, "a", encoding="utf-8", buffering=1) if args.tee_file else None

    try:
        for raw_line in sys.stdin:
            if tee_file:
                tee_file.write(raw_line)
            for line in clean_lines(raw_line):
                sys.stdout.write(format_line(line, color, args.service_width) + "\n")
                sys.stdout.flush()
    finally:
        if tee_file:
            tee_file.close()

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretty-print docker compose logs.")
    parser.add_argument(
        "--color",
        choices=("auto", "always", "never"),
        default=os.environ.get("LOG_COLOR", "auto"),
        help="When to emit ANSI colors.",
    )
    parser.add_argument(
        "--service-width",
        type=int,
        default=int(os.environ.get("LOG_SERVICE_WIDTH", "26")),
        help="Width reserved for the compose service name.",
    )
    parser.add_argument(
        "--tee-file",
        help="Also write the unformatted input stream to this file.",
    )
    return parser.parse_args()


def should_color(mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


def format_line(line: str, color: bool, service_width: int) -> str:
    match = COMPOSE_RE.match(line)
    if not match:
        return line

    service = match.group("service")
    message = match.group("message")
    timestamp, message = extract_docker_timestamp(message)
    app_time, level, message = extract_app_log(message)
    timestamp = timestamp or app_time or "        "

    service_part = paint(pad_service(service, service_width), service_color(service), color)
    time_part = paint(timestamp, None, color, style=DIM)
    level_part = format_level(level, color)
    body = format_body(message, color)

    return f"{time_part} {service_part} {level_part} {body}".rstrip()


def clean_lines(line: str) -> list[str]:
    return [sanitize_line(part) for part in line.splitlines()]


def sanitize_line(line: str) -> str:
    return "".join(
        char if char == "\t" or ord(char) >= 32 else " "
        for char in line
    )


def extract_docker_timestamp(message: str) -> tuple[str | None, str]:
    match = DOCKER_TS_RE.match(message)
    if not match:
        return None, message
    return match.group("time"), match.group("body")


def extract_app_log(message: str) -> tuple[str | None, str | None, str]:
    app_match = APP_LOG_RE.match(message)
    if app_match:
        return app_match.group("time"), app_match.group("level"), app_match.group("body")

    python_match = PYTHON_LOG_RE.match(message)
    if python_match:
        return None, python_match.group("level"), python_match.group("body")

    return None, None, message


def pad_service(service: str, width: int) -> str:
    if len(service) <= width:
        return service.ljust(width)
    if width <= 1:
        return service
    return service[: width - 1] + "."


def service_color(service: str) -> int:
    for prefix, color in SERVICE_COLORS:
        if service == prefix or service.startswith(prefix):
            return color
    index = sum(ord(char) for char in service) % len(FALLBACK_COLORS)
    return FALLBACK_COLORS[index]


def format_level(level: str | None, color: bool) -> str:
    if not level:
        return paint("     ", None, color, style=DIM)

    label = LEVEL_LABELS.get(level, level[:5].ljust(5))
    level_color = LEVEL_COLORS.get(level, 250)
    style = BOLD if level in {"ERROR", "CRITICAL"} else None
    return paint(label, level_color, color, style=style)


def format_body(message: str, color: bool) -> str:
    if not message:
        return ""

    parts = message.split(" | ")
    if len(parts) == 1:
        return format_key_values(message, color)

    event = paint(parts[0], None, color, style=BOLD)
    details = [format_key_values(part, color) for part in parts[1:]]
    return " | ".join([event, *details])


def format_key_values(text: str, color: bool) -> str:
    def replace(match: re.Match[str]) -> str:
        key = paint(match.group("key"), 245, color)
        value = paint(match.group("value"), 255, color)
        return f"{key}={value}"

    return KV_RE.sub(replace, text)


def paint(text: str, color_code: int | None, enabled: bool, style: str | None = None) -> str:
    if not enabled:
        return text

    codes = []
    if style:
        codes.append(style)
    if color_code is not None:
        codes.append(f"38;5;{color_code}")
    if not codes:
        return text
    return f"\033[{';'.join(codes)}m{text}{RESET}"


if __name__ == "__main__":
    raise SystemExit(main())
