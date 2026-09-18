set dotenv-load

alias c := check
alias r := run

[private]
default:
    @just --list

check:
    uv run -- prek run --all-files

run:
    uv run -- fastapi dev ./src/trivial_transcoder/main.py --host 0.0.0.0

audit:
    uv audit

ci: audit check
