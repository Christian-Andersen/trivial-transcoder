set dotenv-load

alias c := check
alias r := run
alias ci := continuous_integration

[private]
default:
    @just --list

check:
    uvx -- prek run --all-files

run:
    uv run -- fastapi dev ./src/trivial_transcoder/main.py --host 0.0.0.0

audit:
    uv audit

continuous_integration: audit check
