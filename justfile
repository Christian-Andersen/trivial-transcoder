set dotenv-load := true

export PYTHON_GIL := '0'

default:
    uv run -- fastapi dev ./src/trivial_transcoder/main.py --host 0.0.0.0
