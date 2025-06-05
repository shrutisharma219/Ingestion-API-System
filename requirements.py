# requirements.py
import subprocess
import sys

packages = [
    "fastapi",
    "uvicorn",
    "httpx",
    "pytest"
]

subprocess.check_call([sys.executable, "-m", "pip", "install", *packages])
