import subprocess
import os

cmd = r".venv\Scripts\activate && sam run"

subprocess.run(cmd, shell=True)