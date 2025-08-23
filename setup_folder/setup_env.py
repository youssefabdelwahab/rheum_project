import os
import subprocess
import sys
from pathlib import Path

venv_name = ".robust_lab"
venv_path = Path(venv_name)

# Step 1: Create venv if it doesn't exist
if not venv_path.exists():
    subprocess.run([sys.executable, "-m", "venv", venv_name])
    print(f"✅ Created virtual environment at {venv_name}")
else:
    print(f"ℹ️ Virtual environment '{venv_name}' already exists")

# Step 2: Determine pip path
if os.name == "nt":
    pip_path = venv_path / "Scripts" / "pip.exe"
else:
    pip_path = venv_path / "bin" / "pip"

# Step 3: Upgrade pip and install requirements
subprocess.run([str(pip_path), "install", "--upgrade", "pip"])
subprocess.run([str(pip_path), "install", "-r", "/work/robust_ai_lab/rheum_project/requirements.txt"])

print("✅ All dependencies installed.")
