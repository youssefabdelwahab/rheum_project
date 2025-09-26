#!/bin/bash

PROJECT_ROOT="$(dirname "$(realpath "$0")")/.."


# Step 1: Create virtual environment
python3.11 -m venv "$PROJECT_ROOT/.robust_lab"

# Step 2: Activate the virtual environment
source .robust_lab/bin/activate

# Step 3: Upgrade pip (optional but recommended)
pip install --upgrade pip

# Step 4: Install dependencies
pip install -r /work/robust_ai_lab/rheum_project/requirements.txt

echo "✅ Environment '.robust_lab' is ready and all dependencies are installed."
