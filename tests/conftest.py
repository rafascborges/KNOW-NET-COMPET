import sys
from pathlib import Path

# Add the project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))
