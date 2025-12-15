import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from sources.contracts_source import Contracts2Source

class DummyDB:
    def save_documents_bulk(self, *args, **kwargs): pass
    def get_all_documents(self, *args, **kwargs): return []

def test_logging_creation():
    # Clean up existing log
    log_file = Path("logs/contracts2.log")
    if log_file.exists():
        log_file.unlink()
    
    print("Instantiating Contracts2Source...")
    # We can pass dummy paths since we won't run extract/transform
    source = Contracts2Source("dummy_path", DummyDB())
    
    if log_file.exists():
        print(f"SUCCESS: {log_file} created.")
        source.logger.info("Test log message")
        with open(log_file, 'r') as f:
            content = f.read()
            if "Test log message" in content:
                print("SUCCESS: Log message written to file.")
            else:
                print("FAILURE: Log message NOT found in file.")
    else:
        print(f"FAILURE: {log_file} NOT created.")

if __name__ == "__main__":
    test_logging_creation()
