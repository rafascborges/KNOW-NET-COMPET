import sys
import os
import shutil
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from sources.contracts_source import Contracts2Source

class DummyDB:
    def save_documents_bulk(self, *args, **kwargs): pass
    def get_all_documents(self, *args, **kwargs): return []

def test_env_logging():
    # Set env vars
    custom_log_dir = "custom_logs"
    os.environ["LOG_PATH"] = custom_log_dir
    os.environ["LOG_LEVEL"] = "DEBUG"
    
    # Clean up
    if Path(custom_log_dir).exists():
        shutil.rmtree(custom_log_dir)
        
    print(f"Testing with LOG_PATH={custom_log_dir} and LOG_LEVEL=DEBUG")
    
    source = Contracts2Source("dummy_path", DummyDB())
    
    log_file = Path(custom_log_dir) / "contracts2.log"
    
    if log_file.exists():
        print(f"SUCCESS: {log_file} created.")
        
        # Check log level
        if source.logger.level == 10: # DEBUG is 10
            print("SUCCESS: Logger level is DEBUG.")
        else:
            print(f"FAILURE: Logger level is {source.logger.level}, expected 10.")
            
        source.logger.debug("Debug message")
        
        with open(log_file, 'r') as f:
            content = f.read()
            if "Debug message" in content:
                print("SUCCESS: Debug message written to file.")
            else:
                print("FAILURE: Debug message NOT found in file.")
                
    else:
        print(f"FAILURE: {log_file} NOT created.")
        
    # Cleanup
    if Path(custom_log_dir).exists():
        shutil.rmtree(custom_log_dir)

if __name__ == "__main__":
    test_env_logging()
