import os
import shutil
import sys

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

from chanlun import config

def clean_cache():
    try:
        path = config.get_data_path()
        print(f"Data path: {path}")
        cl_data_path = os.path.join(path, "cl_data")
        if os.path.exists(cl_data_path):
            # shutil.rmtree(cl_data_path)
            # Just delete files inside to be safe
            for root, dirs, files in os.walk(cl_data_path):
                for f in files:
                    os.remove(os.path.join(root, f))
            print(f"Cache cleared in {cl_data_path}")
        else:
            print(f"Cache directory not found at {cl_data_path}")
    except Exception as e:
        print(f"Error clearing cache: {e}")

if __name__ == "__main__":
    clean_cache()
