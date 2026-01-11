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
        
        # 定义要清理的缓存目录列表
        # cl_data: 缠论计算数据缓存
        # klines: K线数据缓存
        # cache_pkl: 临时对象缓存
        cache_dirs = ["cl_data", "klines", "cache_pkl"]
        
        for dir_name in cache_dirs:
            target_path = os.path.join(path, dir_name)
            if os.path.exists(target_path):
                print(f"Cleaning {target_path} ...")
                # 遍历并删除文件，保留目录结构
                for root, dirs, files in os.walk(target_path):
                    for f in files:
                        try:
                            os.remove(os.path.join(root, f))
                        except Exception as e:
                            print(f"Failed to delete {f}: {e}")
                print(f"Cleared {target_path}")
            else:
                print(f"Directory not found: {target_path}")
                
    except Exception as e:
        print(f"Error clearing cache: {e}")

if __name__ == "__main__":
    clean_cache()
