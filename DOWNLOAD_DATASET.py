import os
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# ===================================================================
# USER CONFIGURATION
# ===================================================================

# 1. SELECT DATASET SIZE (Options: "50K", "500K", "1M", "10M", "100M")
DATASET_SIZE = "1M"  

# 2. ROOT DIRECTORY
ROOT_DIR = "/home/admin/vectordataset"

# 3. GLOBAL SETTINGS
MAX_WORKERS = 10

# ===================================================================
# ================== DATASET REFERENCE TABLE ========================
# ===================================================================
# | Size Selector | Dataset Name    | Dimensions | Vector Count | Approx Size |
# |---------------|-----------------|------------|--------------|-------------|
# | "50K"         | OpenAI (Small)  | 1536       | 50,000       | ~450 MB     |
# | "500K"        | OpenAI (Medium) | 1536       | 500,000      | ~4.5 GB     |
# | "1M"          | Cohere (Medium) | 768        | 1,000,000    | ~4.5 GB     |
# | "10M"         | Cohere (Large)  | 768        | 10,000,000   | ~45 GB      |
# | "100M"        | LAION (Large)   | 768        | 100,000,000  | ~250 GB     |
# ===================================================================


# ===================================================================
# DATASET CONFIGURATIONS
# ===================================================================
DATASET_CONFIG = {
    "50K": {
        "name": "OpenAI (Small) 1536D",
        "base_url": "https://assets.zilliz.com/benchmark/openai_small_50k/",
        "rel_path": "openai/openai_small_50k",
        "train_prefix": "shuffle_train.parquet", # Single file
        "train_count": 1,
        "train_suffix": "",
        "extra_files": ["scalar_labels.parquet"]
    },
    "500K": {
        "name": "OpenAI (Medium) 1536D",
        "base_url": "https://assets.zilliz.com/benchmark/openai_medium_500k/",
        "rel_path": "openai/openai_medium_500k",
        "train_prefix": "shuffle_train.parquet", # Single file
        "train_count": 1,
        "train_suffix": "",
        "extra_files": ["scalar_labels.parquet"]
    },
    "1M": {
        "name": "Cohere (Medium) 768D",
        "base_url": "https://assets.zilliz.com/benchmark/cohere_medium_1m/",
        "rel_path": "cohere/cohere_medium_1m",
        "train_prefix": "shuffle_train.parquet", # Single file
        "train_count": 1,
        "train_suffix": "",
        "extra_files": ["scalar_labels.parquet"]
    },
    "10M": {
        "name": "Cohere (Large) 768D",
        "base_url": "https://assets.zilliz.com/benchmark/cohere_large_10m/",
        "rel_path": "cohere/cohere_large_10m",
        "train_prefix": "shuffle_train",
        "train_count": 10,
        "train_suffix": "of-10.parquet",
        "extra_files": ["scalar_labels.parquet"]
    },
    "100M": {
        "name": "LAION (Large) 768D",
        "base_url": "https://assets.zilliz.com/benchmark/laion_large_100m/",
        "rel_path": "laion/laion_large_100m",
        "train_prefix": "train",
        "train_count": 100,
        "train_suffix": "of-100.parquet",
        "extra_files": []
    }
}

def get_file_list(config):
    """Generates the list of files to download based on the config."""
    files = ["test.parquet", "neighbors.parquet"]
    
    # Add extra files (like scalar_labels) if they exist
    files.extend(config.get("extra_files", []))

    count = config["train_count"]
    
    # If count is 1, it's a single unpartitioned training file
    if count == 1:
        files.append(config["train_prefix"])
    else:
        # Generate partitioned files (e.g., shuffle_train-00-of-10.parquet)
        prefix = config["train_prefix"]
        suffix = config["train_suffix"]
        for i in range(count):
            files.append(f"{prefix}-{i:02d}-{suffix}")
            
    return files

def download_file(args):
    """Downloads a single file. args is (filename, base_url, dest_dir)."""
    filename, base_url, dest_dir = args
    local_file_path = os.path.join(dest_dir, filename)
    url = base_url + filename

    # 1. Skip if fully downloaded (File > 1KB)
    if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 1024:
        return f"Skipped (Exists): {filename}"

    try:
        # 2. Stream download
        with requests.get(url, stream=True, timeout=60) as r:
            if r.status_code == 404:
                return f"Missing (404): {filename}"
            
            r.raise_for_status()
            
            # Write to file
            with open(local_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                    f.write(chunk)
                  
        return f"Downloaded: {filename}"
        
    except Exception as e:
        return f"Error {filename}: {str(e)}"

if __name__ == "__main__":
    DATASET_SIZE = DATASET_SIZE.upper()
    if DATASET_SIZE not in DATASET_CONFIG:
        print(f"Error: Invalid dataset size '{DATASET_SIZE}'. Choose from: {list(DATASET_CONFIG.keys())}")
        exit(1)

    cfg = DATASET_CONFIG[DATASET_SIZE]
    
    # Construct the full destination path
    full_dest_path = os.path.join(ROOT_DIR, cfg["rel_path"])
    
    # Ensure directory exists
    os.makedirs(full_dest_path, exist_ok=True)
    
    files_to_download = get_file_list(cfg)

    print(f"--- Unified Downloader ---")
    print(f"Selected:    {DATASET_SIZE} ({cfg['name']})")
    print(f"Source:      {cfg['base_url']}")
    print(f"Destination: {full_dest_path}")
    print(f"File Count:  {len(files_to_download)}")
    print(f"Threads:     {MAX_WORKERS}\n")
    
    # Pack arguments: (filename, base_url, full_dest_path)
    map_args = [(f, cfg["base_url"], full_dest_path) for f in files_to_download]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(
            executor.map(download_file, map_args), 
            total=len(files_to_download), 
            unit="file"
        ))

    skipped = [r for r in results if r.startswith("Skipped")]
    failures = [r for r in results if "Error" in r or "Missing" in r]

    if skipped:
        print(f"\nSkipped {len(skipped)} already-present file(s):")
        for s in skipped:
            print(f"  {s}")

    if failures:
        print("\nSummary of Issues:")
        for fail in failures:
            print(fail)
    else:
        print("\nAll files processed successfully!")



# # =================================================================== #
# # ============ FOR COHERE 10M AND LAION 100M DATASET ================ # 
# # =================================================================== #


# import os
# import requests
# from concurrent.futures import ThreadPoolExecutor
# from tqdm import tqdm

# # ===================================================================
# # ======================== USER CONFIGURATION =======================

# # 1. SELECT DATASET
# DATASET_NAME = "cohere"  # Options: "cohere", "laion"

# # 2. ROOT DIRECTORY
# ROOT_DIR = "/home/admin/vectordataset"

# # ===================================================================

# # 3. DATASET DETAILS
# DATASET_CONFIG = {
#     "cohere": {
#         "base_url": "https://assets.zilliz.com/benchmark/cohere_large_10m/",
#         "rel_path": "cohere/cohere_large_10m",  # Subfolder inside ROOT_DIR
#         "train_prefix": "shuffle_train",
#         "train_count": 10,
#         "train_suffix": "of-10.parquet",
#         "extra_files": ["scalar_labels.parquet"] 
#     },
#     "laion": {
#         "base_url": "https://assets.zilliz.com/benchmark/laion_large_100m/",
#         "rel_path": "laion/laion_large_100m",   # Subfolder inside ROOT_DIR
#         "train_prefix": "train",
#         "train_count": 100,
#         "train_suffix": "of-100.parquet",
#         "extra_files": []
#     }
# }

# # 4. GLOBAL SETTINGS
# MAX_WORKERS = 10 


# def get_file_list(config):
#     """Generates the list of files based on the dataset config."""
#     files = ["test.parquet", "neighbors.parquet"]

#     # Add extra files (like scalar_labels) if they exist in config
#     files.extend(config["extra_files"])

#     # Generate Training Files
#     # Example: shuffle_train-00-of-10.parquet OR train-00-of-100.parquet
#     prefix = config["train_prefix"]
#     count = config["train_count"]
#     suffix = config["train_suffix"]

#     for i in range(count):
#         # Format usually needs leading zeros (00, 01..99)
#         files.append(f"{prefix}-{i:02d}-{suffix}")
    
#     return files

# def download_file(args):
#     """
#     Downloads a single file. args is (filename, base_url, full_local_path)
#     """
#     filename, base_url, dest_dir = args
#     local_file_path = os.path.join(dest_dir, filename)
#     url = base_url + filename

#     # 1. Skip if fully downloaded (File > 1KB)
#     if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 1024:
#         return f"Skipped (Exists): {filename}"

#     try:
#         # 2. Stream download
#         with requests.get(url, stream=True, timeout=60) as r:
#             if r.status_code == 404:
#                 return f"Missing (404): {filename}"
            
#             r.raise_for_status()
            
#             # Write to file
#             with open(local_file_path, 'wb') as f:
#                 for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
#                     f.write(chunk)
                  
#         return f"Downloaded: {filename}"
        
#     except Exception as e:
#         return f"Error {filename}: {str(e)}"

# if __name__ == "__main__":
#     if DATASET_NAME not in DATASET_CONFIG:
#         print(f"Error: Invalid dataset name '{DATASET_NAME}'")
#         exit(1)

#     cfg = DATASET_CONFIG[DATASET_NAME]
    
#     # Construct the full destination path
#     full_dest_path = os.path.join(ROOT_DIR, cfg["rel_path"])
    
#     # Ensure directory exists
#     os.makedirs(full_dest_path, exist_ok=True)
    
#     files_to_download = get_file_list(cfg)

#     print(f"--- Unified Downloader: {DATASET_NAME.upper()} ---")
#     print(f"Source:      {cfg['base_url']}")
#     print(f"Destination: {full_dest_path}")
#     print(f"File Count:  {len(files_to_download)}")
#     print(f"Threads:     {MAX_WORKERS}\n")
    
#     # Pack arguments: (filename, base_url, full_dest_path)
#     map_args = [(f, cfg["base_url"], full_dest_path) for f in files_to_download]

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         results = list(tqdm(
#             executor.map(download_file, map_args), 
#             total=len(files_to_download), 
#             unit="file"
#         ))

#     failures = [r for r in results if "Error" in r or "Missing" in r]
#     if failures:
#         print("\nSummary of Issues:")
#         for fail in failures:
#             print(fail)
#     else:
#         print("\nAll files processed successfully!")

