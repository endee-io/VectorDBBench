import os
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# ===================================================================
# ==================== FOR COHERE 1M DATASET ========================
# ===================================================================

# --- CONFIGURATION ---
# Base URL for Cohere Medium (1M) dataset
BASE_URL = "https://assets.zilliz.com/benchmark/cohere_medium_1m/"

# Destination Directory
ROOT_DIR = "/home/debian/ssd/vectordataset"
LOCAL_DIR = os.path.join(ROOT_DIR, "cohere", "cohere_medium_1m")

# Number of parallel downloads
MAX_WORKERS = 5

# --- FILE LIST GENERATION ---
# For Cohere 1M, the benchmark uses a single training file.
files = [
    "test.parquet",
    "neighbors.parquet",
    "scalar_labels.parquet",
    "shuffle_train.parquet"
]

def download_file(filename):
    local_path = os.path.join(LOCAL_DIR, filename)
    url = BASE_URL + filename
    
    # 1. Skip if fully downloaded (File > 1KB)
    if os.path.exists(local_path) and os.path.getsize(local_path) > 1024:
        return f"✅ Skipped (Exists): {filename}"

    try:
        # 2. Stream download
        with requests.get(url, stream=True, timeout=60) as r:
            if r.status_code == 404:
                return f"❌ Missing (404): {filename}"
            
            r.raise_for_status()
            
            # Write to file
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                    f.write(chunk)
                  
        return f"⬇️ Downloaded: {filename}"
        
    except Exception as e:
        return f"⚠️ Error {filename}: {str(e)}"

if __name__ == "__main__":
    # Ensure directory exists
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    print(f"--- Cohere 1M (Medium) Downloader ---")
    print(f"Source:      {BASE_URL}")
    print(f"Destination: {LOCAL_DIR}")
    print(f"Files to check: {len(files)}")
    print(f"Parallel threads: {MAX_WORKERS}\n")
    
    # Start Parallel Download
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # We use tqdm to show a progress bar for the *number of files* completed
        results = list(tqdm(
            executor.map(download_file, files), 
            total=len(files), 
            unit="file"
        ))

    # Print summary of missing or failed files
    failures = [r for r in results if "Error" in r or "Missing" in r]
    if failures:
        print("\nSummary of Issues:")
        for fail in failures:
            print(fail)
    else:
        print("\n🎉 All files processed successfully!")