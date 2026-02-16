import sys
import os
import subprocess
import shutil
import platform
import urllib.request

# ================= CONFIGURATION =================
REPO_URL = "https://github.com/endee-io/VectorDBBench.git"
REPO_DIR = "VectorDBBench"
PYTHON_VERSION = "3.11.9"
# =================================================

def run_command(command, shell=False, cwd=None):
    """Runs a shell command and exits on failure."""
    use_shell = shell
    # On Windows, list commands usually need shell=True to find executables
    if platform.system() == "Windows" and isinstance(command, list):
        use_shell = True
        
    cmd_str = ' '.join(command) if isinstance(command, list) else command
    print(f"--> [EXEC]: {cmd_str}")
    
    try:
        subprocess.check_call(command, shell=use_shell, cwd=cwd)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        sys.exit(1)

def get_os_type():
    """Returns 'windows', 'macos', or 'linux'."""
    os_name = platform.system().lower()
    
    if "darwin" in os_name:
        return "macos"
    elif "win" in os_name:
        return "windows"
    return "linux"

def find_python311():
    """Finds python3.11 executable path cross-platform."""
    candidates = []
    
    if get_os_type() == "windows":
        candidates = ["py", "python", "python3.11"]
    else:
        # Check standard PATH first, then explicit locations
        candidates = ["python3.11", "/usr/bin/python3.11", "/usr/local/bin/python3.11", "/opt/homebrew/bin/python3.11"]

    for cmd in candidates:
        path = shutil.which(cmd)
        if path:
            try:
                # Verify it is actually 3.11
                # We use --version to check the actual output
                ver = subprocess.check_output([path, "--version"]).decode().strip()
                if "3.11" in ver:
                    return path
            except:
                continue
    return None

def install_linux_strategy():
    """Installs Python 3.11 on Linux (Ubuntu PPA or Debian Source)."""
    print("\n[Linux] Python 3.11 not found. Determining installation strategy...")
    
    if not shutil.which("apt-get"):
         print("Error: This script requires 'apt-get' (Debian/Ubuntu/Mint/Kali).")
         sys.exit(1)

    # 1. Detect Ubuntu (Use PPA for speed)
    is_ubuntu = False
    try:
        if os.path.exists("/etc/os-release"):
            with open("/etc/os-release") as f:
                if "ubuntu" in f.read().lower():
                    is_ubuntu = True
    except:
        pass

    run_command("sudo apt-get update", shell=True)

    if is_ubuntu:
        print("Detected Ubuntu. Attempting PPA install...")
        try:
            run_command("sudo apt-get install -y software-properties-common", shell=True)
            run_command("sudo add-apt-repository -y ppa:deadsnakes/ppa", shell=True)
            run_command("sudo apt-get update", shell=True)
            run_command("sudo apt-get install -y python3.11 python3.11-venv python3.11-dev", shell=True)
            return
        except Exception as e:
            print(f"Ubuntu PPA failed ({e}). Falling back to source build.")

    # 2. Debian/Fallback Strategy (Source Build)
    print("Detected Debian/Other. Using Source Build (Robust Method)...")
    
    # Install Build Dependencies
    deps = [
        "wget", "build-essential", "zlib1g-dev", "libncurses5-dev", 
        "libgdbm-dev", "libnss3-dev", "libssl-dev", "libreadline-dev", 
        "libffi-dev", "libsqlite3-dev", "libbz2-dev", "pkg-config"
    ]
    run_command(f"sudo apt-get install -y {' '.join(deps)}", shell=True)
    
    # Download & Build
    tarball = f"Python-{PYTHON_VERSION}.tgz"
    url = f"https://www.python.org/ftp/python/{PYTHON_VERSION}/{tarball}"
    
    if not os.path.exists(tarball):
        run_command(f"wget {url}", shell=True)
    
    run_command(f"tar -xf {tarball}", shell=True)
    src_dir = f"Python-{PYTHON_VERSION}"
    
    # Configure & Install
    # --enable-optimizations is standard for production python builds
    run_command("./configure --enable-optimizations", shell=True, cwd=src_dir)
    nproc = subprocess.check_output("nproc", shell=True).decode().strip()
    run_command(f"make -j {nproc}", shell=True, cwd=src_dir)
    
    # CRITICAL: Use altinstall to avoid overwriting /usr/bin/python3
    run_command("sudo make altinstall", shell=True, cwd=src_dir)
    
    # Cleanup
    os.remove(tarball)
    run_command(f"sudo rm -rf {src_dir}", shell=True)

def install_macos_strategy():
    print("\n[macOS] Installing Python 3.11 via Homebrew...")
    if shutil.which("brew") is None:
        print("Error: Homebrew not found. Please install it from brew.sh")
        sys.exit(1)
    run_command("brew install python@3.11", shell=True)

def install_windows_strategy():
    print("\n[Windows] Installing Python 3.11 via Winget/Installer...")
    # Try Winget first (standard on Win 10/11)
    if shutil.which("winget"):
        try:
            run_command("winget install -e --id Python.Python.3.11", shell=True)
            return
        except:
            pass
    
    # Fallback to direct download
    installer = "python-3.11.9-amd64.exe"
    url = f"https://www.python.org/ftp/python/3.11.9/{installer}"
    print(f"Downloading {url}...")
    urllib.request.urlretrieve(url, installer)
    run_command([installer, "/quiet", "InstallAllUsers=1", "PrependPath=1"])
    os.remove(installer)

def setup_project(python_exe):
    print(f"\n[Project] Setting up repo using found Python: {python_exe}")
    
    # 1. Clone
    if not os.path.exists(REPO_DIR):
        run_command(["git", "clone", REPO_URL])
    
    os.chdir(REPO_DIR)
    
    # 2. Checkout Branch
    run_command(["git", "fetch", "origin"])
    run_command(["git", "checkout", "Endee"])
    run_command(["git", "pull", "origin", "Endee"])

    # 3. Create Venv
    if not os.path.exists("venv"):
        print("Creating virtual environment...")
        run_command([python_exe, "-m", "venv", "venv"])
    else:
        print("Virtual environment already exists. Skipping creation.")

    # 4. Install Deps
    # Check OS *again* here to determine correct PIP path
    if get_os_type() == "windows":
        venv_pip = os.path.join("venv", "Scripts", "pip.exe")
    else:
        # MacOS and Linux use this path
        venv_pip = os.path.join("venv", "bin", "pip")

    print(f"Installing dependencies using: {venv_pip}")
    run_command([venv_pip, "install", "--upgrade", "pip"])
    run_command([venv_pip, "install", "endee==0.1.10"])
    run_command([venv_pip, "install", "-e", "."])
    
    return venv_pip

if __name__ == "__main__":
    # 0. Check Git
    if shutil.which("git") is None:
        print("Error: Git is not installed.")
        if get_os_type() == "linux":
            run_command("sudo apt-get update && sudo apt-get install -y git", shell=True)
        else:
            sys.exit(1)

    # 1. Check for Existing Python 3.11
    py_path = find_python311()

    if py_path:
        print("\n" + "="*50)
        print(f"FOUND PYTHON 3.11: {py_path}")
        print("Skipping OS installation steps.")
        print("="*50)
    else:
        # Install if missing
        os_type = get_os_type()
        if os_type == "linux":
            install_linux_strategy()
        elif os_type == "macos":
            install_macos_strategy()
        elif os_type == "windows":
            install_windows_strategy()
        
        # Verify installation
        py_path = find_python311()
        if not py_path:
            print("Error: Installation failed or binary not found.")
            sys.exit(1)

    # 2. Setup Project
    setup_project(py_path)

    print("\n" + "="*50)
    print("SETUP SUCCESSFUL!")
    print("="*50)
    
    if get_os_type() == "windows":
        print(f"To start: {os.path.join(os.getcwd(), 'venv', 'Scripts', 'activate')}")
    else:
        print(f"To start: source {os.path.join(os.getcwd(), 'venv', 'bin', 'activate')}")
    print("="*50)

