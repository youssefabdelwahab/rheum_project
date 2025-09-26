#!/usr/bin/env python3
import os, sys, glob, shutil, subprocess, textwrap, pathlib, time
import torch
import torch.utils.cpp_extension as ce
from pathlib import Path


# ---- Tunables (override with env/CLI if you want) ----
ARCHS = os.environ.get("TORCH_CUDA_ARCH_LIST", "9.0")          # H100 = SM90
EXT_DIR = os.environ.get("TORCH_EXTENSIONS_DIR",
                         "/work/robust_ai_lab/torch_extensions_cache")
MAX_JOBS = os.environ.get("MAX_JOBS", "20")                      # keep RAM usage low
VERBOSE = True

def log(msg): print(f"[build] {msg}", flush=True)

def ensure_dirs():
    pathlib.Path(EXT_DIR).mkdir(parents=True, exist_ok=True)

def show_env():
    log(f"Python: {sys.version.split()[0]}")
    try:
        import torch
        log(f"Torch: {torch.__version__}  CUDA available: {torch.cuda.is_available()}")
    except Exception as e:
        log(f"Could not import torch: {e}")
    # CUDA toolkit / nvcc
    try:
        out = subprocess.check_output(["nvcc", "--version"], text=True)
        log("nvcc: " + out.splitlines()[-1])
    except Exception as e:
        log(f"nvcc not found in PATH: {e}")

def setup_env():
    os.environ["TORCH_CUDA_ARCH_LIST"]   = ARCHS
    os.environ["MAX_JOBS"]               = str(MAX_JOBS)
    os.environ["TORCH_EXTENSIONS_DIR"]   = EXT_DIR
    os.environ.setdefault("TORCH_CUDA_VERBOSE_BUILD", "1")
    # Some sites need this to avoid tmpfs; route temp to $EXT_DIR
    os.environ.setdefault("TMPDIR", EXT_DIR)

# def get_build_dir():
#     from torch.utils import cpp_extension as ce
#     # Name used by ExLlamaV2
#     name = "exllamav2_ext"
#     bd = ce.get_build_directory(name)
#     log(f"Extension build dir: {bd}")
#     return name, bd

def get_build_dir(name: str):
    # public API: where Torch stores compiled extensions
    if hasattr(ce, "get_default_build_root"):
        root = ce.get_default_build_root()
    else:
        root = str(Path.home() / ".cache" / "torch_extensions")  # fallback

    py_tag = f"py{sys.version_info.major}{sys.version_info.minor}"
    cu_tag = f"cu{torch.version.cuda.replace('.', '')}" if torch.version.cuda else "cpu"

    build_dir = os.path.join(root, f"{py_tag}_{cu_tag}", name)
    return name, build_dir

def try_import_exllama():
    log("Importing exllamav2 to trigger JIT build…")
    import importlib
    # Importing the package triggers build of exllamav2_ext
    importlib.invalidate_caches()
    import exllamav2  # noqa: F401
    log("Import returned.")

def find_so(build_dir):
    sos = glob.glob(os.path.join(build_dir, "exllamav2_ext*.so"))
    return sos

def ninja_build(build_dir):
    if shutil.which("ninja") is None:
        raise RuntimeError("ninja is not in PATH. Install it (pip install --user ninja) or load the module.")
    log("Running ninja -v to finish the link (this prints the real error if it fails)…")
    subprocess.run(["ninja", "-C", build_dir, "-v"], check=True)

def tail_file(p, n=200):
    try:
        with open(p, "r", errors="ignore") as f:
            lines = f.readlines()[-n:]
        log(f"Last {len(lines)} lines of {p}:\n" + "".join(lines))
    except Exception:
        pass

def main():
    setup_env()
    ensure_dirs()
    show_env()

    # Make sure ninja is present (optional auto-install)
    try:
        import ninja  # noqa: F401
    except Exception:
        log("ninja not found; attempting user install…")
        subprocess.run([sys.executable, "-m", "pip", "install", "ninja"], check=False)

    name, build_dir = get_build_dir("exllamav2_ext")
    print("Using build dir:", build_dir)


    # Clean half-built leftovers if any (optional; comment out if you want to reuse)
    if os.path.isdir(build_dir) and not find_so(build_dir):
        log("Cleaning previous partial build…")
        shutil.rmtree(build_dir, ignore_errors=True)

    # Trigger the JIT build by importing
    t0 = time.time()
    try:
        try_import_exllama()
    except Exception as e:
        log(f"Import raised: {e}")

    sos = find_so(build_dir)
    if not sos:
        # Try a manual ninja pass so you can see the failing link command
        try:
            ninja_build(build_dir)
        except subprocess.CalledProcessError as e:
            log(f"ninja failed with return code {e.returncode}")
        sos = find_so(build_dir)

    # Diagnostics if still missing
    if not sos:
        log("Shared library not found after build attempt.")
        # show a quick directory listing & any logs/ninja files
        try:
            ls = subprocess.check_output(["/bin/ls", "-lah", build_dir], text=True)
            log("Build dir contents:\n" + ls)
        except Exception:
            pass
        for candidate in ["build.log", ".ninja_log", "build.ninja"]:
            fp = os.path.join(build_dir, candidate)
            if os.path.exists(fp):
                tail_file(fp, 200)
        # Fail the job so Slurm marks it as FAILED
        sys.exit(1)

    dt = time.time() - t0
    log(f"SUCCESS: {sos[0]}")
    log(f"Build finished in {dt:.1f}s")
    return 0

if __name__ == "__main__":
    sys.exit(main())
