#!/usr/bin/env python3
"""Run all preprocess_* scripts for a given date folder.

Behavior:
- Determine date tag (YYYYMMDD) from argv or use today.
- For each preprocessor in the ordered list, attempt to import and call a `main(date_tag)` function.
  If import or call fails, fall back to running the corresponding script with subprocess.
- Each preprocessor run is isolated with try/except and writes its own log file `log_preprocess_{date}_{name}.log`.
- A summary xlsx is written at the end listing status and outputs.
"""
import sys
import os
import subprocess
import importlib
import traceback
from datetime import datetime
import logging
import pandas as pd
import re


PREPROCESSORS = [
    ("preprocess_motc", "交通部"),
    ("preprocess_moea", "經濟部能源署"),
    ("preprocess_mof", "財政部"),
    ("preprocess_ntc_index", "國發會"),
    ("preprocess_dgbas", "行政院主計處"),
    ("preprocess_moi", "內政部"),
    ("preprocess_ee520", "經濟部(EE520)"),
    ("preprocess_mol", "勞動部"),
]


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def run_preprocessor(module_name, date_tag, workspace_root):
    """Run a preprocessor by attempting import then fallback to subprocess.

    Returns a dict: {"module": module_name, "status": "success"|"failed", "msg": str, "outputs": [files]}
    """
    result = {"module": module_name, "status": "not-run", "msg": "", "outputs": []}

    # put logs under logs/YYYYMMDD/
    log_root = os.path.join(workspace_root, "logs")
    log_dir = os.path.join(log_root, date_tag)
    ensure_dir(log_dir)
    log_path = os.path.join(log_dir, f"log_preprocess_{date_tag}_{module_name}.log")

    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    # remove previous handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(fh)

    logger.info(f"Start preprocessor {module_name} for date {date_tag}")

    # Try import and call main(date_tag)
    try:
        spec = importlib.import_module(module_name)
        if hasattr(spec, "main"):
            try:
                # attempt to call main with date_tag if it accepts an argument
                import inspect
                sig = None
                try:
                    sig = inspect.signature(spec.main)
                except Exception:
                    sig = None

                if sig is not None and len(sig.parameters) >= 1:
                    spec.main(date_tag)
                else:
                    # call without args
                    spec.main()

                result["status"] = "success"
                result["msg"] = "ran via import.module main()"
            except Exception:
                tb = traceback.format_exc()
                logger.error("Exception running main() via import:\n" + tb)
                result["status"] = "failed"
                result["msg"] = tb
        else:
            logger.info(f"Module {module_name} has no main(), will run script via subprocess")
            raise ImportError("no main()")
    except Exception as e:
        # Fallback: run as script
        try:
            script_path = os.path.join(workspace_root, f"{module_name}.py")
            if not os.path.exists(script_path):
                msg = f"Script {script_path} not found"
                logger.error(msg)
                result["status"] = "failed"
                result["msg"] = msg
            else:
                # run the script with date_tag arg
                proc = subprocess.run([sys.executable, script_path, date_tag], capture_output=True, text=True)
                logger.info("subprocess stdout:\n" + (proc.stdout or ""))
                logger.info("subprocess stderr:\n" + (proc.stderr or ""))
                if proc.returncode == 0:
                    result["status"] = "success"
                    result["msg"] = "ran via subprocess"
                else:
                    result["status"] = "failed"
                    result["msg"] = f"returncode={proc.returncode}\n{proc.stderr}"
        except Exception:
            tb = traceback.format_exc()
            logger.error("Exception running subprocess:\n" + tb)
            result["status"] = "failed"
            result["msg"] = tb

    # After run, try to parse the module log to extract outputs (prefer explicit 'wrote' or 'OUTPUT:' lines)
    outputs = []
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as lf:
            for line in lf:
                line = line.strip()
                # common pattern: wrote /full/path or wrote relative/path
                m = re.search(r"wrote\s+(.+)$", line)
                if m:
                    p = m.group(1).strip()
                    # remove surrounding quotes
                    p = p.strip('"\'')
                    outputs.append(os.path.join(workspace_root, p) if not os.path.isabs(p) and p else p)
                    continue
                # pattern: OUTPUT: /path
                m2 = re.search(r"OUTPUT[:=]\s*(.+)$", line)
                if m2:
                    p = m2.group(1).strip()
                    p = p.strip('"\'')
                    outputs.append(os.path.join(workspace_root, p) if not os.path.isabs(p) and p else p)
    except Exception:
        outputs = []

    # fallback: scan date folder for files matching module hints if log parsing didn't find outputs
    date_folder = os.path.join(workspace_root, date_tag)
    if not outputs and os.path.exists(date_folder):
        for fn in os.listdir(date_folder):
            if module_name.split("preprocess_")[-1] in fn or module_name.replace("preprocess_", "") in fn or "(修正)" in fn:
                outputs.append(os.path.join(date_folder, fn))

    result["outputs"] = sorted(list(set([os.path.normpath(x) for x in outputs if x])))

    logger.info(f"Finish {module_name} status={result['status']}")
    # clean handlers
    logger.removeHandler(fh)
    fh.close()
    return result


def write_summary(results, date_tag, workspace_root):
    rows = []
    for mod, label in PREPROCESSORS:
        r = next((x for x in results if x["module"] == mod), None)
        if r is None:
            rows.append({"module": mod, "label": label, "status": "not-run", "msg": "", "outputs": ""})
        else:
            rows.append({"module": r["module"], "label": label, "status": r.get("status", ""), "msg": r.get("msg", "")[:4000], "outputs": ";".join(r.get("outputs", []))})

    df = pd.DataFrame(rows)
    out_dir = os.path.join(workspace_root, date_tag)
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"preprocess_summary_{date_tag}.xlsx")
    try:
        df.to_excel(out_path, index=False)
    except Exception:
        # last-resort CSV
        df.to_csv(out_path.replace('.xlsx', '.csv'), index=False)
    return out_path


def main(argv):
    workspace_root = os.path.dirname(os.path.abspath(__file__))
    if len(argv) >= 2:
        date_tag = argv[1]
    else:
        date_tag = datetime.now().strftime('%Y%m%d')

    results = []
    for module_name, label in PREPROCESSORS:
        try:
            res = run_preprocessor(module_name, date_tag, workspace_root)
        except Exception as e:
            res = {"module": module_name, "status": "failed", "msg": str(e), "outputs": []}
        results.append(res)

    summary_path = write_summary(results, date_tag, workspace_root)
    print(f"Wrote summary to {summary_path}")


if __name__ == '__main__':
    main(sys.argv)
