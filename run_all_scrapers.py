import os
import json
from datetime import datetime
import traceback

# list of scraper modules and a friendly name
SCRAPERS = [
    ("motc_scraper", "交通部"),
    ("moea_scraper", "經濟部能源署"),
    ("mof_scraper", "財政部"),
    ("ndc_index_scraper", "國發會"),
    ("dgbas_scraper", "行政院主計處"),
    ("moi_scraper", "內政部"),
    ("ee520_export_orders_scraper", "經濟部(EE520)"),
    ("mol_average_hours_scraper", "勞動部-平均工時"),
    ("mol_reduce_hours_scraper", "勞動部-減少工時"),
    ("mol_unemployment_scraper", "勞動部-失業率"),
]

# modules that should run in headful mode regardless of the global flag
HEADFUL_OVERRIDES = {
    'mof_scraper': True,
    'ndc_index_scraper': True,
}

def ensure_logs_dir():
    d = os.path.join(os.getcwd(), 'logs')
    os.makedirs(d, exist_ok=True)
    return d


def run_all(output_dir='.', keep_browser_open=False, dry_run=False):
    """Run all scrapers in SCRAPERS. dry_run=True will only import modules and report availability.
    Logs a summary JSON to ./logs/YYYYMMDD_runlog.json"""
    date_tag = datetime.now().strftime('%Y%m%d')
    log_dir = ensure_logs_dir()
    log_path = os.path.join(log_dir, f'{date_tag}_runlog.json')

    summary = {
        'date': date_tag,
        'results': []
    }

    for mod_name, friendly in SCRAPERS:
        entry = {'module': mod_name, 'friendly': friendly, 'status': 'not-started', 'error': None, 'elapsed_seconds': None, 'files': []}
        try:
            mod = __import__(mod_name)
            entry['status'] = 'imported'
            if dry_run:
                entry['status'] = 'skipped-dry-run'
            else:
                # call run if available
                if hasattr(mod, 'run'):
                    try:
                        # prepare output folder snapshot
                        date_out = date_tag
                        out_folder = os.path.join(output_dir, date_out) if output_dir else os.path.join(os.getcwd(), date_out)
                        try:
                            before = set(os.listdir(out_folder))
                        except Exception:
                            before = set()

                        import time as _time
                        start = _time.time()
                        # allow per-module override for headful mode
                        module_keep_open = HEADFUL_OVERRIDES.get(mod_name, keep_browser_open)
                        mod.run(output_dir=output_dir, keep_browser_open=module_keep_open)
                        end = _time.time()
                        entry['elapsed_seconds'] = round(end - start, 2)

                        try:
                            after = set(os.listdir(out_folder))
                        except Exception:
                            after = set()

                        new_files = sorted(list(after - before))
                        entry['files'] = new_files
                        entry['status'] = 'success'
                    except Exception as e:
                        entry['status'] = 'failed'
                        entry['error'] = str(e)
                        entry['traceback'] = traceback.format_exc()
                else:
                    entry['status'] = 'no-run-function'
        except Exception as e:
            entry['status'] = 'import-failed'
            entry['error'] = str(e)
            entry['traceback'] = traceback.format_exc()

        summary['results'].append(entry)

        # write incremental log after each module to ensure partial results saved
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', '-o', default='.')
    parser.add_argument('--headful', action='store_true', help='Keep browsers open (headful)')
    parser.add_argument('--dry-run', action='store_true', help='Only import modules, do not execute network calls')
    args = parser.parse_args()

    res = run_all(output_dir=args.output, keep_browser_open=args.headful, dry_run=args.dry_run)
    print('Run summary written to logs folder')
