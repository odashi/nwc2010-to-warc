import argparse
import logging
import multiprocessing
import pathlib
import subprocess

logging.basicConfig(level=logging.INFO)


ap = argparse.ArgumentParser()
ap.add_argument("in_dir", type=pathlib.Path, help="Input directory")
ap.add_argument("out_dir", type=pathlib.Path, help="Output directory")
ap.add_argument("num_processes", type=int, help="Number of parallel processes")
args = ap.parse_args()
in_dir: pathlib.Path = args.in_dir
out_dir: pathlib.Path = args.out_dir

in_files = in_dir.glob("*.gz")
out_dir.mkdir(mode=0o755, parents=True, exist_ok=True)


def process(in_file: pathlib.Path) -> None:
    stem = in_file.stem
    out_file = out_dir / (stem + ".warc.gz")
    log_file = out_dir / (stem + ".log")
    command = f"python html_to_warc.py {in_file} {out_file} 2>{log_file}"
    logging.info(command)
    subprocess.run(command, shell=True)
    return out_file


pool = multiprocessing.Pool(args.num_processes)
for _ in pool.imap_unordered(process, in_files):
    pass
