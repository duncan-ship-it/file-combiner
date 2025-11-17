import argparse
import shutil
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def move_file(src: Path, dest_dir: Path):
    """
    Move a file to dest_dir, renaming duplicates automatically.
    """
    dest = dest_dir / src.name

    # Handle duplicates
    if dest.exists():
        stem = dest.stem
        suffix = dest.suffix
        i = 1
        while True:
            new_dest = dest_dir / f"{stem} ({i}){suffix}"
            if not new_dest.exists():
                dest = new_dest
                break
            i += 1

    shutil.move(str(src), str(dest))
    return dest

def gather_files(root: Path):
    files = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            files.append(Path(dirpath) / f)
    return files


def parse_args():
    parser = argparse.ArgumentParser(description="Move files from source to destination with duplicates handled.")
    parser.add_argument("source", type=Path, help="Source directory")
    parser.add_argument("dest", type=Path, help="Destination directory")
    parser.add_argument("-t", "--threads", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--batch", type=int, default=2000, help="Number of files to process per batch")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without moving files")
    return parser.parse_args()




def main(source: Path, dest: Path, max_workers=8, batch_size=2000, dry_run=False):
    if not source.exists():
        print(f"Source path does not exist: {source}")
        return
    dest.mkdir(parents=True, exist_ok=True)

    # Gather all files first
    all_files = gather_files(source)
    print(f"Found {len(all_files)} files to move.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, f in enumerate(all_files, 1):
            if dry_run:
                print(f"Would move: {f} -> {dest}")
                continue
            futures.append(executor.submit(move_file, f, dest))

            # Batch submission to limit memory usage
            if len(futures) >= batch_size:
                for fut in as_completed(futures):
                    fut.result()  # wait for batch to complete
                futures = []

        # Final batch
        for fut in as_completed(futures):
            fut.result()

    print("Done moving files.")


if __name__ == "__main__":
    args = parse_args()
    main(
        source=args.source,
        dest=args.dest,
        max_workers=args.threads,
        batch_size=args.batch,
        dry_run=args.dry_run
    )
