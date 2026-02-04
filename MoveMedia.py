import os
from pathlib import Path
import shutil

SOURCE = r"X:\_Media"
DEST = r"D:\WrongYear"
FILE_EXTENSIONS = ["jpg", "heic", "gif", "mov", "mpg", "mp4", "m4a"]


def is_valid_media_file(file_name: str) -> bool:
    """Return True if the file extension is one of the supported media types."""
    return file_name.lower().split(".")[-1] in FILE_EXTENSIONS


def is_wrong_place(file_name, source_path: Path) -> bool:
    parent = source_path.parent.name
    return file_name[:4] != parent[:4]


def scan_media_folder(base_folder: str, dest_folder: str):
    for source_folder, _, files in os.walk(base_folder):
        print(f"Scanning: {source_folder}")

        for file in files:
            source_path = Path(source_folder) / file
            dest_path = Path(dest_folder) / file

            if not is_valid_media_file(file):
                continue
            if is_wrong_place(file, source_path):
                shutil.move(source_path, dest_path)


def main():
    """Main execution flow for scanning and reporting."""
    scan_media_folder(SOURCE, DEST)


if __name__ == "__main__":
    main()
