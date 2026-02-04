"""
Media Renamer
-------------

A tool for organizing large photo and video libraries by generating consistent,
chronological filenames based on the most reliable "date taken" metadata.
It extracts timestamps from EXIF, FFmpeg headers, and OS data, then builds a
unified index to determine correct ordering.

The system detects naming conflicts, groups simultaneous captures, applies
deterministic suffix rules, and performs safe, logged renaming. Processing
runs folder by folder for reproducible results even with incomplete metadata.

Core features:
- Extracts dates from EXIF, FFmpeg, and OS metadata
- Produces stable chronological filenames
- Resolves naming conflicts and mutual groups
- Builds a global metadata index
- Performs safe renaming with structured logging

Designed for clarity, reproducibility, and robustness in real-world libraries.
"""

import os
import ffmpeg
import exifread
import subprocess
import pandas as pd
import lib.lib_logger as lib_logger
from typing import Optional
from dataclasses import asdict, dataclass
from pathlib import Path
from datetime import datetime, timezone
from itertools import product
from pillow_heif import register_heif_opener


# Constants
APP_NAME = Path(__file__).stem
NAME_FORMAT = "%Y%m%d-%H%M%S"
DT_FORMAT = "%Y:%m:%d %H:%M:%S"
ISO_FORMAT = "%Y-%m-%d %H:%M:%S"
YEARS = range(2018, 2019)
BASE = r"X:\_Media"
CMD_DIR = "dir /B /o:d"
SLASH = "\\"
NAME_LENGTH = 15
LINE_LENGTH = 50
YES = True
NO = False

FILE_EXTENSIONS = ["jpg", "heic", "mov", "mp4", "mpg", "gif", "m4a"]
EXIF_TAGS = ["Image DateTime", "EXIF DateTimeOriginal", "EXIF DateTimeDigitized"]
FFMPEG_TAGS = ["com.apple.quicktime.creationdate", "creation_time"]

_COL_METADATA = [
    "no",
    "folder",
    "first_name",
    "actual_name",
    "actual_full_name",
    "actual_full_path",
    "date_taken",
    "new_name",
    "new_full_name",
    "new_full_path",
    "ext",
    "is_mutual",
    "mutual_order",
    "mutual_suffix",
    "has_conflict",
    "conflict_suffix",
]


@dataclass
class FileMetadata:
    """
    Holds all metadata used during the media renaming process.

    Stores original file info, extracted date values, generated names,
    mutual‑group ordering, and conflict flags. The renaming pipeline
    updates this object step by step to produce deterministic and
    consistent output filenames.
    """

    no: int = 0
    folder: str | None = None
    first_name: str | None = None
    actual_name: str | None = None
    actual_full_name: str | None = None
    actual_full_path: str | None = None
    date_taken: str | None = None
    new_name: str | None = None
    new_full_name: str | None = None
    new_full_path: str | None = None
    ext: str | None = None
    is_mutual: bool = NO
    mutual_order: int = 0
    mutual_suffix: str = ""
    has_conflict: bool = NO
    conflict_suffix: str = ""


def set_file_metadata(
    file_meta: FileMetadata,
    folder: str | None = None,
    first_name: str | None = None,
    actual_name: str | None = None,
    date_taken: str | None = None,
    new_name: str | None = None,
    ext: str | None = None,
    is_mutual: bool | None = None,
    mutual_order: int = 0,
    has_conflict: bool | None = None,
    conflict_suffix: str | None = None,
    no: int = 0,
):
    """
    Update FileMetadata fields selectively based on provided arguments.

    This function acts as a controlled mutator:
    - Only updates fields when a new value is provided.
    - Rebuilds full names and paths after updates.
    - Handles mutual-group suffixes and conflict suffixes.
    """

    # Update basic fields only when provided
    file_meta.no = no or file_meta.no
    file_meta.folder = folder or file_meta.folder
    file_meta.first_name = first_name or file_meta.first_name
    file_meta.actual_name = actual_name or file_meta.actual_name
    file_meta.date_taken = date_taken or file_meta.date_taken
    file_meta.new_name = new_name or file_meta.new_name
    file_meta.ext = ext or file_meta.ext

    # Mutual group flags
    if is_mutual is not None:
        file_meta.is_mutual = is_mutual
    if mutual_order > 0:
        file_meta.mutual_order = mutual_order
        file_meta.mutual_suffix = f"-{mutual_order:02d}"

    # Conflict flags
    if has_conflict is not None:
        file_meta.has_conflict = has_conflict
    if conflict_suffix is not None:
        file_meta.conflict_suffix = conflict_suffix

    # Rebuild full names and paths
    set_actual_name(file_meta)
    set_new_name(file_meta)


def set_actual_name(meta: FileMetadata):
    """
    Build the actual_full_name and actual_full_path fields
    based on the current actual_name and extension.
    """
    # If no actual name, nothing to build
    if not meta.actual_name:
        return

    # Build full file name and full path for the actual/original file
    meta.actual_full_name = concat_full_name(meta.actual_name, meta.ext)
    meta.actual_full_path = concat_full_path(meta.folder, meta.actual_full_name)  # type: ignore


def set_new_name(meta: FileMetadata):
    """
    Build the new file name and full path using:
    - date_taken (preferred)
    - new_name (fallback)
    - mutual and conflict suffixes
    """

    # If no date and no new name, nothing to build
    if not meta.date_taken and not meta.new_name:
        return

    # Prefer EXIF/OS date; fallback to manually assigned new_name
    meta.new_name = meta.date_taken or meta.new_name
    # Combine mutual + conflict suffixes
    suffix = f"{meta.mutual_suffix}{meta.conflict_suffix}"
    meta.new_name = f"{meta.new_name}{suffix}"
    # Build full new name and path
    meta.new_full_name = concat_full_name(meta.new_name, meta.ext)
    meta.new_full_path = concat_full_path(meta.folder, meta.new_full_name)  # type: ignore


def concat_full_name(file_name: str, ext: str | None = None):
    """Return 'file_name.ext' if extension exists, otherwise return file_name."""
    # Build full file name
    full_name = f"{file_name}.{ext}" if ext else file_name
    return full_name


def concat_full_path(
    folder: str,
    file_name: str,
    ext: str | None = None,
):
    """Return full file path by combining folder and full file name."""
    # Build full file path
    full_name = concat_full_name(file_name, ext)  # type: ignore
    full_path = SLASH.join([folder, full_name])
    return full_path


def date_to_str(date_obj: datetime):
    """Convert datetime object to formatted string, or return None."""
    # If no date object, return None
    date_taken = date_obj.strftime(NAME_FORMAT) if date_obj else None
    return date_taken


def get_min(value1, value2):
    """Return the minimum of two datetime values, handling None values."""
    min_value = min(value1, value2) if value1 and value2 else value1 or value2
    return min_value


def get_utc_time(_time: datetime):
    """Convert a naive datetime to UTC timezone-aware datetime."""
    # If no time provided, return None
    if not _time:
        return None
    r_time = _time.replace(tzinfo=timezone.utc)
    return r_time


def get_os_date(full_path: str):
    """Return the earliest of creation or modification time as UTC datetime."""
    # Get file system timestamps
    c_time = datetime.fromtimestamp(os.path.getctime(full_path))
    c_time = get_utc_time(c_time)
    m_time = datetime.fromtimestamp(os.path.getmtime(full_path))
    m_time = get_utc_time(m_time)

    # OS date cannot be empty.
    date_taken = get_min(c_time, m_time)
    return date_taken


from pathlib import Path
from datetime import datetime
from typing import Optional


def get_date_taken(full_path: str) -> Optional[datetime]:
    """
    Extract the earliest valid datetime value from EXIF metadata using exifread.
    Checked tags (in order of priority):
        - DateTimeOriginal
        - DateTimeDigitized
        - DateTime

    Returns the earliest UTC-normalized datetime found, or None.
    """
    date_taken: Optional[datetime] = None

    # Read EXIF data from the image file
    with open(Path(full_path), "rb") as image:
        exif = exifread.process_file(image, details=False)

    # Check known EXIF date tags
    for tag in EXIF_TAGS:
        raw = exif.get(tag)
        if not raw:
            continue

        # Parse and normalize the datetime
        try:
            dt = datetime.strptime(str(raw), DT_FORMAT)
            dt = get_utc_time(dt)
            date_taken = get_min(date_taken, dt)
        except Exception:
            ...
    return date_taken


def get_ffmpeg_time(full_path: str) -> Optional[datetime]:
    """Extract date taken from media metadata using ffmpeg."""
    path = Path(full_path)
    try:
        probe = ffmpeg.probe(path)
    except Exception as _:
        return None

    # Extract relevant tags from ffmpeg metadata
    format_tags = probe.get("format", {}).get("tags", {})
    date_ffmpeg = None

    # Check known ffmpeg date tags
    for tag in FFMPEG_TAGS:
        dt = format_tags.get(tag)
        if dt:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            dt = get_utc_time(dt)
            date_ffmpeg = get_min(date_ffmpeg, dt)
    return date_ffmpeg


def find_date_taken(meta: FileMetadata) -> Optional[datetime]:
    """Determine the date taken for a file using EXIF, FFmpeg, and OS timestamps."""
    full_path = meta.actual_full_path
    ext = meta.ext
    date_taken = None

    # Try EXIF or FFmpeg depending on file type
    if ext in ["jpg", "heic"]:
        date_taken = get_date_taken(full_path)  # type: ignore
    elif ext in ["mov", "mp4", "mpg", "gif"]:
        date_taken = get_ffmpeg_time(full_path)  # type: ignore

    # Always fallback to OS timestamps
    date_os = get_os_date(full_path)  # type: ignore
    return get_min(date_taken, date_os)


def os_rename(meta: FileMetadata):
    """Perform the actual filesystem rename operation."""
    os.rename(Path(meta.actual_full_path), Path(meta.new_full_path))  # type: ignore


def rename_file(meta: FileMetadata, only_conflicts: bool = NO):
    """Rename a file on the filesystem, optionally only if it has conflicts."""
    # Skip if only conflicts should be processed
    if only_conflicts and not meta.has_conflict:
        return

    # Safety check
    if not meta.new_full_path:
        raise Exception(f"{meta.no} - Error: File path cannot be none!")

    # No rename needed
    same_name = meta.new_full_name == meta.actual_full_name
    if same_name:
        logger.info(f"{meta.no}- {meta.actual_full_name}: File names are identical")
    else:
        os_rename(meta)
        logger.info(
            f"{meta.no}- {meta.actual_full_name} >> {meta.new_full_name}: Completed"
        )
    # Update metadata after rename
    set_file_metadata(meta, actual_name=meta.new_name)


def create_df_global_media(list_metadata: list[FileMetadata]):
    """Build the global DataFrame from the list of FileMetadata objects."""
    global df_global_media

    # Convert metadata objects into a DataFrame using their dict representation
    df_global_media = pd.DataFrame(
        [asdict(md) for md in list_metadata],
        columns=_COL_METADATA,
    )
    # Use 'no' as the index for stable ordering
    df_global_media.set_index("no", drop=True, inplace=True)
    # Store the original metadata objects for direct access
    df_global_media["metadata"] = list_metadata


def search_mutual_names(list_metadata: list[FileMetadata], only_conflicts: bool = NO):
    """Mark files with the same date_taken as mutual."""
    global df_global_media

    # If only conflict processing is requested, skip mutual-name detection
    if only_conflicts:
        return

    # Collect all unique dates from the global media dataframe
    unique_dates = df_global_media["date_taken"].unique().tolist()

    # Iterate over each unique date and file extension combination
    for date_value, ext in product(unique_dates, FILE_EXTENSIONS):
        # Find all files matching the current date and extension
        df_mutual = df_global_media.query(
            f'date_taken == "{date_value}" and ext == "{ext}"'
        )
        # If only one file exists for that date, nothing to mark as mutual
        if df_mutual.index.size <= 1:
            continue

        # Mark all files with the same date as mutual
        idx = 0
        for meta in df_mutual.metadata:
            idx += 1
            set_file_metadata(
                meta,
                is_mutual=YES,
                mutual_order=idx,
            )
    # Rebuild the global dataframe after updates
    create_df_global_media(list_metadata)


def reset_conflicts(list_metadata: list[FileMetadata]):
    """Reset conflict flags for all items that currently have a conflict."""
    for meta in list_metadata:
        if not meta.has_conflict:
            continue
        # Clear conflict state and remove suffix
        set_file_metadata(
            meta,
            has_conflict=NO,
            conflict_suffix="",
        )


def conflict_exists() -> bool:
    """Check if any file has a conflict flag set in the global DataFrame."""
    global df_global_media

    # Check if there is at least one conflicting file
    has_conflict = not df_global_media.query("has_conflict == True").empty
    return has_conflict


def check_conflicts(list_metadata: list[FileMetadata], only_conflicts: bool = NO):
    """Check if the current file has a naming conflict with others."""
    # Skip conflict detection when only conflict processing is requested
    if only_conflicts:
        return NO

    # Check each file for naming conflicts
    for meta in list_metadata:
        # Find any other file with the same new name but a different ID
        df_conflict = df_global_media.query(
            f'actual_full_name == "{meta.new_full_name}" and no != {meta.no}'
        )
        # No conflict found
        if df_conflict.empty:
            continue
        # Mark current file as conflicting
        set_file_metadata(
            meta,
            has_conflict=YES,
            conflict_suffix=str(meta.no),
        )
        # Log the first conflicting file for clarity
        logger.info(
            f"{meta.no}- {df_conflict.iloc[0].actual_full_name}: Conflict detected"
        )


def initialize_conflicts(list_metadata: list[FileMetadata]):
    """Initialize conflict processing if any conflicts exist."""
    # If no conflicts exist, skip the entire process
    if not conflict_exists():
        return

    # Visual separator for logging
    logger.info("*" * LINE_LENGTH)
    # Centered header for conflict processing
    logger.info(str.center("CONFLICTS RUNNING", LINE_LENGTH, " "))

    # Clear previous conflict flags before reprocessing
    reset_conflicts(list_metadata)


def run_renamer(list_metadata: list[FileMetadata], only_conflicts: bool = NO):
    """Rename files, handling mutual names and conflicts."""
    global dict_file_counts

    # Visual separator for renaming phase
    logger.info("*" * LINE_LENGTH)

    # Check for conflicts and mark files accordingly
    for meta in list_metadata:
        # Print header for the first file of each extension loop
        if meta.no == 1:
            logger.info(str.center(f" {meta.ext.upper()} ", LINE_LENGTH, "="))  # type: ignore
            file_number = dict_file_counts.get(meta.ext, 0)  # type: ignore
            logger.info(f"{file_number} files being renamed... ")
            logger.info("-" * LINE_LENGTH)

        # Attempt to rename the file, logging any errors
        try:
            logger.info("-")
            rename_file(meta, only_conflicts)
        except Exception as _:
            logger.error(
                f"{meta.no}- {meta.actual_full_name} - {meta.new_full_name}: "
                + "Error: An error occurred while renaming file."
            )


def find_file_counts(
    list_metadata: list[FileMetadata], only_conflicts: bool = NO
) -> dict[str, int]:
    """Return the global dictionary of file counts per extension."""
    global dict_file_counts
    # Count files per extension
    dict_file_counts = {}
    for ext in FILE_EXTENSIONS:
        if only_conflicts:
            count = sum(
                1 for meta in list_metadata if meta.ext == ext and meta.has_conflict
            )
        else:
            count = sum(1 for meta in list_metadata if meta.ext == ext)
        dict_file_counts[ext] = count
    return dict_file_counts


def process_files(list_metadata: list[FileMetadata], only_conflicts: bool = NO):
    """Process files for metadata extraction and renaming, optionally only handling conflicts."""
    # If only conflict processing is requested but no conflicts exist, skip
    if only_conflicts and not conflict_exists():
        return

    # Check for conflicts and count files
    global dict_file_counts
    check_conflicts(list_metadata, only_conflicts)
    find_file_counts(list_metadata, only_conflicts)

    # Process each file for metadata extraction
    for meta in list_metadata:
        # Print header for the first file of each extension loop
        if meta.no == 1:
            logger.info(str.center(f" {meta.ext.upper()} ", LINE_LENGTH, "="))  # type: ignore
            file_number = dict_file_counts.get(meta.ext, 0)  # type: ignore
            logger.info(f"{file_number} files being processed... ")
            logger.info("-" * LINE_LENGTH)

        # Log processing info when relevant
        if not only_conflicts or meta.has_conflict:
            logger.info(f"{meta.no}- {meta.actual_full_name}")

        # Extract date_taken only in full processing mode
        date_taken = None
        if not only_conflicts:
            date_taken = date_to_str(find_date_taken(meta))  # type: ignore
        # Update metadata with extracted date
        set_file_metadata(meta, date_taken=date_taken)  # type: ignore

    # Rebuild global dataframe only in full processing mode
    if not only_conflicts:
        create_df_global_media(list_metadata)

    # Identify mutual names before renaming
    search_mutual_names(list_metadata, only_conflicts)

    # Continue with renaming phase
    run_renamer(list_metadata, only_conflicts)


def fetch_list_files(base_folder: str) -> list[FileMetadata]:
    """Recursively fetch files from base_folder and build their metadata."""
    list_metadata = []

    for root, _, _ in os.walk(base_folder):
        for ext in FILE_EXTENSIONS:
            cmd_dir = f'{CMD_DIR} "{root}{SLASH}*.{ext}"'

            try:
                # Execute directory listing command
                result = subprocess.check_output(
                    cmd_dir,
                    shell=True,
                    text=True,
                    stderr=subprocess.DEVNULL,
                )

                # Build metadata objects for each file found
                for i, file in enumerate(result.splitlines()):
                    file_meta = FileMetadata()

                    name, extension = file.split(".", 1)
                    extension = extension.lower()

                    set_file_metadata(
                        file_meta,
                        folder=root,
                        first_name=name,
                        actual_name=name,
                        ext=extension,
                        no=i + 1,
                    )
                    list_metadata.append(file_meta)
            except subprocess.CalledProcessError:
                # Ignore folders with no matching files
                ...

        # Rebuild global dataframe after fetching all files
        create_df_global_media(list_metadata)
    return list_metadata


# Global variables
df_global_media = pd.DataFrame()
dict_file_counts: dict[str, int] = {}
dict_conflict_counts: dict[str, int] = {}

# Initialize logger
logger = lib_logger.setup_logging(APP_NAME)
logger.info("\n" + "=" * LINE_LENGTH)
logger.info(
    str.center(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - MEDIA RENAMER STARTED",
        LINE_LENGTH,
        " ",
    )
)

# Enable HEIC support
register_heif_opener()

# Process each year folder
# for yyyy in YEARS:
#     logger.info("\n" + str.center(f" {yyyy} ", LINE_LENGTH, "*"))

#     dir_year = f"{BASE}{SLASH}{yyyy}"
#     list_metadata = fetch_list_files(dir_year)

#     # Extract metadata and rename files
#     process_files(list_metadata)

#     # Resolve any conflicts found
#     start_for_conflicts(list_metadata)
#     process_files(list_metadata, only_conflicts=YES)

# Test block (optional)
dir_year = "C:\\Users\\aykan\\Desktop"
list_metadata = fetch_list_files(dir_year)
process_files(list_metadata)

# Resolve any conflicts found
initialize_conflicts(list_metadata)
process_files(list_metadata, only_conflicts=YES)

# Final log
logger.info("=" * LINE_LENGTH)
logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - MEDIA RENAMER FINISHED")
logger.info("=" * LINE_LENGTH)
