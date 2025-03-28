# deduplicate-rs

The tool `deduplicate-rs` identifies duplicate media files and generates a bash script to cure the duplication.

## Features

- Identifies duplicate media files across directories.
- Uses SHA256 checksums to ensure accurate matching.
- Maintains a database of file checksums for quicker future runs.
- Generates a non-destructive remediation script.
- Detects both within-directory and cross-directory duplicates.
- Supports automatic filename cleanup by removing numeric suffixes.

## Supported Media Formats

`mp4`, `flv`, `mkv`, `avi`, `mov`, `wmv`, `webm`, `m4v`, `mpg`, `mpeg`, `ts`.

## Installation

```console
cargo build --release
```

The compiled binary will be available in `target/release/deduplicate-rs`.

## Usage

To run from the current directory:

```console
# Run in the current directory
./deduplicate-rs
```

To specify a different directory:

```console
./deduplicate-rs --filepath /path/to/media/directory
```

## How It Works

1. The tool recursively scans the specified directory for media files.
2. Calculates SHA256 checksums for each file (or uses cached values).
3. Identifies duplicates within the same directory and across different directories.
4. Generates a bash script (`potentially-destructive-remove.sh`) containing:
   - Commands to backup files before modification.
   - Commands to remove within-directory duplicates (keeping one copy).
   - Commands to handle cross-directory duplicates (commented out by default).
   - Commands to clean up filenames by removing numeric suffixes.

## Safety Features

- All operations are non-destructive - the tool only generates a script.
- The script creates backups before making any changes.
- Cross-directory duplicates are marked but commented out by default.
- The script must be manually reviewed and executed by the user.
