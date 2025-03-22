import os
import sys

if len(sys.argv) >= 3:
    directory = sys.argv[1].strip()
    filename_parts = sys.argv[2:]
    filename = " ".join(filename_parts).strip()
    full_path = os.path.join(directory, filename)

    # Optional debug log
    with open("/tmp/cleanup_debug.txt", "a") as log:
        log.write(f"Trying to delete: {full_path}\n")

    if os.path.exists(full_path):
        os.remove(full_path)
        with open("/tmp/cleanup_debug.txt", "a") as log:
            log.write(f"Deleted: {full_path}\n")
    else:
        with open("/tmp/cleanup_debug.txt", "a") as log:
            log.write(f"File not found: {full_path}\n")
else:
    with open("/tmp/cleanup_debug.txt", "a") as log:
        log.write("Incorrect arguments passed to script.\n")
