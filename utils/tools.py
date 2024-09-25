import os
import zlib

def create_folder(*args: str, is_full=False):
    if is_full:
        os.makedirs(*args, exist_ok=True)
    else:
        os.makedirs(os.path.join(*args), exist_ok=True)

def generate_hash_filename(url):
    """Generate a hash of the URL to use as a filename."""
    url_hash = zlib.crc32(url.encode())
    file_name = f"{url_hash:x}"
    return file_name

def get_files(path_dir, ignored=""):
    """Get all files in a directory"""
    all_files = []
    for root, dirs, files in os.walk(path_dir):
        all_files += [os.path.join(root, file) for file in files]

    # ignore files with the ignored string
    if ignored:
        all_files = [file for file in all_files if ignored not in file]
    return all_files