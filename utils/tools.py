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
