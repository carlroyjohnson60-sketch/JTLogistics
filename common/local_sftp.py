import os
import shutil
from typing import List


class LocalSFTP:
    def __init__(self, base_dir=None):
        self.base_dir = base_dir or os.getcwd()

    def list_files(self, path: str) -> List[str]:
        p = os.path.join(self.base_dir, path)
        if not os.path.exists(p):
            return []
        return [os.path.join(p, f) for f in os.listdir(p) if os.path.isfile(os.path.join(p, f))]

    def download(self, remote_path: str, dest_dir: str) -> List[str]:
        """Copy all files from remote_path (local folder) into dest_dir and return list of local files."""
        src = os.path.join(self.base_dir, remote_path)
        if not os.path.exists(src):
            return []
        os.makedirs(dest_dir, exist_ok=True)
        copied = []
        for fname in os.listdir(src):
            fsrc = os.path.join(src, fname)
            if os.path.isfile(fsrc):
                fdst = os.path.join(dest_dir, fname)
                shutil.copy2(fsrc, fdst)
                copied.append(fdst)
        return copied

    def upload(self, local_file: str, remote_dir: str, remote_name: str = None) -> str:
        
        # Normalize remote_dir so leading slashes don't escape base_dir on Windows
        remote_dir_clean = remote_dir.lstrip('/\\') if remote_dir else ''
        dst = os.path.join(self.base_dir, remote_dir_clean)
        os.makedirs(dst, exist_ok=True)
        name = remote_name or os.path.basename(local_file)
        dst_file = os.path.join(dst, name)
        shutil.copy2(local_file, dst_file)
        return dst_file
