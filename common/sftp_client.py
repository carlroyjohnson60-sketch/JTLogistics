# sftp_client.py
import os
import stat
import posixpath
import paramiko
import logging
from typing import List, Tuple, Optional


class SFTPClient:
    """
    Lightweight wrapper around paramiko SSHClient + SFTPClient.
    Provides:
      - connect/close handling
      - download(remote_file_or_dir, dest_dir) which supports either a single file or a directory
      - upload(local_file, remote_dir, remote_name=None)
      - list_files(remote_dir)
      - remove(remote_path)
      - rename(remote_src, remote_dest)
    """

    def __init__(self, cfg: dict):
        self.host = cfg.get('host')
        self.port = int(cfg.get('port', 22))
        self.username = cfg.get('username')
        self.password = cfg.get('password')
        self.key_filename = cfg.get('key_filename')
        self.timeout = int(cfg.get('timeout', 10))
        self.logger = logging.getLogger(__name__)

    def _connect(self) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = dict(
            hostname=self.host,
            port=self.port,
            username=self.username,
            timeout=self.timeout,
        )
        if self.password:
            kwargs["password"] = self.password
        if self.key_filename:
            kwargs["key_filename"] = self.key_filename

        self.logger.debug("Connecting to SFTP %s:%s", self.host, self.port)
        client.connect(**kwargs)
        return client, client.open_sftp()

    def _ensure_remote_dir(self, sftp: paramiko.SFTPClient, remote_dir: str):
        """Create remote directory (and parents) if missing. remote_dir must be posix-style."""
        if not remote_dir or remote_dir == '/':
            return
        parts = remote_dir.strip('/').split('/')
        cur = '/'
        for p in parts:
            cur = posixpath.join(cur, p)
            try:
                sftp.stat(cur)
            except IOError:
                try:
                    sftp.mkdir(cur)
                    self.logger.debug("Created remote directory: %s", cur)
                except Exception as e:
                    # Could be a race / permissions; re-check
                    try:
                        sftp.stat(cur)
                    except Exception:
                        self.logger.error("Failed to create remote dir %s: %s", cur, e)

    def _is_file(self, sftp: paramiko.SFTPClient, remote_path: str) -> bool:
        try:
            st = sftp.stat(remote_path)
            return stat.S_ISREG(st.st_mode)
        except IOError:
            return False

    def _is_dir(self, sftp: paramiko.SFTPClient, remote_path: str) -> bool:
        try:
            st = sftp.stat(remote_path)
            return stat.S_ISDIR(st.st_mode)
        except IOError:
            return False

    def list_files(self, remote_path: str) -> List[str]:
        """
        List regular files in remote_path directory.
        Returns absolute remote posix paths.
        """
        client = None
        sftp = None
        files: List[str] = []
        try:
            client, sftp = self._connect()
            # If remote_path is a file, just return that single file if it exists
            if self._is_file(sftp, remote_path):
                files.append(remote_path)
                return files

            # Otherwise treat as directory
            try:
                items = sftp.listdir(remote_path)
            except IOError as e:
                self.logger.warning("Remote path not accessible %s: %s", remote_path, e)
                return []

            for name in items:
                full = posixpath.join(remote_path, name)
                try:
                    st = sftp.stat(full)
                    if stat.S_ISREG(st.st_mode):
                        files.append(full)
                except Exception:
                    continue
            return files
        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()

    def download(self, remote_path: str, dest_dir: str) -> List[str]:
        """
        Download either:
          - a single remote file (remote_path is a file), or
          - all files inside a remote directory (remote_path is a directory)
        Returns list of local file paths downloaded.
        """
        os.makedirs(dest_dir, exist_ok=True)
        client = None
        sftp = None
        downloaded: List[str] = []

        try:
            client, sftp = self._connect()

            # Check if remote_path is file
            try:
                st = sftp.stat(remote_path)
                if stat.S_ISREG(st.st_mode):
                    # Single file download
                    filename = posixpath.basename(remote_path)
                    local_path = os.path.join(dest_dir, filename)
                    self.logger.info("Downloading remote file %s -> %s", remote_path, local_path)
                    sftp.get(remote_path, local_path)
                    downloaded.append(local_path)
                    return downloaded
            except IOError:
                # If stat fails, maybe path doesn't exist - will attempt listdir below and it will also error
                pass

            # Treat remote_path as directory
            try:
                items = sftp.listdir(remote_path)
            except IOError as e:
                self.logger.warning("Remote directory not accessible %s: %s", remote_path, e)
                return downloaded

            self.logger.debug("Found %d item(s) in remote dir %s", len(items), remote_path)
            for name in items:
                remote_file = posixpath.join(remote_path, name)
                try:
                    st = sftp.stat(remote_file)
                    if stat.S_ISDIR(st.st_mode):
                        self.logger.debug("Skipping remote directory: %s", remote_file)
                        continue
                except Exception:
                    self.logger.debug("Unable to stat remote file %s; skipping", remote_file)
                    continue

                local_file = os.path.join(dest_dir, name)
                try:
                    sftp.get(remote_file, local_file)
                    downloaded.append(local_file)
                    self.logger.info("Downloaded: %s -> %s", remote_file, local_file)
                except Exception as e:
                    self.logger.warning("Failed to download %s: %s", remote_file, e)

            return downloaded

        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()

    def upload(self, local_file: str, remote_dir: str, remote_name: Optional[str] = None) -> str:
        """
        Upload a local file to a remote directory. Creates remote_dir if needed.
        Returns the remote path of the uploaded file.
        """
        client = None
        sftp = None
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"Local file not found: {local_file}")

        try:
            client, sftp = self._connect()
            remote_dir = remote_dir or '/'
            # Ensure remote dir exists
            self._ensure_remote_dir(sftp, remote_dir)
            name = remote_name or os.path.basename(local_file)
            remote_path = posixpath.join(remote_dir, name)
            self.logger.info("Uploading %s -> %s", local_file, remote_path)
            sftp.put(local_file, remote_path)
            return remote_path
        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()

    def remove(self, remote_path: str):
        """Remove a remote file (posix path)."""
        client = None
        sftp = None
        try:
            client, sftp = self._connect()
            try:
                sftp.remove(remote_path)
                self.logger.info("Removed remote file: %s", remote_path)
            except IOError as e:
                self.logger.warning("Failed to remove remote %s: %s", remote_path, e)
                raise
        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()

    def rename(self, remote_src: str, remote_dest: str):
        """Rename/move a remote file (posix path)."""
        client = None
        sftp = None
        try:
            client, sftp = self._connect()
            try:
                self._ensure_remote_dir(sftp, posixpath.dirname(remote_dest))
                sftp.rename(remote_src, remote_dest)
                self.logger.info("Renamed remote %s -> %s", remote_src, remote_dest)
            except Exception as e:
                self.logger.error("Failed to rename remote %s -> %s: %s", remote_src, remote_dest, e)
                raise
        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()
