import os
import paramiko
from config_loader import load_config

class SFTPDownloader:
    def __init__(self, source: str):
        """
        source: "gnc" or "jt"
        """
        self.config = load_config()
        sftp_cfg = self.config["sftp"][source]

        self.host = sftp_cfg["host"]
        self.port = sftp_cfg.get("port", 22)
        self.username = sftp_cfg["username"]
        self.password = sftp_cfg.get("password")
        self.key_file = sftp_cfg.get("key_file")
        self.remote_dir = sftp_cfg["remote_dir"]
        self.local_dir = self.config["paths"]["data_dir"]

        os.makedirs(self.local_dir, exist_ok=True)

    def download_files(self):
        """Download files from this SFTP remote_dir into local_dir"""
        print(f"🌐 Connecting to SFTP {self.host}:{self.port} ...")

        transport = paramiko.Transport((self.host, self.port))
        if self.key_file:
            private_key = paramiko.RSAKey.from_private_key_file(self.key_file)
            transport.connect(username=self.username, pkey=private_key)
        else:
            transport.connect(username=self.username, password=self.password)

        sftp = paramiko.SFTPClient.from_transport(transport)

        downloaded = []
        try:
            sftp.chdir(self.remote_dir)
            files = sftp.listdir()
            print(f"📂 Found {len(files)} files in {self.remote_dir}")

            for file in files:
                if file.lower().endswith((".csv")):
                    remote_path = f"{self.remote_dir}/{file}" 
                    local_path = os.path.join(self.local_dir, file)
                    print(f"📂 Found {files} files in {self.remote_dir}")
                    print(f"⬇️ Downloading {remote_path} → {local_path}")
                    sftp.get(remote_path, local_path)
                    downloaded.append(local_path)
                    print("✅ Download complete.")
         
        finally:
            sftp.close()
            transport.close()

        return downloaded
""""# 🔽 Allow direct testing
if __name__ == "__main__":
    # Change "gnc" or "jt" here for testing
    downloader = SFTPDownloader("gnc")
    files = downloader.download_files()
    print("📥 Test run complete. Files downloaded:", files)"""
