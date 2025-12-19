import os
import json
from GNCASNConverter import ASNConverter
from oder_api_client import call_order_api
from config_loader import load_config
from sftp_downloader import SFTPDownloader


def main():
    config = load_config()
    data_dir = config["paths"]["data_dir"]
    output_dir = config["paths"]["output_dir"]
    responses_dir = config["paths"]["response_dir"]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(responses_dir, exist_ok=True)

    SFTPDownloader("gnc").download_files()

    csv_files = [f for f in os.listdir(data_dir) if f.lower().endswith(".csv")]

    if not csv_files:
        print("⚠️ No .csv files found in", data_dir)
        return

    for csv_file in csv_files:
        csv_path = os.path.join(data_dir, csv_file)
        print(f"🔄 Processing: {csv_path}")

        try:
            converter = ASNConverter(csv_path)
            json_data = converter.convert()   # 👈 json_data is a dict here

            api_response = call_order_api(json_data)   # 👈 this should expect a dict

            response_file = os.path.join(
                responses_dir,
                f"{os.path.splitext(csv_file)[0]}_response.json"
            )

            with open(response_file, "w", encoding="utf-8") as f:
                if isinstance(api_response, (dict, list)):
                    json.dump(api_response, f, ensure_ascii=False, indent=2)
                else:
                    f.write(str(api_response))

            print(f"📥 Response saved to: {response_file}")

        except Exception as e:
            print(f"❌ Failed to process {csv_file}: {e}")


if __name__ == "__main__":
    main()
