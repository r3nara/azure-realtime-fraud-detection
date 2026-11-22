
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

STORAGE_ACCOUNT = "stfrauddev2026"
FILE_SYSTEM = "fraud-data"
DIRECTORY = "raw"
FILE_NAME = "phase1-connectivity-test.txt"


def main():
    credential = AzureCliCredential()

    try:
        account_url = (
            f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
        )

        service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=credential
        )

        filesystem = service_client.get_file_system_client(
            file_system=FILE_SYSTEM
        )

        directory = filesystem.get_directory_client(DIRECTORY)

        file_client = directory.create_file(FILE_NAME)

        file_client.append_data(
            data="Python successfully connected to ADLS Gen2!",
            offset=0,
            length=len("Python successfully connected to ADLS Gen2!")
        )

        file_client.flush_data(
            len("Python successfully connected to ADLS Gen2!")
        )

        print("SUCCESS: File written to ADLS Gen2!")
        print(f"Storage account: {STORAGE_ACCOUNT}")
        print(f"File system: {FILE_SYSTEM}")
        print(f"Directory: {DIRECTORY}")
        print(f"File: {FILE_NAME}")

        # Clean up the temporary connectivity test file
        file_client.delete_file()
        print("Temporary test file deleted successfully.")

        service_client.close()

    finally:
        credential.close()


if __name__ == "__main__":
    main()
