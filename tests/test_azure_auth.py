
from azure.identity import AzureCliCredential

def main():
    credential = AzureCliCredential()

    token = credential.get_token(
        "https://management.azure.com/.default"
    )

    print("Azure authentication successful!")
    print(f"Token expires at: {token.expires_on}")

    credential.close()


if __name__ == "__main__":
    main()
