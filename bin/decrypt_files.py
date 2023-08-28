import argparse
import getpass
import os

from cryptography.fernet import Fernet


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    key = getpass.getpass("Enter your encryption key: ")
    cipher = Fernet(key)
    os.makedirs(args.output_dir, exist_ok=True)

    for name in os.listdir(args.input_dir):
        base, ext = os.path.splitext(name)
        out_name = f"{base}.decrypted{ext}"
        with open(os.path.join(args.input_dir, name), "rb") as infile, open(
            os.path.join(args.output_dir, out_name), "wb"
        ) as outfile:
            outfile.write(cipher.decrypt(infile.read()))


if __name__ == "__main__":
    main()
