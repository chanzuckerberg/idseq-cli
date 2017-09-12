import argparse
from idseq import uploader

def main():
    parser = argparse.ArgumentParser(description='Upload to idseq.')

    parser.add_argument('-t', '--token', metavar='token', type=str, required=True)
    parser.add_argument('-p', '--project', metavar='name', type=str, required=True)
    parser.add_argument('-s', '--sample-name', metavar='name', type=str, required=True)
    parser.add_argument('-u', '--url', metavar='url', type=str, required=True)
    parser.add_argument('-e', '--email', metavar='email', type=str, required=True)
    parser.add_argument('files', metavar='file', type=str, nargs='+', help='files to upload')

    args = parser.parse_args()
    uploader.upload(args.sample_name, args.project, args.files, args.email, args.token, args.url)
