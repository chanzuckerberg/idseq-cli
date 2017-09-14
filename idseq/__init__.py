import argparse
import re
import sys
from idseq import uploader

def validate_file(path, name):
    pattern = ".+\.fastq\.gz"
    if not re.search(pattern, path):
        print("ERROR: %s (%s) file does not appear to be a fastq.gz file." % (name, path))
        print("ERROR: Basename expected to match regex '%s'" % pattern)
        sys.exit(-1)


def main():
    parser = argparse.ArgumentParser(description='Upload to idseq.')

    parser.add_argument('-t', '--token', metavar='token', type=str, required=True)
    parser.add_argument('-p', '--project', metavar='name', type=str, required=True)
    parser.add_argument('-s', '--sample-name', metavar='name', type=str, required=True)
    parser.add_argument('-u', '--url', metavar='url', type=str, required=True)
    parser.add_argument('-e', '--email', metavar='email', type=str, required=True)
    parser.add_argument('--r1', metavar='file', type=str, required=True)
    parser.add_argument('--r2', metavar='file', type=str, required=True)

    args = parser.parse_args()

    validate_file(args.r1, 'R1')
    validate_file(args.r2, 'R2')

    uploader.upload(args.sample_name, args.project, args.email, args.token, args.url, args.r1, args.r2)
