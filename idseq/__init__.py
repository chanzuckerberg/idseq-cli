import argparse
import re
import sys
from idseq import uploader


def validate_file(path, name):
    pattern = ".+\.fastq\.gz"
    if not re.search(pattern, path):
        print(
            "ERROR: %s (%s) file does not appear to be a fastq.gz file." %
            (name, path))
        print("ERROR: Basename expected to match regex '%s'" % pattern)
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Submit a sample to idseq. (Only accept paired gzipped fastq files)')

    parser.add_argument(
        '-p',
        '--project',
        metavar='name',
        type=str,
        required=True,
        help='Project name. Make sure the project is created on the website ')
    parser.add_argument(
        '-s',
        '--sample-name',
        metavar='name',
        type=str,
        required=True,
        help='Sample name. It should be unique within a project')
    parser.add_argument('-u', '--url', metavar='url', type=str, required=True,
                        help='idseq website url: i.e. dev.idseq.net')
    parser.add_argument(
        '-e',
        '--email',
        metavar='email',
        type=str,
        required=True,
        help='Your login email for idseq website')
    parser.add_argument(
        '-t',
        '--token',
        metavar='token',
        type=str,
        required=True,
        help='Your authentication token')
    parser.add_argument(
        '--r1',
        metavar='file',
        type=str,
        required=True,
        help='first gziped fastq file path. could be a local file or s3 path')
    parser.add_argument(
        '--r2',
        metavar='file',
        type=str,
        required=True,
        help='second gziped fastq file path. could be a local file or s3 path')
    parser.add_argument(
        '--preload',
        metavar='file',
        type=str,
        help='s3 path for preloading the results for lazy run')

    try:
        args = parser.parse_args()

        validate_file(args.r1, 'R1')
        validate_file(args.r2, 'R2')

        uploader.upload(
            args.sample_name,
            args.project,
            args.email,
            args.token,
            args.url,
            args.r1,
            args.r2,
            args.preload)
    except BaseException:
        parser.print_help()
        sys.exit(1)
