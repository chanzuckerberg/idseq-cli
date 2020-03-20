import argparse
import re
import requests
import traceback

from . import uploader

from builtins import input
from future.utils import viewitems


def validate_file(path, name):
    pattern = uploader.INPUT_REGEX
    if not re.search(pattern, path):
        print("ERROR: {} ({}) file does not appear to be a fastq or fasta file.".format(
            name, path))
        print("Accepted formats: fastq/fq, fasta/fa, fastq.gz/fq.gz, fasta.gz/fa.gz")
        raise ValueError


def main():
    parser = argparse.ArgumentParser(
        description='Submit a sample to idseq. (Accepts fastq or fasta files, single or paired, gzipped or not.)'
    )

    parser.add_argument(
        '-p',
        '--project',
        metavar='name',
        type=str,
        help='Project name. Make sure the project is created on the website ')
    parser.add_argument(
        '-s',
        '--sample-name',
        metavar='name',
        type=str,
        help='Sample name. It should be unique within a project. Ignored for bulk uploads.')
    parser.add_argument(
        '-m',
        # TODO: (gdingle): accept a URL?
        '--metadata',
        metavar='file',
        type=str,
        help='Metadata local file path.')
    parser.add_argument(
        '-u',
        '--url',
        metavar='url',
        type=str,
        default='https://idseq.net',
        help='IDseq website url: i.e. https://idseq.net by default')
    parser.add_argument(
        '-e',
        '--email',
        metavar='email',
        type=str,
        help='Your login email for idseq website')
    parser.add_argument(
        '-t',
        '--token',
        metavar='token',
        type=str,
        help='Your authentication token')
    parser.add_argument(
        '--r1',
        metavar='file',
        type=str,
        help='Read 1 file path. Could be a local file or s3 path')
    parser.add_argument(
        '--r2',
        metavar='file',
        type=str,
        help='Read 2 file path (optional). Could be a local file or s3 path')
    parser.add_argument(
        '-b',
        '--bulk',
        metavar='file',
        type=str,
        help='Input folder for bulk upload')
    parser.add_argument(
        '--uploadchunksize',
        metavar='value',
        type=int,
        default=uploader.DEFAULT_MAX_PART_SIZE_IN_MB,
        help='Break up uploaded files into chunks of this size in MB')
    parser.add_argument(
        '--accept-all',
        action='store_true',
        help='Use this argument to automatically accept confirmation messages, '
        'including confirmation of geosearch suggestions.')
    parser.add_argument(
        '--skip-geosearch',
        action='store_true',
        help='Use this argument to skip searching for geo-location via third-party API')
    args = parser.parse_args()

    print("Instructions: https://idseq.net/cli_user_instructions\nStarting "
          "IDseq command line...")

    # Prompt the user for missing fields
    if not args.email:
        args.email = required_input("\nEnter your IDseq account email: ")
    if not args.token:
        args.token = required_input("\nEnter your IDseq authentication token:\n("
                                    "see instructions at "
                                    "http://idseq.net/cli_user_instructions): ")
    if not args.project:
        args.project = required_input("\nEnter the project name: ")
    if not args.bulk:
        if not args.sample_name:
            inp = input("{:35}".format("\nEnter the sample name (or press Enter to "
                                       "use bulk mode): "))
            if inp is '':
                args.bulk = "."  # Run bulk auto-detect on the current folder
            else:
                args.sample_name = inp
                if not args.r1:
                    args.r1 = required_input(
                        "\nEnter the first file:\n(first in a paired-end run or "
                        "sole file in a single-end run): ")
                if not args.r2:
                    r2 = input(
                        "\nEnter the second paired-end file if applicable (or "
                        "press Enter to skip): ")
                    if r2 != '':
                        args.r2 = r2

    # Headers for server requests
    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "X-User-Email": args.email,
        "X-User-Token": args.token,
    }

    args.project, args.project_id = uploader.validate_project(
        args.url, headers, args.project)

    print("\n{:20}{}".format("PROJECT:", args.project))

    # Bulk upload
    if args.bulk:
        _bulk_upload(args, headers)
        return

    _single_upload(args, headers)
    return


def required_input(msg):
    resp = input(msg.ljust(35))
    if resp is '':
        raise RuntimeError("Value required!")
    return resp


def upload_sample(sample_name, file_0, file_1, headers, args, csv_metadata):
    try:
        uploader.upload(
            sample_name, args.project_id, headers, args.url, file_0, file_1,
            args.uploadchunksize, csv_metadata
        )
    except requests.exceptions.RequestException as e:
        sample_error_text(sample_name, e)
        network_err_text()
    except Exception as e:
        traceback.print_exc()
        sample_error_text(sample_name, e)


def print_sample_files_info(sample, files):
    print("{:20}{}".format("Sample name:", sample))
    print("{:20}{}".format("Input files:", " ".join(files)))


def sample_error_text(sample, err):
    print("\nFailed to upload \"{}\"".format(sample))
    print("Error: {}".format(err))


def network_err_text():
    print(
        "\nThere was a network error. Please check your network connection "
        "and try again.\nYour sample may say \"Waiting\" on IDseq but likely "
        "needs to be re-uploaded (under a different name).")


def _bulk_upload(args, headers):
    samples2files = uploader.detect_samples(args.bulk)

    if len(samples2files) == 0:
        print("No proper single or paired samples detected")
        return
    print("\nSamples and files to upload:")
    for sample, files in viewitems(samples2files):
        print_sample_files_info(sample, files)
    csv_metadata = uploader.get_user_metadata(
        args.url,
        headers,
        list(samples2files.keys()),
        args.project_id,
        args.metadata,
        args.skip_geosearch,
        args.accept_all,
    )
    if not args.accept_all:
        uploader.get_user_agreement()
    for sample, files in viewitems(samples2files):
        if len(files) < 2:
            files.append(None)
        upload_sample(sample, files[0], files[1],
                      headers, args, csv_metadata[sample])
    return


def _single_upload(args, headers):
    # Single upload
    validate_file(args.r1, 'R1')
    input_files = [args.r1]
    if args.r2:
        validate_file(args.r2, 'R2')
        input_files.append(args.r2)
    print_sample_files_info(args.sample_name, input_files)
    csv_metadata = uploader.get_user_metadata(
        args.url,
        headers,
        [args.sample_name],
        args.project_id,
        args.metadata,
        args.skip_geosearch,
        args.accept_all,
    )
    if not args.accept_all:
        uploader.get_user_agreement()
    upload_sample(args.sample_name, args.r1, args.r2, headers,
                  args, csv_metadata[args.sample_name])
    return


if __name__ == "__main__":
    main()
