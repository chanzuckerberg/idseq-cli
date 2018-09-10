import argparse
import re

import requests
from builtins import input
from future.utils import viewitems

from idseq import uploader
from idseq.uploader import DEFAULT_MAX_PART_SIZE_IN_MB


def validate_file(path, name):
    pattern = uploader.INPUT_REGEX
    if not re.search(pattern, path):
        print(
            "ERROR: {} ({}) file does not appear to be a fastq or fasta file.".format(name, path))
        print(
            "Accepted formats: fastq/fq, fasta/fa, fastq.gz/fq.gz, fasta.gz/fa.gz"
        )
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
        help='Sample name. It should be unique within a project')
    parser.add_argument(
        '-u',
        '--url',
        metavar='url',
        type=str,
        default='https://idseq.net',
        help='idseq website url: i.e. https://idseq.net by default')
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
        '--host-genome-name',
        metavar='name',
        type=str,
        help='Host Genome Name')
    parser.add_argument(
        '--r1',
        metavar='file',
        type=str,
        help='read 1 file path. could be a local file or s3 path')
    parser.add_argument(
        '--r2',
        metavar='file',
        type=str,
        help='read 2 file path (optional). could be a local file or s3 path')
    parser.add_argument(
        '--preload',
        metavar='file',
        type=str,
        help='s3 path for preloading the results for lazy run')
    parser.add_argument(
        '-b',
        '--bulk',
        metavar='file',
        type=str,
        help='Input folder for bulk upload')
    parser.add_argument(
        '--starindex',
        metavar='file',
        type=str,
        help='s3 path for STAR index (tar.gz)')
    parser.add_argument(
        '--bowtie2index',
        metavar='file',
        type=str,
        help='s3 path for bowtie2 index (tar.gz)')
    parser.add_argument(
        '--samplehost',
        metavar='name',
        type=str,
        help='Name of the host the sample was taken from')
    parser.add_argument(
        '--samplelocation',
        metavar='name',
        type=str,
        help='Location of sample collection')
    parser.add_argument(
        '--sampledate',
        metavar='date',
        type=str,
        help='Date of sample collection')
    parser.add_argument(
        '--sampletissue', metavar='name', type=str, help='Tissue sampled')
    parser.add_argument(
        '--sampletemplate',
        metavar='name',
        type=str,
        help='Nucleic acid type of assay template (DNA/RNA)')
    parser.add_argument(
        '--samplelibrary',
        metavar='name',
        type=str,
        help='Library preparation method')
    parser.add_argument(
        '--samplesequencer',
        metavar='name',
        type=str,
        help='Sequencing instrument')
    parser.add_argument(
        '--samplenotes',
        metavar='name',
        type=str,
        help='Any additional notes about the sample')
    parser.add_argument(
        '--samplememory',
        metavar='value',
        type=int,
        help='Memory requirement in MB')
    parser.add_argument(
        '--host-id', metavar='value', type=int, help='Host Genome Id')
    parser.add_argument(
        '--job-queue', metavar='name', type=str, help='Job Queue')
    parser.add_argument(
        '--uploadchunksize',
        metavar='value',
        type=int,
        default=DEFAULT_MAX_PART_SIZE_IN_MB,
        help='Break up uploaded files into chunks of this size in MB')
    parser.add_argument(
        '--accept-all',
        action='store_true',
        help='Use this argument to automatically accept confirmation messages')
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
    while args.host_genome_name not in [
            "Human", "Mosquito", "Tick", "Mouse", "ERCC only"
    ]:
        # TODO:
        # (1) deduplicate list of host genomes
        # (2) consider getting this list through a request to idseq-web?
        args.host_genome_name = required_input(
            "\nEnter the host genome name (to be filtered out):\nOptions: "
            "'Human', 'Mosquito', 'Tick', 'Mouse', or 'ERCC only': ").strip("'")

    print("\n{:20}{}".format("PROJECT:", args.project))
    print("{:20}{}".format("HOST GENOME:", args.host_genome_name))

    # Bulk upload
    if args.bulk:
        samples2files = uploader.detect_samples(args.bulk)

        print("\nSamples and files to upload:")
        for sample, files in viewitems(samples2files):
            print_sample_files_info(sample, files)
        if not args.accept_all:
            uploader.get_user_agreement()
        for sample, files in viewitems(samples2files):
            if len(files) < 2:
                files.append(None)
            upload_sample(sample, files[0], files[1], args)
        return

    # Single upload
    validate_file(args.r1, 'R1')
    input_files = [args.r1]
    if args.r2:
        validate_file(args.r2, 'R2')
        input_files.append(args.r2)
    print_sample_files_info(args.sample_name, input_files)
    if not args.accept_all:
        uploader.get_user_agreement()
    upload_sample(args.sample_name, args.r1, args.r2, args)


def required_input(msg):
    resp = input(msg.ljust(35))
    if resp is '':
        raise RuntimeError("Value required!")
    return resp


def upload_sample(sample_name, file_0, file_1, args):
    try:
        uploader.upload(
            sample_name, args.project, args.email, args.token, args.url,
            file_0, file_1, args.preload, args.starindex, args.bowtie2index,
            args.samplehost, args.samplelocation, args.sampledate,
            args.sampletissue, args.sampletemplate, args.samplelibrary,
            args.samplesequencer, args.samplenotes, args.samplememory,
            args.host_id, args.host_genome_name, args.job_queue,
            args.uploadchunksize)
    except requests.exceptions.RequestException as e:
        sample_error_text(sample_name, e)
        network_err_text()
    except Exception as e:
        sample_error_text(sample_name, e)


def print_sample_files_info(sample, files):
    print("\n{:20}{}".format("SAMPLE NAME:", sample))
    print("{:20}{}".format("INPUT FILES:", " ".join(files)))


def sample_error_text(sample, err):
    print("\nFailed to upload \"{}\"".format(sample))
    print("Error: {}".format(err))


def network_err_text():
    print(
        "\nThere was a network error. Please check your network connection "
        "and try again.\nYour sample may say \"Waiting\" on IDseq but likely "
        "needs to be re-uploaded (under a different name).")
