import argparse
import re
import sys
from idseq import uploader


def validate_file(path, name):
    pattern = ".+\.(fastq|fasta)(\.gz|$)"
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
        help='second gziped fastq file path. could be a local file or s3 path')
    parser.add_argument(
        '--preload',
        metavar='file',
        type=str,
        help='s3 path for preloading the results for lazy run')
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
        '--sampletissue',
        metavar='name',
        type=str,
        help='Tissue sampled')
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
        '--host_id',
        metavar='value',
        type=int,
        help='Host Genome Id')
    parser.add_argument(
        '--job_queue',
        metavar='name',
        type=str,
        help='Job Queue')

    #try:
    if True:
        args = parser.parse_args()

        validate_file(args.r1, 'R1')
        if args.r2:
            validate_file(args.r2, 'R2')

        uploader.upload(
            args.sample_name,
            args.project,
            args.email,
            args.token,
            args.url,
            args.r1,
            args.r2,
            args.preload,
            args.starindex,
            args.bowtie2index,
            args.samplehost,
            args.samplelocation,
            args.sampledate,
            args.sampletissue,
            args.sampletemplate,
            args.samplelibrary,
            args.samplesequencer,
            args.samplenotes,
            args.samplememory,
            args.host_id,
            args.job_queue)
    '''
    except BaseException:
        parser.print_help()
        sys.exit(1)
    '''
