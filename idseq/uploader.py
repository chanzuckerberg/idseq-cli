import csv
import glob
import io
import json
import os
import pkg_resources
import re
import requests
import stat
import subprocess
import sys
import time

from builtins import input
from future.utils import viewitems
from itertools import product
from string import ascii_lowercase

from . import constants
from . import locations

sys.tracebacklimit = 0

DEFAULT_MAX_PART_SIZE_IN_MB = 5000
INPUT_REGEX = "(.+)\.(fastq|fq|fasta|fa)(\.gz|$)"
PAIRED_REGEX = "(.+)(_R\d)(_001)?\.(fastq|fq|fasta|fa)(\.gz|$)"
PART_SUFFIX = "__AWS-MULTI-PART-"


class File():
    def __init__(self, path):
        self.path = path

    def source_type(self):
        if self.path.startswith('s3://'):
            return 's3'
        elif stat.S_ISREG(os.stat(self.path).st_mode):
            return 'local'

    def parts(self, max_part_size):
        # Check if any file is over max_part_size and, if so, chunk
        if self.source_type() == 'local' and os.path.getsize(
                self.path) > max_part_size:
            part_prefix = self.path + PART_SUFFIX
            return self.split_file(max_part_size, part_prefix)
        else:
            return [self.path]

    def split_file(self, max_part_size, prefix):
        # Using MB (10^6) instead of MiB (2^16)
        print("Splitting large file into {} MB chunks...".format(int(max_part_size // 1E6)))
        if not os.path.isfile(self.path):
            print("Sample file not found: {}".format(self.path))
            return []

        partial_files = []
        suffix_iter = product(ascii_lowercase, repeat=2)
        try:
            with open(self.path, 'rb') as fread:
                for chunk in iter(lambda: fread.read(max_part_size), b''):
                    partial_files.append("{}{}".format(prefix, ''.join(next(suffix_iter))))
                    with open(partial_files[-1], 'bw') as fwrite:
                        fwrite.write(chunk)
            return partial_files
        except StopIteration:
            print("[ERROR] File too large")
            remove_files(partial_files)


def build_path(bucket, key):
    return "s3://{}/{}".format(bucket, key)


def determine_level(file_path, search_key):
    n_parts_file = len(file_path.split("/"))
    n_parts_key = len(search_key.rstrip("/").split("/"))
    return n_parts_file - n_parts_key


def detect_files(path, level=1):
    # S3 source (user needs access to the location they're trying to upload from):
    if path.startswith('s3://'):
        clean_path = path.rstrip('/')
        bucket = path.split("/")[2]
        file_list = subprocess.check_output(
            "aws s3 ls {}/ --recursive | awk '{{print $4}}'".format(clean_path),
            shell=True).splitlines()
        file_list = [f.decode("UTF-8") for f in file_list]
        return [
            build_path(bucket, f)
            for f in file_list
            if re.search(INPUT_REGEX, f) and determine_level(build_path(bucket, f), clean_path) == level
        ]
    # local source:
    wildcards = "/*" * level
    return [
        f for f in glob.glob(path + wildcards)
        if re.search(INPUT_REGEX, f) and os.stat(f).st_size > 0
    ]


def clean_samples2files(samples2files):
    # Sort files (R1 before R2) and remove samples that don't have 1 or 2 files:
    return {
        k: sorted(v)
        for k, v in viewitems(samples2files) if len(v) in [1, 2]
    }

def remove_files(files):
    for partial_file in files:
        if PART_SUFFIX in partial_file:
            os.remove(files)


def detect_samples(path):
    samples2files = {}
    # First try to find top-level files in the folder.
    # Paired files for the same sample must be labeled with R1 and R2 as indicated in PAIRED_REGEX
    files_level1 = detect_files(path, level=1)
    if files_level1:
        for f in files_level1:
            m2 = re.search(PAIRED_REGEX, f)
            m = re.search(INPUT_REGEX, f)
            sample_name = os.path.basename(
                m2.group(1)) if m2 else os.path.basename(m.group(1))
            samples2files[sample_name] = samples2files.get(sample_name, []) + [f]
        return clean_samples2files(samples2files)
    # If there are no top-level files, try to find them in subfolders.
    # In this case, each subfolder corresponds to one sample.
    files_level2 = detect_files(path, level=2)
    if files_level2:
        for f in files_level2:
            sample_name = os.path.basename(os.path.dirname(f))
            samples2files[sample_name] = samples2files.get(sample_name, []) + [f]
        return clean_samples2files(samples2files)
    # If there are still no suitable files, tell the user hopw folders must be structured.
    print(
        "\n\nNo fastq/fasta files found in this folder.\n"
        "Files can have extensions fastq/fq/fasta/fa "
        "with optionally the additional extension gz.\n"
        "If the folder you specified has no sub-directories, "
        "paired files need to be indicated using the labels _R1 and _R2 before the "
        "extension, otherwise each file will be treated as a separate sample. Sample names "
        "will be derived from file names with the extensions and any R1/R2 labels trimmed off.\n"
        "Alternatively, your folder can be structured to have one subfolder per sample. "
        "In that case, the name of the subfolder will be used as the sample name.\n"
        "Example names: RR004_water_2_S23_R1_001.fastq.gz and RR004_water_2_S23_R2_001.fastq.gz"
    )
    raise ValueError()


def upload(sample_name, project_id, headers, url, r1, r2, chunk_size, csv_metadata):
    print("\nPreparing to upload sample \"{}\" ...".format(sample_name))

    files = [File(r1)]
    if r2:
        files.append(File(r2))

    source_type = files[0].source_type()

    # Raise exception if a file is empty
    if source_type == 'local' and any(
            os.stat(f.path).st_size == 0 for f in files):
        print("ERROR: input file must not be empty")
        raise ValueError()

    if r2 and files[0].source_type() != files[1].source_type():
        print("ERROR: input files must be same type")
        raise ValueError()

    # Clamp max_part_size to a valid value
    max_part_size = int(max(min(DEFAULT_MAX_PART_SIZE_IN_MB, chunk_size), 1) * 1E6)

    # Get version of CLI from setuptools
    version = pkg_resources.require("idseq")[0].version

    host_genome_name = pop_match_in_dict(constants.HOST_GENOME_ALIASES, csv_metadata)
    if not host_genome_name:
        print("ERROR: no host organism in CSV")
        raise ValueError()

    all_file_parts = [f.parts(max_part_size) for f in files]
    data = {
        "samples": [
            {
                "name": sample_name,
                "project_id": project_id,
                "input_files_attributes": [
                    {
                        "name": os.path.basename(f.path),
                        "source": f.path if f.source_type() == 's3' else os.path.basename(f.path),
                        "source_type": f.source_type(),
                        "parts": ", ".join([os.path.basename(f) for f in file_parts]),
                    }
                    for f, file_parts in zip(files, all_file_parts)
                ],
                "host_genome_name": host_genome_name,
                "status": "created"
            }
        ],
        "metadata": {sample_name: csv_metadata},
        "client": version
    }

    raw_resp = requests.post(
        url + '/samples/bulk_upload_with_metadata.json', data=json.dumps(data), headers=headers)
    resp = raw_resp.json()

    if raw_resp.status_code == 200:
        if len(resp.get("errors", {})) == 0:
            print("Connected to the server.")
        else:
            print("\nFailed. Error response from IDseq server: {}".format(resp["errors"]))
            remove_files(all_file_parts)
            return
    else:
        # Handle potential responses without proper error fields
        print("\nFailed. Error response: {}".format(resp))
        remove_files(all_file_parts)
        return

    if source_type == 'local':
        sample_data = resp["samples"][0]
        num_files = len(sample_data["input_files"])
        if num_files == 1:
            msg = "1 file to upload..."
        else:
            msg = "{} files to upload...".format(num_files)
        print(msg)
        time.sleep(1)

        for raw_input_file in sample_data['input_files']:
            presigned_urls = raw_input_file['presigned_url'].split(", ")
            input_parts = raw_input_file["parts"].split(", ")
            for part_index, file in enumerate(input_parts):
                presigned_url = presigned_urls[part_index]
                print('Uploading {} (part {} of {})...'.format(
                    file, part_index, len(input_parts)
                ))
                with Tqio(file, part_index, num_files) as f:
                    resp_put = requests.put(presigned_url, data=f)
                    if resp_put.status_code != 200:
                        print('Sample was not successfully uploaded. Status code: {}, '
                              'Input file: {}, Sample name: {}'.format(str(resp_put.status_code),
                                                                       str(file),
                                                                       str(sample_name)))
                        remove_files(all_file_parts)
                        return
                if PART_SUFFIX in file:
                    os.remove(file)

        # Mark as uploaded
        sample_id = resp["sample_ids"][0]
        update = {
            "sample": {
                "id": sample_id,
                "name": sample_name,
                "status": "uploaded"
            }
        }

        resp = requests.put(
            '{}/samples/{}.json'.format(url, sample_id),
            data=json.dumps(update),
            headers=headers)

        has_file_parts = any(len(parts) > 1 for parts in all_file_parts)
        if resp.status_code == 504 and has_file_parts:
            # Note: Not ideal, but for now idseq-web times out trying to concatenate file parts on the server
            print('Sample is being processed on our server. Check for status on IDseq https://idseq.net')
            remove_files(all_file_parts)
            return
        elif resp.status_code != 200:
            print('Sample was not successfully uploaded. Status code: {}, '
                  'Sample name: {}'.format(str(resp.status_code), str(sample_name)))
            remove_files(all_file_parts)
            return

    print("All done!")


def get_user_agreement():
    def prompt(msg):
        resp = input(msg)
        if resp.lower() not in ["y", "yes"]:
            print("Exiting...")
            quit()

    msg = "\nConfirm details above.\nProceed (y/N)? y for yes or N to cancel: "
    prompt(msg)
    msg = "\nI agree that the data I am uploading to IDseq has been lawfully " \
          "collected and that I have all the necessary consents, permissions, " \
          "and authorizations needed to collect, share, and export data to " \
          "IDseq as outlined in the Terms (https://idseq.net/terms) and Data " \
          "Privacy Notice (https://idseq.net/privacy).\nProceed (y/N)? y for " \
          "yes or N to cancel: "
    prompt(msg)


def print_metadata_instructions():
    print(
        "\nInstructions: https://idseq.net/metadata/instructions"
        "\nMetadata dictionary: https://idseq.net/metadata/dictionary"
        "\nMetadata CSV template: https://idseq.net/metadata/metadata_template_csv"
    )


def get_user_metadata(base_url, headers, sample_names, project_id, metadata_file=None):
    instructions_printed = False

    if not metadata_file:
        print("\nPlease provide some metadata for your sample(s):")
        print_metadata_instructions()
        instructions_printed = True
        metadata_file = input("\nEnter the metadata file: ")
    else:
        print("{:20}{}".format("Metadata file:", metadata_file))

    # Loop for metadata CSV validation
    errors = [-1]
    while len(errors) != 0:
        try:
            try:
                with io.open(metadata_file, 'r', encoding='utf-8') as f:
                    csv_data = list(csv.reader(f))
            # If a Unicode error is thrown, it's possible that the user has generated
            # a CSV from Excel that uses latin-1 encoding. Try alternate encoding.
            except UnicodeDecodeError:
                with io.open(metadata_file, 'r', encoding='latin-1') as f:
                    csv_data = list(csv.reader(f))

            # Format data for the validation endpoint
            data = {
                "metadata": {"headers": csv_data[0], "rows": csv_data[1:]},
                "samples": [
                    {"name": name, "project_id": project_id} for name in sample_names
                ],
            }
            resp = requests.post(
                base_url + "/metadata/validate_csv_for_new_samples.json",
                data=json.dumps(data),
                headers=headers,
            )
            errors = display_metadata_errors(resp)
        except (OSError, ValueError, requests.exceptions.RequestException) as err:
            errors = [str(err)]
            print(errors)

        if len(errors) != 0:
            print("\n====================")
            if not instructions_printed:
                print_metadata_instructions()
                instructions_printed = True
            resp = input("\nPlease fix the errors and press Enter to upload again. Or enter a different file name: ")
            metadata_file = resp or metadata_file
        else:
            print("\nCSV validation successful!")

            # Send metadata as { sample_name => {metadata_key: value} }
            csv_data = {}
            with open(metadata_file) as file_data:
                for row in list(csv.DictReader(file_data)):
                    name = pop_match_in_dict(["sample_name", "Sample Name"], row)
                    csv_data[name] = row
            csv_data = locations.geosearch_and_set_csv_locations(base_url, headers, csv_data, project_id)
            return csv_data


# Display issues with the submitted metadata CSV based on the server response
def display_metadata_errors(resp):
    resp = json.loads(resp.text)
    issues = resp.get("issues", {})

    # Show a section for errors and warnings
    for issue_type in ["errors", "warnings"]:
        group = issues.get(issue_type, {})
        if len(group) != 0:
            print("\n===== {} =====".format(issue_type.capitalize()))
            for issue in group:
                # TODO: Backend may return str or issue groups. Can consolidate.
                if type(issue) == str:  # Won't work for unicode
                    print(issue)
                else:
                    print()
                    issue.pop("isGroup", None)  # Ignore field
                    for msg in issue.values():
                        print(msg)
    return issues.get("errors", {})


def validate_project(base_url, headers, project_name):
    print("Checking project name...")
    resp = requests.get(base_url + "/projects.json", headers=headers)
    if resp.status_code == 401:
        print("Invalid email or token. Please double-check your formatting and try again.")
        quit()
    all_projects = resp.json()
    names_to_ids = {}

    for project in all_projects["projects"]:
        names_to_ids[project["name"]] = project["id"]

    while project_name not in names_to_ids:
        user_resp = input("\nProject does not exist. Press Enter to create. Or check a different "
                          "project name: ")
        if user_resp:
            project_name = user_resp
        else:
            # Create the project
            resp = requests.post(
                base_url + "/projects.json",
                data=json.dumps({"project": {"name": project_name}}),
                headers=headers
            )
            if resp.status_code == 422:
                print("Project name is too similar to an existing project. Please try another name.")
                continue
            resp = resp.json()
            print("Project created!")
            return resp["name"], resp["id"]

    return project_name, names_to_ids[project_name]


def pop_match_in_dict(keys, dictionary):
    for k in keys:
        if k in dictionary:
            return dictionary.pop(k)


class Tqio(io.BufferedReader):
    def __init__(self, file_path, i, count):
        super(Tqio, self).__init__(io.open(file_path, "rb"))
        self.progress = 0
        self.chunk_idx = 0
        self.total = os.path.getsize(file_path)
        self.done = False

    def write_stdout(self, msg):
        sys.stdout.write(msg)
        sys.stdout.flush()

    def write_percent_stdout(self, percentage):
        self.write_stdout("{:3.1f} % \r".format(percentage))

    def update(self, len_chunk):
        self.progress += len_chunk
        self.chunk_idx += 1
        if self.chunk_idx % 500 == 0:
            # don't slow the upload process down too much
            self.write_percent_stdout((100.0 * self.progress) / self.total)
        if self.progress >= self.total and not self.done:
            self.write_percent_stdout(100.0)
            self.write_stdout("\nDone.\n")
            self.done = True

    def read(self, *args, **kwargs):
        chunk = super(Tqio, self).read(*args, **kwargs)
        self.update(len(chunk))
        return chunk
