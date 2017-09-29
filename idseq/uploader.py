#!/usr/bin/env python

import io
import json
import os
import sys
import requests
import stat
import tqdm


class File():
    def __init__(self, path):
        self.path = path

    def source_type(self):
        if self.path.startswith('s3://'):
            return 's3'
        elif stat.S_ISREG(os.stat(self.path).st_mode):
            return 'local'


def upload(
        sample_name,
        project_name,
        email,
        token,
        url,
        r1,
        r2,
        preload_s3_path,
        starindex_s3_path,
        bowtie2index_s3_path,
        sample_host,
        sample_location,
        sample_date,
        sample_tissue,
        sample_template,
        sample_library,
        sample_sequencer,
        sample_notes):

    files = [File(r1), File(r2)]

    if files[0].source_type() != files[1].source_type():
        print("ERROR: input files must be same type")
        raise

    source_type = files[0].source_type()
    data = {
        "sample": {
            "name": sample_name,
            "project_name": project_name,
            "input_files_attributes": [
                {
                    "name": os.path.basename(f.path),
                    "source": f.path,
                    "source_type": f.source_type()
                } for f in files
            ],
            "status": "created"
        }
    }
    if preload_s3_path:
        data["sample"]["s3_preload_result_path"] = preload_s3_path
    if starindex_s3_path:
        data["sample"]["s3_star_index_path"] = starindex_s3_path
    if bowtie2index_s3_path:
        data["sample"]["s3_bowtie2_index_path"] = bowtie2index_s3_path
    if sample_host:
        data["sample"]["sample_host"] = sample_host
    if sample_location:
        data["sample"]["sample_location"] = sample_location
    if sample_date:
        data["sample"]["sample_date"] = sample_date
    if sample_tissue:
        data["sample"]["sample_tissue"] = sample_tissue
    if sample_template:
        data["sample"]["sample_template"] = sample_template
    if sample_library:
        data["sample"]["sample_library"] = sample_library
    if sample_sequencer:
        data["sample"]["sample_sequencer"] = sample_sequencer
    if sample_notes:
        data["sample"]["sample_notes"] = sample_notes

    headers = {
        'Accept': 'application/json',
        'Content-type': 'application/json',
        'X-User-Email': email,
        'X-User-Token': token
    }

    resp = requests.post(
        'http://' + url + '/samples.json',
        data=json.dumps(data),
        headers=headers)

    if resp.status_code == 201:
        print("successfully created entry")
    else:
        print('failed %s' % resp.status_code)
        print(resp.json())
        raise

    if source_type == 'local':
        data = resp.json()

        l = len(data['input_files'])

        print("uploading %d files" % l)

        for i, file in enumerate(data['input_files']):
            with Tqio(file['name'], i, l) as f:
                requests.put(file['presigned_url'], data=f)

        update = {
            "sample": {
                "id": data['id'],
                "name": sample_name,
                "status": "uploaded"
            }
        }

        resp = requests.put(
            'http://%s/samples/%d.json' %
            (url, data['id']), data=json.dumps(update), headers=headers)

        if resp.status_code == 200:
            print("success")
        else:
            print("failure")


class Tqio(io.BufferedReader):
    def __init__(self, file_path, i, count):
        super(Tqio, self).__init__(io.open(file_path, "rb"))
        desc = "%s (%d/%d)" % (file_path, i + 1, count)
        self.tqdm = tqdm.tqdm(
            desc=desc,
            unit="bytes",
            unit_scale=True,
            total=os.path.getsize(file_path))

    def read(self, *args, **kwargs):
        chunk = super(Tqio, self).read(*args, **kwargs)
        self.tqdm.update(len(chunk))
        return chunk
