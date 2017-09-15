#!/usr/bin/env python

import io
import json
import os
import sys
import requests
import tqdm

def detect_source_type(file):
    if file.startswith('s3://'):
        return 's3'
    else: #assume local
        stat_result = os.stat(file)
        return 'local'

def upload(sample_name, project_name, email, token, url, r1, r2):

    files = [r1, r2]

    # TODO detect source type - local or s3
    # TODO validate that source types are the same
    # TODO add source type - local or s3 and send as an attribute for intput_files

    # over in idseq-web
        # write job to cp
        # enqueue based on source types (all files in sample 1 job)
        # validate that input file source types are the same
        # maybe test permissions in-band?

    data = {
        "sample": {
            "name": sample_name,
            "project_name": project_name,
            "input_files_attributes": [{"name": os.path.basename(f)} for f in files],
            "status": "created"
        }
    }

    headers = {
        'Accept': 'application/json',
        'Content-type': 'application/json',
        'X-User-Email': email,
        'X-User-Token': token
    }

    resp = requests.post('http://' + url + '/samples.json', data=json.dumps(data), headers=headers)

    if resp.status_code == 201:
        print("successfully created entry")
    else:
        print('failed')
        sys.exit()

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

    resp = requests.put('http://%s/samples/%d.json' % (url, data['id']), data=json.dumps(update), headers=headers)

    if resp.status_code == 200:
        print("success")
    else:
        print("failure")


class Tqio(io.BufferedReader):
    def __init__(self, file_path, i, count):
        super(Tqio, self).__init__(io.open(file_path, "rb"))
        desc = "%s (%d/%d)" % (file_path, i + 1, count)
        self.tqdm = tqdm.tqdm(desc=desc, unit="bytes", unit_scale=True, total=os.path.getsize(file_path))

    def read(self, *args, **kwargs):
        chunk = super(Tqio, self).read(*args, **kwargs)
        self.tqdm.update(len(chunk))
        return chunk
