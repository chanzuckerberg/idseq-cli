#!/usr/bin/env python

import io
import json
import os
import sys
import requests
import tqdm

def upload(sample_name, project_id, files, email, token, url):

    data = {
        "sample": {
            "name": sample_name,
            "project_id": project_id,
            "input_files_attributes": [{"name": f} for f in files]
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

    for file in data['input_files']:
        with Tqio(file['name']) as f:
            requests.put(file['presigned_url'], data=f)


class Tqio(io.BufferedReader):
    def __init__(self, file_path):
        super(Tqio, self).__init__(io.open(file_path, "rb"))
        self.tqdm = tqdm.tqdm(desc=file_path, unit="bytes", unit_scale=True, total=os.path.getsize(file_path))

    def read(self, *args, **kwargs):
        chunk = super(Tqio, self).read(*args, **kwargs)
        self.tqdm.update(len(chunk))
        return chunk
