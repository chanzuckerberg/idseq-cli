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
            "input_files_attributes": [{"name": f} for f in files],
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
