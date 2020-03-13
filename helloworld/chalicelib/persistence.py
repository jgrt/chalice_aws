import json
import pickle
import os
import io
import boto3
import time
from urllib.parse import urlparse
from pathlib import Path


class FilePersistenceBase:
    def __init__(self, assets=None):
        self.assets = assets if assets else json.loads(os.environ["ASSETS"])

        self.codecs = {
            "pickle": {
                "encoder": lambda x: pickle.dumps(x),
                "decoder": lambda x: pickle.loads(x)
            },
            "json": {
                "encoder": lambda x: json.dumps(x).encode(),
                "decoder": lambda x: json.loads(x)
            }
        }

        self.ios = {
            "fs": {
                "read": lambda x: open(x, 'rb').read(),
                "write": lambda x, y: (Path(x).parent.mkdir(parents=True, exist_ok=True), open(x, 'wb').write(y)),
                "keymaker": lambda x, y: f"{self.netloc}/{self.path}{'/'.join(x)}.{y}"
            },
            "s3": {
                "read": lambda x: self.s3.Object(x).get()['Body'].read(),
                "write": lambda x, y: self.s3.Object(x).upload_fileobj(io.BytesIO(y)),
                "keymaker": lambda x, y: f"{self.path}{'/'.join(x)}.{y}"
            }
        }


class FilePersistenceSingle(FilePersistenceBase):
    def __init__(self, uri=None, assets=None):
        uri = uri if uri else os.environ["ASSET_PREFIX"]
        assert uri, f"Invalid URI {uri} for a prefix "
        self.configure(uri)
        super(FilePersistenceSingle, self).__init__(assets)

    def configure(self, uri):
        parsed = urlparse(uri)
        self.scheme = parsed.scheme
        self.netloc = parsed.netloc
        self.path = parsed.path[1:]
        if self.scheme == "s3":
            self.s3 = boto3.resource('s3').Bucket(self.netloc)

    def save(self, data, user, asset, name=None):
        assert asset in self.assets, f"Invalid Asset!! {asset} not in {self.assets}"
        name = name if name else str(time.time())
        keymaker = self.ios[self.scheme]['keymaker']
        writer = self.ios[self.scheme]['write']
        codec = self.assets[asset]['codec']
        encoder = self.codecs[codec]['encoder']
        file_key = keymaker([user, asset, name], codec)
        file_buffer = encoder(data)
        writer(file_key, file_buffer)
        return file_key

    def load(self, user, asset, name=None, file_key=None):
        assert asset in self.assets, f"Invalid Asset!! {asset} not in {self.assets}"
        assert (name or file_key), "Both name and file_key cannot be none"
        keymaker = self.ios[self.scheme]['keymaker']
        codec = self.assets[asset]['codec']
        reader = self.ios[self.scheme]["read"]
        decoder = self.codecs[codec]["decoder"]
        file_key = file_key if file_key else keymaker([user, asset, name], codec)
        file_buffer = reader(file_key)
        data = decoder(file_buffer)
        return data
