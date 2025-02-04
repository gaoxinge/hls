import io
import os
from abc import ABC, abstractmethod
from urllib.parse import urljoin
from typing import Callable, IO, List
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from Crypto.Cipher import AES
from tqdm import tqdm


Get = Callable[[str, IO], None]


class BS:

    def __init__(self, get: Get):
        self.get = get

    def get_bytes(self, url: str) -> bytes:
        with io.BytesIO() as f:
            self.get(url, f)
            return f.getvalue()

    def get_str(self, url: str) -> str:
        return self.get_bytes(url).decode("utf-8")


class Decoder(ABC):

    @abstractmethod
    def decode(self, content: bytes):
        raise NotImplementedError


class DefaultDecoder(Decoder):

    def decode(self, content: bytes):
        return content


class AESDecoder(Decoder):

    def __init__(self, key: bytes, iv: bytes):
        self.aes = AES.new(key=key, IV=iv, mode=AES.MODE_CBC)

    def decode(self, content: bytes):
        return self.aes.decrypt(content)


class Cache:

    def __init__(self, directory: str, get1: Get):
        self.directory = directory
        self.get1 = get1

    def get(self, url: str):
        _, url_suffix = url.rsplit("/", 1)
        file_path = os.path.join(self.directory, url_suffix)
        if os.path.exists(file_path):
            return
        with open(file_path, "wb") as f:
            self.get1(url, f)

    def merge(self, url_list: List[str], file_path: str, decoder: Decoder):
        with open(file_path, "wb") as f:
            for url in url_list:
                _, url_suffix = url.rsplit("/", 1)
                file_path1 = os.path.join(self.directory, url_suffix)
                with open(file_path1, "rb") as g:
                    f.write(decoder.decode(g.read()))


class HLS:

    EXT_X_KEY = "#EXT-X-KEY:"
    EXT_X_KEY_METHOD = "METHOD"
    EXT_X_KEY_URI = "URI"
    EXT_X_KEY_IV = "IV"

    def __init__(self):
        self.ext_x_key = None
        self.ext_x_key_enc_key = None
        self.m3u8_url_list = None
        self.ts_url_list = None

    @staticmethod
    def parse_ext_x_key(line: str):
        line = line.split(HLS.EXT_X_KEY)[1]
        result = {}
        for line in line.split(","):
            k, v = line.split("=")
            result[k] = v
        return result

    @staticmethod
    def parse(url: str, get: Get) -> 'HLS':
        bs = BS(get)
        url_prefix, _ = url.rsplit("/", 1)
        url_prefix += "/"

        hls = HLS()
        m3u8_url_list = []
        ts_url_list = []

        result = bs.get_str(url)
        for line in result.split("\n"):
            if not line.startswith("#"):
                if line.endswith("m3u8"):
                    m3u8_url_list.append(urljoin(url_prefix, line))
                if line.endswith("ts"):
                    ts_url_list.append(urljoin(url_prefix, line))
            else:
                if line.startswith(HLS.EXT_X_KEY):
                    hls.ext_x_key = HLS.parse_ext_x_key(line)
                    if HLS.EXT_X_KEY_URI in hls.ext_x_key:
                        hls.ext_x_key_enc_key = bs.get_bytes(
                            urljoin(url_prefix, hls.ext_x_key[HLS.EXT_X_KEY_URI].replace("\"", ""))
                        )
                    if HLS.EXT_X_KEY_IV in hls.ext_x_key:
                        hls.ext_x_key[HLS.EXT_X_KEY_IV] = bytes.fromhex(hls.ext_x_key[HLS.EXT_X_KEY_IV][2:])

        for m3u8_url in m3u8_url_list:
            result = bs.get_str(m3u8_url)
            for line in result.split("\n"):
                if not line.startswith("#"):
                    if line.endswith("ts"):
                        ts_url_list.append(urljoin(url_prefix, line))

        hls.m3u8_url_list = m3u8_url_list
        hls.ts_url_list = ts_url_list
        return hls

    def get_decoder(self) -> 'Decoder':
        decoder = DefaultDecoder()
        if self.ext_x_key is not None and HLS.EXT_X_KEY_METHOD in self.ext_x_key:
            method = self.ext_x_key[HLS.EXT_X_KEY_METHOD]
            if method == "AES-128":
                decoder = AESDecoder(key=self.ext_x_key_enc_key, iv=self.ext_x_key[HLS.EXT_X_KEY_IV])
        return decoder

    def __str__(self):
        return f"HLS(" \
               f"ext_x_key={self.ext_x_key}, " \
               f"ext_x_key_enc_key={self.ext_x_key_enc_key}, " \
               f"m3u8_url_list={self.m3u8_url_list}, " \
               f"ts_url_list={self.ts_url_list})"

    __repr__ = __str__


class MTDownloader:

    def __init__(self, url: str, directory: str, file_path: str):
        self.session = requests.Session()

        # def getter(u: str, f: IO):
        #     response = self.session.get(u, stream=True)
        #     if response.status_code != 200:
        #         raise Exception(f"get {u} with status code {response.status_code}")
        #     for chunk in response.iter_content(chunk_size=1024):
        #         if chunk:
        #             f.write(chunk)

        def getter(u: str, f: IO):
            response = self.session.get(u)
            if response.status_code != 200:
                raise Exception(f"get {u} with status code {response.status_code}")
            f.write(response.content)

        self.hls = HLS.parse(url, getter)
        self.cache = Cache(directory, getter)
        self.file_path = file_path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def download(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self.cache.get, ts_url) for ts_url in self.hls.ts_url_list]
            total = len(futures)
            with tqdm(total=total) as bar:
                while total > 0:
                    done, futures = wait(futures, return_when=FIRST_COMPLETED)
                    for future in done:
                        e = future.exception()
                        if e is not None:
                            raise e
                    total -= len(done)
                    bar.update(len(done))
        self.cache.merge(self.hls.ts_url_list, self.file_path, self.hls.get_decoder())
