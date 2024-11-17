import io
import os
from urllib.parse import urljoin
from typing import Callable, IO, List

import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

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

    def merge(self, url_list: List[str], file_path: str):
        with open(file_path, "wb") as f:
            for url in url_list:
                _, url_suffix = url.rsplit("/", 1)
                file_path1 = os.path.join(self.directory, url_suffix)
                with open(file_path1, "rb") as g:
                    f.write(g.read())


class HLS:

    def __init__(self, ts_url_list: List[str]):
        self.ts_url_list = ts_url_list

    @staticmethod
    def parse(url: str, get: Get) -> 'HLS':
        bs = BS(get)
        url_prefix, _ = url.rsplit("/", 1)
        result = bs.get_str(url)
        m3u8_url_list = [urljoin(url_prefix, m3u8_name) for m3u8_name in result.split("\n")
                         if not m3u8_name.startswith("#") and m3u8_name.endswith("m3u8")]
        print(m3u8_url_list)
        ts_url_list = []
        for m3u8_url in m3u8_url_list:
            result = bs.get_str(m3u8_url)
            ts_url_list.extend([urljoin(url_prefix, ts_name) for ts_name in result.split("\n")
                                if not ts_name.startswith("#") and ts_name.endswith("ts")])
        return HLS(ts_url_list)


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
        for url in self.hls.ts_url_list:
            print(url)
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
            self.cache.merge(self.hls.ts_url_list, self.file_path)
