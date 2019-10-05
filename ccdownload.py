import gzip
import io
from gzip import decompress
import urllib3
import itertools
import random
import sqlite3
import os


# There are 56000 files, each with size of 150MB (gzipped) / 450MB (text).
# Each file has 45000 urls.
#
# In total, there are about 2520000000 urls with a 25200 GB size (text).
#
# If we want to sample 1 GB data, we can download 1000 files and select 0.0025 of them.

FILE_SAMPLE_SIZE = 1000
SAMPLE_RATIO = 0.0025

paths_url_index = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-39/wet.paths.gz"

def get_wet_index(sample_size=None):
    data_domain_prefix = "https://commoncrawl.s3.amazonaws.com/"
    index_content = gz_url(paths_url_index)
    ans = [data_domain_prefix + line.rstrip() for line in index_content]
    if sample_size is None:
        return ans
    else:
        sample_ans = [ans[i] for i in sorted(random.sample(range(len(ans)), sample_size))]
        return sample_ans

# Sample input: https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-39/segments/1568514570740.10/wet/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz
def gz_url(url):
    response = urllib3.request.urlopen(url)
    conn = gzip.GzipFile(fileobj=response)
    return io.TextIOWrapper(conn)

def parse_wet(textio):
    seek_status = 0
    target_url = None
    content_lines = []
    content = None
    while True:
        if seek_status == 0:
            try:
                if next(textio).rstrip() == "WARC/1.0" and next(textio).rstrip() == "WARC-Type: conversion":
                    url_line = next(textio).rstrip()
                    # Remove unused lines
                    date_line = next(textio)
                    record_id_line = next(textio)
                    refers_id_line = next(textio)
                    block_digest_line = next(textio)
                    content_type_line = next(textio)
                    content_length_line = next(textio)
                    empty_line = next(textio)
                    # get url and set seek_status
                    target_url = url_line.replace("WARC-Target-URI: ", "", 1)
                    seek_status = 1
                continue
            except StopIteration:
                break
        if seek_status > 0:
            line = next(textio).rstrip()
            if line != "":
                content_lines.append(line)
                continue
            if line == "":
                content = "\n".join(content_lines)
                content_lines = []
                yield target_url, content
                seek_status = 0
                continue
            raise Exception("Should not happen")
        raise Exception("Should not happen")

class CCDLDB:
    conn = None
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
    def create_urlcontent(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS urlcontent(target_url TEXT, content TEXT)
        """)
        return self.conn.commit()
    def insert_urlcontent(self, iter_url_content):
        sql = """
            INSERT INTO urlcontent(target_url, content) VALUES(?, ?)
        """
        cur = self.conn.cursor()
        cur.executemany(sql, iter_url_content)
        return self.conn.commit()

def iter_sample(iter, ratio):
    for each in iter:
        if random.uniform(0, 1) <= ratio:
            yield each

def main():
    random.seed(42)
    ccdldb = CCDLDB("data/ccdldb.sqlite3")
    ccdldb.create_urlcontent()

    urls = get_wet_index(sample_size=FILE_SAMPLE_SIZE)
    for url in urls:
        print("====== Processing WET file at {} ======".format(url))
        iter_url_content = parse_wet(gz_url(url))
        iter_url_content = iter_sample(iter_url_content, ratio=SAMPLE_RATIO)
        ccdldb.insert_urlcontent(iter_url_content)

main()

#first_url = get_wet_index()[0]
#records = parse_wet(gz_url(first_url))
#for url, content in records:
#    print(url)
#    print("===========")
#    print(content)
