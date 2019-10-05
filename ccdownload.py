import gzip
import io
from gzip import decompress
import requests
import urllib
import itertools
import random

# There are 56000 files, each with size of 150MB (gzipped) / 450MB (text).
# Each file has 45000 urls.
#
# In total, there are about 2520000000 urls with a 25200 GB size (text).
#
# If we want to sample 1 GB data, we can download 1000 files and select 0.0025 of them.

paths_url_index = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-39/wet.paths.gz"

def get_wet_index(sample_size=None):
    data_domain_prefix = "https://commoncrawl.s3.amazonaws.com/"
    index_content = decompress(requests.get(paths_url_index).content)
    ans = [data_domain_prefix + line.decode().rstrip() for line in index_content.split()]
    if sample_size is None:
        return ans
    else:
        sample_ans = [ans[i] for i in sorted(random.sample(range(len(ans)), sample_size))]
        return sample_ans

def gz_url(url):
    """
    Sample input: https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-39/segments/1568514570740.10/wet/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz
    """
    response = urllib.request.urlopen(url)
    conn = gzip.GzipFile(fileobj=response)
    return io.TextIOWrapper(conn)

def parse_wet(textio):
    text = None
    stack = []
    for line in textio:


first_url = get_wet_index()[0]

https://commoncrawl.org/2015/04/announcing-the-common-crawl-index/