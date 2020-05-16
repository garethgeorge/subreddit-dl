import requests
import urllib.parse
import json
import argparse
import gzip
import datetime
import time

parser = argparse.ArgumentParser(description='Download some reddit posts')
parser.add_argument('subreddit', type=str,
                    help="the subreddit to download")
parser.add_argument('--before', type=int, default=None,
                    help='UTC Epoch time posts must come before')
parser.add_argument('--after', type=int, default=None,
                    help='UTC Epoch time posts must come after')
parser.add_argument('--pushshift_urlbase', type=str, default="https://api.pushshift.io/",
                    help='UTC Epoch time posts must come after')
parser.add_argument('dumpfile', type=str,
                    help='a .ndjson.gz file that will contain new line delimited JSON entries for all of the posts downloaded from reddit (ndjson spec: http://ndjson.org/)')

args = parser.parse_args()

PUSHSHIFT_URL = args.pushshift_urlbase

fields = ["author", "created_utc", "permalink", "id", "num_comments",
          "num_crossposts", "retrieved_on", "score", "subreddit", "title", "selftext"]


def make_pushshift_url(after=None, before=None, subreddit=None):
    query_hash = {
        "size": 500,
        "sort": "asc",
        "sort_type": "created_utc",
    }

    if subreddit:
        query_hash["subreddit"] = subreddit
    if before:
        query_hash["before"] = before
    if after:
        query_hash["after"] = after

    url = PUSHSHIFT_URL + "/reddit/search/submission/?" + \
        urllib.parse.urlencode(query_hash) + "&fields=" + ",".join(fields)
    return url


def download_posts(after=None, before=None,  subreddit=None):
    latest_post = after

    while True:
        url = make_pushshift_url(
            after=latest_post, before=before, subreddit=subreddit)
        r = requests.get(url)

        posts = r.json()["data"]
        if after:
            posts = (post for post in posts if post["created_utc"] >= after)
        if before:
            posts = (post for post in posts if post["created_utc"] <= before)
        posts = list(posts)
        print("fetched batch of %d posts from %s, up to date: %s" % (
            len(posts), url, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(latest_post))))
        if len(posts) == 0:
            print("no more data available -- returning early")
            break
        yield from posts

        latest_post = max(post["created_utc"] for post in posts) + 1


with gzip.open(args.dumpfile, "wb", 9) as dumpf:
    for post in download_posts(after=args.after, before=args.before, subreddit=args.subreddit):
        data = json.dumps(post, sort_keys=True) + "\n"
        dumpf.write(data.encode("utf-8"))
