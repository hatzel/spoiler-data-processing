import requests
from bs4 import BeautifulSoup as BS
import subprocess
import re
import os

INCOMING_DIR = "/home/2hatzel/reddit_incoming"
HDFS_REDDIT_DATA_DIR = "/user/2hatzel/reddit"
BASE_URL = "https://files.pushshift.io/reddit"


def main():
    hdfs_comment_files, hdfs_post_files = [
        get_hdfs_file_list("comments"),
        get_hdfs_file_list("submissions"),
    ]
    comment_files = set(get_pushshift_file_list("comments"))
    post_files = set(get_pushshift_file_list("submissions"))
    for file_name in comment_files:
        if file_name not in hdfs_comment_files:
            if download("comments", file_name):
                push_to_hdfs("comments", file_name)
    for file_name in post_files:
        if file_name not in hdfs_post_files:
            if download("submissions", file_name):
                push_to_hdfs("submissions", file_name)


def push_to_hdfs(data_type, file_name):
    path = f"{INCOMING_DIR}/{data_type}/{file_name}"
    ret = subprocess.run([
        "hdfs",
        "dfs",
        "-D",
        "dfs.replication=2",
        "-copyFromLocal",
        path,
        f"{HDFS_REDDIT_DATA_DIR}/{data_type}"
    ])
    if ret.returncode == 0:
        print(f"Successfully copied {file_name} to hdfs, removing local copy.")
        # We have copied it over
        os.remove(path)
    else:
        print("Failed to copy to hdfs, preserving file.")


def download(data_type, file_name):
    print(f"Downloading {file_name}...")
    ret = subprocess.run([
        "wget",
        "--output-file=/tmp/reddit-download-log",
        "-c",
        "-O", f"{INCOMING_DIR}/{data_type}/{file_name}",
        f"{BASE_URL}/{data_type}/{file_name}",
    ])
    if ret.returncode == 0:
        print(f"Successfully downloaded {file_name}.")
    else:
        print(f"Failed to download {file_name}: {ret.stderr}, {ret.stdout}")
    return ret.returncode == 0


def get_pushshift_file_list(data_type):
    short_form = {
        "comments": "RC",
        "submissions": "RS",
    }[data_type]
    resp = requests.get(f"{BASE_URL}/{data_type}/")
    soup = BS(resp.text, features="lxml")
    hrefs = soup.find_all("a", href=True)
    return [item["href"][2:] for item in hrefs
            if item["href"].startswith(f"./{short_form}")]


def get_hdfs_file_list(data_type):
    ret = subprocess.run(f"hdfs dfs -ls {HDFS_REDDIT_DATA_DIR}/{data_type}",
                         shell=True, capture_output=True)
    file_name_pattern = re.compile(
        f"{re.escape(HDFS_REDDIT_DATA_DIR)}/{data_type}/(.+)\n"
    )
    matches = file_name_pattern.finditer(ret.stdout.decode("utf-8"))
    return [m.group(1) for m in matches]


if __name__ == "__main__":
    main()
