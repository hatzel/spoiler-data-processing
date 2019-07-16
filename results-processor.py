#!/usr/bin/env python3
import argparse
import markdown
import pyspark
from pyspark.sql.functions import rand
from reddit_import import util
from bs4 import BeautifulSoup, NavigableString

SPLITS = [
    ("train", 0.7),
    ("dev", 0.1),
    ("test", 0.2),
]


def build_parser():
    parser = argparse.ArgumentParser(description="Result Transformation")
    parser.add_argument("--text-mode", choices=["html", "plain"],
                        help="Output mode of text", default="plain")
    parser.add_argument("--classify", choices=["document", "token"],
                        help="Class labels on token or document level.", default="document")
    parser.add_argument(
        "--comments", nargs="+",
        help="Text files to convert, usually one spoiler and one non spoiler file.",
    )
    return parser


def convert_text(text, text_mode, classify, renderer):
    remove_other_tags = text_mode == "plain"
    remove_spoiler_tags = classify == "document"
    tag_whitelist = [] if remove_spoiler_tags else ["spoiler"]
    tag_blacklist = ["spoiler"] if remove_spoiler_tags else []
    return remove_tags(
        convert_to_spoiler_tags(
            renderer.convert(text)
        ),
        remove_others=remove_other_tags,
        whitelist=tag_whitelist,
        blacklist=tag_blacklist,
    )


def main(args):
    session = util.build_session(name="Merge spoilers and non-spoilers")
    comments = None
    for name in args.comments:
        if comments is None:
            comments = session.read.json(name)
        else:
            comments = comments.unionAll(session.read.json(name))
    renderer = markdown.Markdown(
        extensions=["spoilers"]
    )
    parsed_comments = comments.select("text", "contains_spoiler")\
        .orderBy(rand())\
        .rdd\
        .map(lambda row: (
            convert_text(row["text"], args.text_mode, args.classify, renderer),
            row["contains_spoiler"],
        ))
    output = session.createDataFrame(parsed_comments, ["text", "spoiler"]).cache()
    split_data = output.randomSplit([value for _, value in SPLITS], seed=1)
    for i, (split, _) in enumerate(SPLITS):
        split_data[i].write.json(
            "reddit/%s-%s-%s-%s-%s.json"
            % (split, args.text_mode, args.text_mode, args.classify, session.sparkContext.applicationId)
        )


def convert_to_spoiler_tags(html):
    """Converts span tags with spoiler class to spoiler tag."""
    soup = BeautifulSoup(html)
    for tag in soup.find_all(True):
        if tag.name == "span" and "spoiler" in tag.get("class"):
            tag.name = "spoiler"
            del tag["class"]
    return soup


def remove_tags(soup, remove_others, whitelist=tuple(), blacklist=tuple()):
    """Remove all but whitelisted tags."""
    for tag in soup.find_all(True):
        if (remove_others and tag.name not in whitelist) or (tag.name in blacklist):
            tag.unwrap()
    return str(soup)



if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)
