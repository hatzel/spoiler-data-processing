#!/usr/bin/env python3
import argparse
import markdown
import pyspark
from reddit_import import util
from bs4 import BeautifulSoup


def build_parser():
    parser = argparse.ArgumentParser(description="Result Transformation")
    parser.add_argument("text-mode", nargs="?", choices=["html", "plain", "raw"],
                        help="Output mode of text", default="raw")
    # parser.add_argument("label-scale", nargs=1, choices=["document", "token"],
    #                     help="Class labels on token or document level.", default="document")
    parser.add_argument(
        "--comments", nargs="+",
        help="Text files to convert, usually one spoiler and one non spoiler file.",
    )
    return parser


def main(args):
    session = util.build_session(name="Merge spoilers and non-spoilers")
    comments = None
    for name in args.comments:
        if comments is None:
            comments = pyspark.read.json(name)
        else:
            comments.unionAll(pyspark.read.json(name))
    renderer = markdown.Markdown(
        extensions=["spoilers"]
    )
    parsed_comments = comments.select("text", "contains_spoiler")\
        .rdd\
        .map(lambda row: (
            parse_text(row["text"], renderer),
            row["contains_spoiler"],
        ))
    output = session.createDataFrame(parsed_comments, ["text", "spoiler"])
    output.write.csv("reddit/merged-comments-%s-%s.csv" % (args.text_mode, session.sparkContext.applicationId))


def parse_text(text, renderer):
    rendered = renderer.convert(text)
    soup = BeautifulSoup(rendered, "html.parser")
    return soup.get_text()


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)
