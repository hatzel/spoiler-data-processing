import reddit_import
from reddit_import import Comment, Post
import json
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import desc, date_trunc
import argparse

SUBREDDIT_WHITELIST = [
    "asoiaf",
    "gameofthrones",
    # this is just for testing
    "AdviceAnimals"
]

def build_parser():
    parser = argparse.ArgumentParser(description="Reddit Spoiler Processing")
    parser.add_argument("collect", nargs="+", choices=["statistics", "spoiler_comments", "non_spoiler_comments"])
    parser.add_argument("--posts-path", help="Path to submissions.", default="reddit/submissions")
    parser.add_argument("--comments-path", help="Path to posts.", default="reddit/comments")
    parser.add_argument("--whitelist", nargs="+",
                        type=argparse.FileType("r"),
                        default="subreddit_whitelist.txt")
    return parser


def main(args):
    session = build_session(name="Reddit Subreddit Counts")
    sc = session.sparkContext
    comments = Comment.load_comments(session, path=args.comments_path).persist(StorageLevel.DISK_ONLY)
    spoiler_comments = comments.filter(comments.contains_spoiler == True).persist(StorageLevel.MEMORY_AND_DISK)

    posts = Post.load_posts(session, path=args.posts_path).persist(StorageLevel.DISK_ONLY)
    non_spoiler_post_ids = posts.filter(posts.spoiler == False).select("id").persist(StorageLevel.DISK_ONLY)

    whitelist = [line.strip() for line in args.whitelist.readlines()]
    whitelisted_spoiler_comments = spoiler_comments\
        .filter(comments.author != "AutoModerator")\
        .filter(" or ".join(["subreddit == '%s'" % s for s in whitelist]))\
        .persist(StorageLevel.MEMORY_AND_DISK)

    if "statistics" in args.collect:
        comment_spoilers_per_subreddit(session, spoiler_comments)
        comment_spoilers_per_month(session, spoiler_comments)
    if "spoiler_comments" in args.collect:
        if len(whitelist) == 0:
            print("Error, whitelist has 0 elements")
        spoiler_comments_without_spoiler_posts = whitelisted_spoiler_comments\
                .join(non_spoiler_post_ids, whitelisted_spoiler_comments.post_id == non_spoiler_post_ids.id)\
                .drop(non_spoiler_post_ids.id)
        spoiler_comments_without_spoiler_posts\
                .write.json("reddit/spoiler-comments-%s.csv" % session.sparkContext.applicationId)
    if "non_spoiler_comments" in args.collect:
        spoiler_counts_per_sub = whitelisted_spoiler_comments.groupby("subreddit")\
            .count()\
            .collect()
        spoiler_counts_per_sub = {row["subreddit"]: row["count"] for row in spoiler_counts_per_sub}
        print(spoiler_counts_per_sub)

        non_spoiler_comments = comments.filter(comments.contains_spoiler == False).persist(StorageLevel.DISK_ONLY)
        for subreddit, spoiler_count in spoiler_counts_per_sub.items():
            subreddit_non_spoilers = non_spoiler_comments.filter(comments.author != "AutoModerator")\
                    .filter(non_spoiler_comments.subreddit == subreddit)\
                    .join(non_spoiler_post_ids, non_spoiler_comments.post_id == non_spoiler_post_ids.id)\
                    .drop(non_spoiler_post_ids.id)\
                    .persist(StorageLevel.MEMORY_AND_DISK)
            non_spoiler_count = subreddit_non_spoilers.count()
            # Due to this sampling we are not guaranteed to get the exact same counts
            if spoiler_count > non_spoiler_count:
                print(
                    "[Warning] We will not be able to sample enough non-spoilers for %s (spoilers: %d, non-spoilers: %d)" %
                    (subreddit, spoiler_count, non_spoiler_count)
                )
                fraction = 1.0
            else:
                fraction = spoiler_count / non_spoiler_count
            sampled = subreddit_non_spoilers.sample(fraction=fraction, seed=42)
            sampled.write\
                    .json("reddit/non-spoilers-%s.csv" % session.sparkContext.applicationId, mode="append")
            subreddit_non_spoilers.unpersist()


def comment_spoilers_per_month(session, spoiler_comments):
    spoiler_counts_per_subreddit = spoiler_comments\
        .select(date_trunc("month", spoiler_comments.created).alias("month"))\
        .groupby("month")\
        .count()
    spoiler_counts_per_subreddit.write.csv(
        "reddit/spoilers_per_month-%s.csv" % session.sparkContext.applicationId
    )


def comment_spoilers_per_subreddit(session, spoiler_comments):
    spoiler_counts_per_subreddit = spoiler_comments.groupby("subreddit")\
        .count()\
        .sort(desc("count"))
    spoiler_counts_per_subreddit.write.csv(
        "reddit/spoilers_per_subreddit-%s.csv" % session.sparkContext.applicationId
    )
    return spoiler_counts_per_subreddit


def build_context(name="Reddit Spoilers"):
    conf = SparkConf()
    conf.setAppName(name)
    sc = SparkContext(conf=conf)
    return sc


def build_session(name="Reddit-Spoilers"):
    return  SparkSession.builder.appName(name).getOrCreate()


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)
