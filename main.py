import reddit_import
from reddit_import import Comment, Post, util
import json
from pyspark import StorageLevel
from pyspark.sql.functions import desc, date_trunc
import argparse
from datetime import datetime


def build_parser():
    parser = argparse.ArgumentParser(description="Reddit Spoiler Processing")
    parser.add_argument("collect", nargs="+", choices=["statistics", "spoiler_comments", "non_spoiler_comments"])
    parser.add_argument("--posts-path", help="Path to submissions.", default="reddit/submissions")
    parser.add_argument("--comments-path", help="Path to posts.", default="reddit/comments")
    parser.add_argument("--month", help="Show comment spoilers in a specific month (only affects subreddit stasitics) YYYY-MM.", type=validate_date)
    parser.add_argument("--whitelist", nargs="?",
                        type=argparse.FileType("r"),
                        default="subreddit_whitelist.txt")
    return parser


def validate_date(date_string):
    try:
        return datetime.strptime(date_string, "%Y-%m")
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Not a valid date: %s" % date_string
        )


def main(args):
    session = util.build_session(name="Reddit Subreddit Counts")
    comments = Comment.load_comments(session, path=args.comments_path).persist(StorageLevel.DISK_ONLY)
    spoiler_comments = comments.filter(comments.contains_spoiler == True).persist(StorageLevel.MEMORY_AND_DISK)

    posts = Post.load_posts(session, path=args.posts_path).persist(StorageLevel.DISK_ONLY)
    whitelist = [line.strip() for line in args.whitelist.readlines()]

    non_spoiler_post_ids = posts.filter(posts.spoiler == False)\
        .filter(posts.over_18 == False)\
        .filter(~(posts.title.contains("Spoiler") | posts.title.contains("spoiler")))\
        .filter(" or ".join(["subreddit == '%s'" % s for s in whitelist]))\
        .select("id")\
        .persist(StorageLevel.DISK_ONLY)

    whitelisted_spoiler_comments = spoiler_comments\
        .filter(comments.author != "AutoModerator")\
        .filter(" or ".join(["subreddit == '%s'" % s for s in whitelist]))\
        .persist(StorageLevel.MEMORY_AND_DISK)

    if "statistics" in args.collect:
        comment_spoilers_per_month_and_sub(session, spoiler_comments)
        save_comment_per_month(session, spoiler_comments, base_name="spoilers_comments_per_month")
        save_comment_per_month(session, comments, base_name="total_comments-per_month")
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

        non_spoiler_comments = comments\
            .filter(comments.distinguished.isNull())\
            .filter(comments.score >= 3)\
            .filter(comments.text != "[deleted]" | comments.author != "[deleted]")\
            .filter(comments.text != "[removed]" | comments.author != "[deleted]")\
            .filter(comments.author != "AutoModerator")\
            .filter(comments.contains_spoiler == False)\
            .filter(" or ".join(["subreddit == '%s'" % s for s in whitelist]))\
            .join(non_spoiler_post_ids, comments.post_id == non_spoiler_post_ids.id)\
            .drop(non_spoiler_post_ids.id)\
            .persist(StorageLevel.DISK_ONLY)

        for subreddit, spoiler_count in spoiler_counts_per_sub.items():
            subreddit_non_spoilers = non_spoiler_comments\
                    .filter(non_spoiler_comments.subreddit == subreddit)\
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


def save_comment_per_month(session, comments, base_name):
    counts_per_month = comments\
        .select(date_trunc("month", comments.created).alias("month"))\
        .groupby("month")\
        .count()
    counts_per_month.write.csv(
        "reddit/%s-%s.csv" % (base_name, session.sparkContext.applicationId)
    )


def comment_spoilers_per_month_and_sub(session, spoiler_comments):
    spoiler_counts = spoiler_comments\
        .select(date_trunc("month", spoiler_comments.created).alias("month"), "subreddit")\
        .groupby("month", "subreddit")\
        .count()\
        .rdd\
        .groupBy(lambda row: row["month"])\
        .cache()
    top_subs = spoiler_counts\
        .mapValues(lambda values: sorted(values, key=lambda row: row["count"]))\
        .map(lambda pair: pair[1][:5])\
        .flatMap(lambda rows: [row["subreddit"] for row in rows])\
        .distinct()\
        .collect()
    top_sub_counts = spoiler_counts\
        .map(lambda pair: [pair[0]] + subreddit_counts(top_subs, pair[1]))
    header = session.sparkContext.parallelize([["month"] + top_subs + ["others"]])
    header.map(lambda row: ",".join(row)).saveAsTextFile(
        "reddit/spoilers_per_month_and_sub_header-%s.csv" % session.sparkContext.applicationId
    )
    top_sub_counts.map(lambda row: ",".join(str(el) for el in row)).saveAsTextFile(
        "reddit/spoilers_per_month_and_sub-%s.csv" % session.sparkContext.applicationId
    )
    spoiler_counts.unpersist()


def subreddit_counts(subreddits, rows):
    counts = {row["subreddit"]: row["count"] for row in rows}
    out_counts = []
    for sub in subreddits:
        out_counts.append(counts.get(sub, 0))
        if counts.get(sub) is not None:
            del counts[sub]
    # sum of 'others'
    out_counts.append(sum(counts.values()))
    return out_counts


def comment_spoilers_per_subreddit(session, spoiler_comments, month=None):
    if month:
        comments_with_month = spoiler_comments\
            .withColumn("month", date_trunc("month", spoiler_comments.created))
        spoiler_comments = comments_with_month\
            .filter(comments_with_month.month == month)
    spoiler_counts_per_subreddit = spoiler_comments.groupby("subreddit")\
        .count()\
        .sort(desc("count"))
    spoiler_counts_per_subreddit.write.csv(
        "reddit/spoilers_per_subreddit-%s.csv" % session.sparkContext.applicationId
    )
    return spoiler_counts_per_subreddit


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)
