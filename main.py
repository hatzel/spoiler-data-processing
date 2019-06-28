import reddit_import
from reddit_import import Comment, Post
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import desc, date_trunc

SUBREDDIT_WHITELIST = [
    "asoiaf",
    "gameofthrones",
    # this is just for testing
    "AdviceAnimals"
]

def main():
    session = build_session(name="Reddit Subreddit Counts")
    sc = session.sparkContext
    comments = Comment.load_comments(session)
    spoiler_comments = comments.filter(comments.contains_spoiler == True).persist()
    comment_spoilers_per_subreddit(session, spoiler_comments)
    comment_spoilers_per_month(session, spoiler_comments)
    # filtered_comments = comments.filter(comments.contains_spoiler == False)\
    #     .filter(comments.author != "AutoModerator")\
    #     .filter(" or ".join(["subreddit == '%s'" % s for s in SUBREDDIT_WHITELIST]))\
    #     .persist()
    # posts = Post.load_posts(session, path="examples/submissions.txt")
    # # print(posts.take(5))
    # posts = posts.persist()
    # # posts.join()
    # spoiler_comments_without_spoiler_posts = filtered_comments.join(posts, filtered_comments.post_id == posts.id)
    # print(spoiler_comments_without_spoiler_posts.collect())


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



def build_context(name="Reddit Spoilers"):
    conf = SparkConf()
    conf.setAppName(name)
    sc = SparkContext(conf=conf)
    return sc


def build_session(name="Reddit-Spoilers"):
    return  SparkSession.builder.appName(name).getOrCreate()


if __name__ == "__main__":
    main()
