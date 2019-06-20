import reddit_import
from reddit_import import Comment, Post
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession


def main():
    session = build_session()
    sc = session.sparkContext
    comments = load_comments(session)
    filtered_comments = comments.filter(comments.contains_spoiler == False)\
        .filter(comments.author != "AutoModerator")\
        .persist()
    posts = load_posts(session)
    posts = posts.persist()
    spoiler_comments_without_spoiler_posts = filtered_comments.join(posts, filtered_comments.post_id == posts.id)


def build_context():
    conf = SparkConf()
    conf.setAppName("Reddit Spoilers")
    sc = SparkContext(conf=conf)
    return sc

def build_session():
    return  SparkSession.builder.appName("Reddit-Spoilers").getOrCreate()


def load_posts(session):
    sc = session.sparkContext
    posts = sc.textFile("reddit/submissions")
    parsed = posts.map(lambda line: Post.from_raw(json.loads(line)))
    rows = parsed.map(lambda post: post.to_row())
    return session.createDataFrame(rows, Post.schema)

def load_comments(session):
    sc = session.sparkContext
    comments = sc.textFile("reddit/comments")
    parsed = comments.map(lambda line: Comment.from_raw(json.loads(line)))
    rows = parsed.map(lambda comment: comment.to_row())
    return session.createDataFrame(rows, Comment.schema)


if __name__ == "__main__":
    main()
