import reddit_import
from reddit_import import Comment, Post
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession


def main():
    session = build_session()
    sc = session.sparkContext
    comments = Comment.load_comments(session)
    filtered_comments = comments.filter(comments.contains_spoiler == False)\
        .filter(comments.author != "AutoModerator")\
        .persist()
    posts = Post.load_posts(session)
    posts = posts.persist()
    spoiler_comments_without_spoiler_posts = filtered_comments.join(posts, filtered_comments.post_id == posts.id)


def build_context():
    conf = SparkConf()
    conf.setAppName("Reddit Spoilers")
    sc = SparkContext(conf=conf)
    return sc


def build_session():
    return  SparkSession.builder.appName("Reddit-Spoilers").getOrCreate()


if __name__ == "__main__":
    main()
