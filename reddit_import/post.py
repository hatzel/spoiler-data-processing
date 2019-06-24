"""
Representation of a Reddit post
"""
from datetime import datetime
import json
from reddit_import.schema import SchemaMixin
from html import unescape
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType


class Post(SchemaMixin):
    schema = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("url", StringType(), nullable=False),
        StructField("selftext", StringType(), nullable=False),
        StructField("author", StringType(), nullable=False),
        StructField("created", TimestampType(), nullable=False),
        StructField("gilded", IntegerType(), nullable=False),
        StructField("flair_text", StringType(), nullable=True),
        StructField("permalink", StringType(), nullable=False),
        StructField("score", LongType(), nullable=False),
        StructField("spoiler", BooleanType(), nullable=False),
        StructField("subreddit", StringType(), nullable=False),
        StructField("subreddit_id", LongType(), nullable=False),
    ])

    def __init__(
        self,
        id,
        title,
        url,
        selftext,
        author,
        created,
        gilded,
        flair_text,
        permalink,
        score,
        spoiler,
        subreddit,
        subreddit_id,
    ):
        self.id = id
        self.title = title
        self.url = url
        self.selftext = selftext
        self.author = author
        self.created = created
        self.gilded = gilded
        self.flair_text = flair_text
        self.permalink = permalink
        self.score = score
        self.spoiler = spoiler
        self.subreddit = subreddit
        self.subreddit_id = subreddit_id

    @staticmethod
    def from_raw(raw):
        post = Post(
            id=int(raw["id"], 36),
            title=unescape(raw["title"]),
            url=raw["url"],
            selftext=unescape(raw.get("selftext").replace("\u0000", "")),
            author=raw.get("author"),
            created=datetime.fromtimestamp(int(raw["created_utc"])),
            gilded=raw.get("gilded", 0),
            flair_text=unescape(raw.get("link_flair_text")),
            permalink=raw["permalink"],
            score=raw.get("score", 0),
            spoiler=raw.get("spoiler", False),
            subreddit=raw["subreddit"],
            subreddit_id=int(raw["subreddit_id"].split("_")[-1], 36),
        )
        return post

    @staticmethod
    def load_posts(session, path="reddit/submissions"):
        sc = session.sparkContext
        posts = sc.textFile(path)
        _ = posts.map(lambda line: line)
        parsed = posts.map(lambda line: Post.from_raw(json.loads(line)))
        rows = parsed.map(lambda post: post.to_row())
        return session.createDataFrame(rows, Post.schema)
