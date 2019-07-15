"""
Representation of a Reddit post
"""
from datetime import datetime
import json
from reddit_import.schema import SchemaMixin
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType


class Post(SchemaMixin):
    schema = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("url", StringType(), nullable=False),
        StructField("selftext", StringType(), nullable=False),
        StructField("author", StringType(), nullable=True),
        StructField("created", TimestampType(), nullable=False),
        StructField("gilded", IntegerType(), nullable=False),
        StructField("flair_text", StringType(), nullable=True),
        StructField("permalink", StringType(), nullable=False),
        StructField("score", LongType(), nullable=False),
        StructField("over_18", BooleanType(), nullable=False),
        StructField("spoiler", BooleanType(), nullable=False),
        StructField("subreddit", StringType(), nullable=True),
        StructField("subreddit_id", LongType(), nullable=True),
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
        over_18,
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
        self.over_18 = over_18
        self.spoiler = spoiler
        self.subreddit = subreddit
        self.subreddit_id = subreddit_id

    @staticmethod
    def from_raw(raw):
        post = Post(
            id=int(raw["id"], 36),
            title=raw.get("title", ""),
            url=raw["url"],
            selftext=raw.get("selftext", "").replace("\u0000", ""),
            author=raw.get("author"),
            created=datetime.fromtimestamp(int(raw["created_utc"])),
            gilded=raw.get("gilded") or 0,
            flair_text=raw.get("link_flair_text"),
            permalink=raw["permalink"],
            score=raw.get("score") or 0,
            over_18=raw.get("over_18", False) or False,
            spoiler=raw.get("spoiler", False) or False,
            subreddit=raw.get("subreddit"),
            subreddit_id=int(raw["subreddit_id"].split("_")[-1], 36) if raw.get("subreddit_id") else None
        )
        return post

    @staticmethod
    def load_posts(session, path="reddit/submissions"):
        def load_from_json(string):
            try:
                return [Post.from_raw(json.loads(string))]
            except json.decoder.JSONDecodeError as err:
                print("[Warning] unable to parse post:", string)
                print("[Warning]", err)
                return []
            except Exception as err:
                print("[Warning] unknown error parsing post:", string)
                print("[Warning]", err)
                return []
        sc = session.sparkContext
        posts = sc.textFile(path)
        _ = posts.map(lambda line: line)
        parsed = posts.flatMap(lambda line: load_from_json(line))
        rows = parsed.map(lambda post: post.to_row())
        return session.createDataFrame(rows, Post.schema)
