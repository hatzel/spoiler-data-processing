"""
Representation of a Reddit post
"""
from datetime import datetime
from reddit_import.schema import SchemaMixin
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType


class Post(SchemaMixin):
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("url", StringType(), nullable=False),
        StructField("selftext", StringType(), nullable=False),
        StructField("author", StringType(), nullable=False),
        StructField("created", DateType(), nullable=False),
        StructField("gilded", BooleanType(), nullable=False),
        StructField("flair_text", StringType(), nullable=False),
        StructField("permalink", StringType(), nullable=False),
        StructField("score", IntegerType(), nullable=False),
        StructField("spoiler", BooleanType(), nullable=False),
        StructField("subreddit", StringType(), nullable=False),
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

    @staticmethod
    def from_raw(raw):
        post = Post(
            id=int(raw["id"], 36),
            title=raw["title"],
            url=raw["url"],
            selftext=raw.get("selftext").replace("\u0000", ""),
            author=raw.get("author"),
            created=datetime.fromtimestamp(int(raw["created_utc"])),
            gilded=raw.get("gilded", 0),
            flair_text=raw.get("link_flair_text"),
            permalink=raw["permalink"],
            score=raw.get("score", 0),
            spoiler=raw.get("spoiler", False),
            subreddit_id=int(raw["subreddit_id"].split("_")[-1], 36),
        )
        return post
