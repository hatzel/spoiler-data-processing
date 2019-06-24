"""
Representation of a Reddit comment.
"""
from datetime import datetime
import json
from reddit_import.schema import SchemaMixin
from reddit_import.spoiler import Spoiler
from html import unescape
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType


class Comment(SchemaMixin):
    schema = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("author", StringType(), nullable=False),
        StructField("text", StringType(), nullable=False),
        StructField("gilded", IntegerType(), nullable=False),
        StructField("created", TimestampType(), nullable=False),
        StructField("permalink", StringType(), nullable=True),
        StructField("score", IntegerType(), nullable=False),
        StructField("post_id", LongType(), nullable=False),
        StructField("contains_spoiler", BooleanType(), nullable=False),
        StructField("parent_comment_id", LongType(), nullable=True),
        StructField("subreddit", StringType(), nullable=False),
        StructField("subreddit_id", LongType(), nullable=False),
    ])


    def __init__(
            self,
            id,
            author,
            text,
            gilded,
            created,
            permalink,
            score,
            post_id,
            subreddit,
            subreddit_id,
            contains_spoiler=None,
            parent_comment_id=None,
    ):
        self.id = id
        self.author = author
        self.text = text
        self.gilded = gilded
        self.created = created
        self.permalink = permalink
        self.score = score
        self.post_id = post_id
        self.parent_comment_id = parent_comment_id
        if contains_spoiler is None:
            self.contains_spoiler = len(self.spoilers()) > 0
        else:
            self.contains_spoiler = contains_spoiler
        self.subreddit = subreddit
        self.subreddit_id = subreddit_id

    def __eq__(self, other):
        if isinstance(other, Comment):
            return all(
                self.__getattribute__(field.name) == other.__getattribute__(field.name)
                for field in self.schema
            )
        else:
            return NotImplemented

    @staticmethod
    def from_raw(raw):
        if raw["link_id"] != raw["parent_id"]:
            parent_comment_id = int(raw["parent_id"].split("_")[-1], 36)
        else:
            parent_comment_id = None
        comment = Comment(
            id=int(str(raw["id"]).split("_")[-1], 36),
            author=raw["author"],
            text=unescape(raw["body"].replace("\u0000", "")),
            gilded=raw["gilded"],
            created=datetime.fromtimestamp(int(raw["created_utc"])),
            permalink=raw.get("permalink"),
            score=raw["score"],
            post_id=int(raw["link_id"].split("_")[-1], 36),
            parent_comment_id=parent_comment_id,
            subreddit=raw["subreddit"],
            subreddit_id=int(raw["subreddit_id"].split("_")[-1], 36),
        )
        return comment

    def spoilers(self):
        return Spoiler.all_from_text(self.text)

    @staticmethod
    def load_comments(session, path="reddit/comments"):
        sc = session.sparkContext
        comments = sc.textFile(path)
        parsed = comments.map(lambda line: Comment.from_raw(json.loads(line)))
        rows = parsed.map(lambda comment: comment.to_row())
        return session.createDataFrame(rows, Comment.schema)

    @staticmethod
    def extract_spoilers(df):
        return df.filter(df.contains_spoiler == True)
