"""
Representation of a Reddit comment.
"""
from datetime import datetime
import json
from reddit_import.schema import SchemaMixin
from reddit_import.spoiler import Spoiler
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType


class Comment(SchemaMixin):
    schema = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("author", StringType(), nullable=False),
        StructField("distinguished", StringType(), nullable=True),
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
            distinguished,
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
        self.distinguished = distinguished
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
            distinguished=raw.get("distinguished"),
            text=raw["body"].replace("\u0000", ""),
            gilded=raw["gilded"],
            created=datetime.fromtimestamp(int(raw["created_utc"])),
            permalink=raw.get("permalink"),
            score=int(raw.get("score") or 0),
            post_id=int(raw["link_id"].split("_")[-1], 36),
            parent_comment_id=parent_comment_id,
            subreddit=raw["subreddit"],
            subreddit_id=int(raw["subreddit_id"].split("_")[-1], 36),
        )
        return comment

    @staticmethod
    def from_serialized(data):
        """Convert serialized json reperesentation back to comment."""
        return Comment(
            data["id"],
            data["author"],
            data["distinguished"],
            data["text"],
            data["gilded"],
            data["created"],
            data.get("permalink"),
            data["score"],
            data["post_id"],
            data["subreddit"],
            data["subreddit_id"],
            contains_spoiler=data.get("contains_spoiler", None),
            parent_comment_id=data.get("parent_comment_id", None),
        )

    def get_text(self, mode="raw"):
        return self.text

    def spoilers(self):
        try:
            return Spoiler.all_from_text(self.text)
        except TypeError as e:
            print("Error parsing spoiler", e)
            print("With text", self.text)
            raise e

    @staticmethod
    def load_comments(session, path="reddit/comments"):
        def load_from_json(string):
            try:
                return [Comment.from_raw(json.loads(string))]
            except json.decoder.JSONDecodeError as err:
                print("[Warning] unable to parse comment:", string)
                print("[Warning]", err)
                return []
            except Exception as err:
                print("[Warning] unknown error parsing:", string)
                print("[Warning]", err)
                return []
        sc = session.sparkContext
        comments = sc.textFile(path)
        parsed = comments.flatMap(
            lambda line: load_from_json(line)
        )
        rows = parsed.map(lambda comment: comment.to_row())
        return session.createDataFrame(rows, Comment.schema)

    @staticmethod
    def extract_spoilers(df):
        return df.filter(df.contains_spoiler == True)
