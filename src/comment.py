"""
Representation of a Reddit comment.
"""
from datetime import datetime
import dataclasses
from collections import OrderedDict
from schema import SchemaMixin


@dataclasses.dataclass
class Comment(SchemaMixin):
    id: int
    author: str
    text: str
    gilded: bool
    created: datetime
    permalink: str
    score: int
    post_id: int
    contains_spoiler: bool = False

    parent_comment_id: int = None

    @staticmethod
    def from_raw(raw):
        comment = Comment(
            id=int(str(raw["id"]).split("_")[-1], 36),
            author=raw["author"],
            text=raw["body"],
            gilded=raw["gilded"],
            created=datetime.fromtimestamp(int(raw["created_utc"])),
            permalink=raw.get("permalink"),
            score=raw["score"],
            post_id=int(raw["link_id"].split("_")[-1], 36),
        )
        comment.spoiler = len(comment.spoilers()) > 0
        if raw["link_id"] != raw["parent_id"]:
            comment.parent_comment_id = int(raw["parent_id"].split("_")[-1], 36)
        return comment

    def spoilers(self):
        return ["lol"]
        # TODO:
        # return Spoiler.extract(self.text)
