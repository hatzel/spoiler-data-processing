import pytest
from reddit_import import Comment


@pytest.fixture
def raw_comment():
    return {
        "id": "666",
        "author": "hansole",
        "body": "",
        "gilded": False,
        "created_utc": 1119552233,
        "permalink": "some/link/herer",
        "score": 1337,
        "link_id": "t3_98a",
        "parent_id": "t1_duzm4vy"
    }


class TestCommentParsing:

    def testParsing(self, raw_comment):
        comment = Comment.from_raw(raw_comment)
        assert comment.id == int("666", base=36)
        assert comment.parent_comment_id == int("duzm4vy", base=36)


class TestSpoilerParsing:
    def testNewNotation(self, raw_comment):
        """Spoilers of the form: `>!Here be spoilers!<`"""
        raw_comment["body"] = "Heyho th(er)e are some (spoilers)[asdf] here" \
                              ">!spoiler text!<"
        comment = Comment.from_raw(raw_comment)
        assert len(comment.spoilers()) == 1
        assert comment.spoilers()[0].text == "spoiler text"
        assert comment.spoilers()[0].topic == None

    def testNewNotationMultiple(self, raw_comment):
        """Spoilers of the form: `>!Here be spoilers!<`"""
        raw_comment["body"] = "Heyho t<here are >!some!< spoilers here >!spoiler text!<"
        comment = Comment.from_raw(raw_comment)
        assert len(comment.spoilers()) == 2
        assert comment.spoilers()[0].text == "some"
        assert comment.spoilers()[0].topic is None
        assert comment.spoilers()[1].text == "spoiler text"
        assert comment.spoilers()[1].topic is None

    @pytest.mark.parametrize("symbol", ["/spoiler", "#spoiler"])
    def testPostfixNotation(self, raw_comment, symbol):
        """Spoilers of the form: [Here be spoilers](#spoiler)`"""
        raw_comment["body"] = f"Heyho th(er)e# are some " \
                              f"[spoiler text]({symbol}) ['link'](text)"
        comment = Comment.from_raw(raw_comment)
        assert len(comment.spoilers()) == 1
        assert comment.spoilers()[0].text == "spoiler text"
        assert comment.spoilers()[0].topic is None

    @pytest.mark.parametrize("symbol", ["/spoiler", "#spoiler"])
    def testPostfixNotationMultiple(self, raw_comment, symbol):
        raw_comment["body"] = f"""Heyho [some]({symbol})th(er)e# are
                                some [spoiler text]({symbol}) ['link'](text)"""
        comment = Comment.from_raw(raw_comment)
        spoilers = comment.spoilers()
        assert len(spoilers) == 2
        assert spoilers[0].text == "some"
        assert spoilers[0].topic is None
        assert spoilers[1].text == "spoiler text"
        assert spoilers[1].topic is None

    @pytest.mark.parametrize("symbol", ["#s", "/s", "/spoiler", "#spoiler"])
    def testTagged(self, raw_comment, symbol):
        raw_comment["body"] = f"""Lets.topic some [Tag]({symbol} spoiler) spoilers /s."""
        comment = Comment.from_raw(raw_comment)
        spoilers = comment.spoilers()
        assert len(spoilers) == 1
        assert spoilers[0].text == "spoiler"
        assert spoilers[0].topic == "Tag"

    @pytest.mark.parametrize("symbol", ["#s", "/s", "/spoiler", "#spoiler"])
    def testTaggedMultiple(self, raw_comment, symbol):
        raw_comment["body"] = f"""Lets.topic some [Tag]({symbol} "spoiler") spoilers [Tag2](/s more spoiler) /s."""
        comment = Comment.from_raw(raw_comment)
        spoilers = comment.spoilers()
        assert len(spoilers) == 2
        assert spoilers[0].text == "spoiler"
        assert spoilers[0].topic == "Tag"
        assert spoilers[1].text == "more spoiler"
        assert spoilers[1].topic == "Tag2"


class TestSparkTypes():
    def test_conversion(self, raw_comment):
        comment = Comment.from_raw(raw_comment)
        assert Comment.from_row(comment.to_row()) == comment
