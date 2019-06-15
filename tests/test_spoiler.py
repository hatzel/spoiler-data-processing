# from src import spoiler
from reddit_import import Spoiler

class TestSpoiler():
    def test_parsing(self):
        spoilers = Spoiler.all_from_text('Hey, Snape [Harry Potter](/s "kills Dumbledore")')
        assert len(spoilers) == 1
        assert spoilers[0].text == "kills Dumbledore"
        assert spoilers[0].topic == "Harry Potter"
