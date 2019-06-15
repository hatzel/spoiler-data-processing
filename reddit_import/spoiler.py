import markdown


# Monkey patching the markdown renderer
def get_spoilers(tree):
    return SpoilerCollection(data=tree.findall(".//span[@class='spoiler']"))


markdown.Markdown.output_formats["spoilers"] = get_spoilers


class SpoilerCollection():
    """Collection that has some methods required to work with markdown."""

    def __init__(self, data=None):
        self.data = data or []

    def strip(self):
        return self.data


class Spoiler():
    renderer = markdown.Markdown(
        output_format="spoilers",
        extensions=["spoilers"]
    )

    def __init__(self, text, topic):
        self.text = text
        self.topic = topic

    @classmethod
    def all_from_text(cls, text):
        cls.renderer.stripTopLevelTags = False
        cls.renderer.postprocessors = []
        spoilers = []
        for element in cls.renderer.convert(text):
            spoilers.append(Spoiler(element.text, element.get("topic")))
        return spoilers
