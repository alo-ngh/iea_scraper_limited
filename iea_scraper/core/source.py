import reprlib


class BaseSource:

    def __init__(self, code=None, url=None, path=None, **kwargs):
        self.code = code
        self.url = url
        self.path = path
        self.__dict__.update(kwargs)

    def __repr__(self):
        class_name = type(self).__name__
        string = f"{class_name}(code={self.code}, url={self.url}, path={self.path})"
        return reprlib.repr(string)
