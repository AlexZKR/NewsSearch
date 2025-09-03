import io


class DummyPbar:
    def __init__(self):
        self.n = 0
        self.total = 0
        self.desc = ""
        self.position = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def update(self, value):
        # emulate tqdm behavior by increasing .n
        self.n += value

    def close(self):
        pass


class DummyHeaders:
    def __init__(self, mapping):
        self._m = mapping

    def get_header(self, name: str):
        return self._m.get(name)


class DummyContentStream:
    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)

    def read(self, *args, **kwargs):
        return self._buf.read(*args, **kwargs)


class DummyRecord:
    def __init__(
        self, rec_type: str, rec_headers: dict, http_headers: dict, content: bytes
    ):
        self.rec_type = rec_type
        self.rec_headers = DummyHeaders(rec_headers)
        self.http_headers = DummyHeaders(http_headers)
        self._content = content

    def content_stream(self):
        return DummyContentStream(self._content)

    def __repr__(self):
        return f"<DummyRecord rec_type={self.rec_type} id={self.rec_headers.get_header('WARC-Record-ID')}>"
