"""Minimal feedparser stub for Python 3.11 where sgmllib is removed."""
class _Feed:
    def __init__(self):
        self.entries = []
        self.feed = {}
        self.status = 200

def parse(url_file_stream_or_string, *args, **kwargs):
    return _Feed()
