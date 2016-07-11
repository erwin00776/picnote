import logging

LOG = logging.getLogger('dfs')
formatter = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
LOG.addHandler(stream_handler)

LOG.setLevel(logging.DEBUG)
