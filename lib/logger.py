import logging
import sys


def logger(name):

    log = logging.getLogger(name)
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    out_hdlr.setLevel(logging.DEBUG)
    log.addHandler(out_hdlr)
    log.setLevel(logging.DEBUG)
    return log
