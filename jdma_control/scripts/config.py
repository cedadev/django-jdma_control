"""Read in the config file for, convert from JSON to a dictionary and return
the config for that backend."""

import json
import logging

def config_path():
    """Return the path of the config file"""
    path = "/etc/jdma/jdma_config.json"
    return path

def read_backend_config(backend):
    """Read in the config file and return the dictionary for the backend."""
    cfg_path = config_path()
    fh = open(cfg_path)
    cfg = json.load(fh)
    fh.close()
    try:
        return cfg["backends"][backend]
    except Exception as e:
        raise Exception("Backend {} not found in config file {}".format(
            backend,
            cfg_path)
        )

def read_process_config(process):
    """Read in the config file and return the dictionary for the process."""
    cfg_path = config_path()
    fh = open(cfg_path)
    cfg = json.load(fh)
    fh.close()
    try:
        return cfg["processes"][process]
    except Exception as e:
        raise Exception("Process {} not found in config file {}".format(
            process,
            cfg_path)
        )

def get_logging_level(loglevel):
    """Convert a logging level string into a logging.LOG_LEVEL"""
    if loglevel == "DEBUG":
        return logging.DEBUG
    elif loglevel == "INFO":
        return logging.INFO
    elif loglevel == "WARNING":
        return logging.WARNING
    elif loglevel == "ERROR":
        return logging.ERROR
    elif loglevel == "CRITICAL":
        return logging.CRITICAL

def get_logging_format():
    """return the format string for the logger"""
    formt = "[%(asctime)s] %(levelname)s:%(message)s"
    return formt
