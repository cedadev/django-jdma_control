"""Read in the config file for, convert from JSON to a dictionary and return
the config for that backend."""

import json

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
