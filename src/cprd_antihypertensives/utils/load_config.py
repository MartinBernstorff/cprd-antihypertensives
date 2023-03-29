from pathlib import Path

from cprd_antihypertensives.cprd.utils.yaml_act import yaml_load


def load_config(config_path: Path):
    class dotdict(dict):
        """dot.notation access to dictionary attributes"""

        __getattr__ = dict.get
        __setattr__ = dict.__setitem__  # type: ignore
        __delattr__ = dict.__delitem__  # type: ignore

    config = yaml_load(
        dotdict({"params": config_path}).params,  # type: ignore
    )

    return config
