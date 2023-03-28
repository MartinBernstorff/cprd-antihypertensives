import yaml


def yaml_load(path):
    with open(path) as ymlfile:
        cfg = yaml.full_load(ymlfile)
    return cfg


def yaml_save(cfg, path):
    with open(path, "w") as ymlfile:
        yaml.dump(
            cfg,
            ymlfile,
            default_style=None,
            default_flow_style=None,
            encoding="utf-8",
            line_break=12,
        )
