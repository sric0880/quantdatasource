import pathlib
import yaml

config = None

if config is None:
    with open("quantdatasource_config.yml", "r") as f:
        config = yaml.safe_load(f)
    pathlib.Path(config["astock_output"]).mkdir(parents=True, exist_ok=True)
    pathlib.Path(config["future_output"]).mkdir(parents=True, exist_ok=True)