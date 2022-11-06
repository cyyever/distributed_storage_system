import hydra

global_config = {}


# @hydra.main(config_path="conf", version_base=None)
def load_config(config_name=None):
    with hydra.initialize(config_path="conf", version_base=None):
        return hydra.compose(config_name)
