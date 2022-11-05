import hydra

global_config = {}


@hydra.main(config_path="conf", version_base=None)
def load_config(conf) -> None:
    global global_config
    global_config |= conf
