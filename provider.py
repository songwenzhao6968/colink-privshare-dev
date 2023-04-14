import sys
import json
from colink import CoLink
from database import DataBase

def provider_setup(cl, config):
    # Load the database to the provider server
    with open(config["database_file_loc"]) as f:
        db = DataBase.deserialize_from_json(json.load(f))
    cl.create_entry("database", db.dump())

if __name__ == "__main__":
    config_dir = "./examples/demo/config.json"
    with open(config_dir) as f:
        config = json.load(f)
    addr = sys.argv[1]
    jwt = sys.argv[2]
    cl = CoLink(addr, jwt)
    provider_setup(cl, config["servers"]["provider_1"])