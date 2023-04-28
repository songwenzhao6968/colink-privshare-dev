import sys
import json
from colink import CoLink
from privshare.database import Database

def provider_setup(cl: CoLink, config):
    # Load the database to the provider server
    with open(config["database_file_loc"]) as f:
        db = Database.deserialize_from_json(json.load(f))
    cl.create_entry("database", db.dump())

if __name__ == "__main__":
    config_file_loc = "./examples/mock/config.json"
    with open(config_file_loc) as f:
        config = json.load(f)
    addr = sys.argv[1]
    jwt = sys.argv[2]
    cl = CoLink(addr, jwt)
    provider_setup(cl, config["servers"]["provider_1"])