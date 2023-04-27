import sys
import json
import colink as CL
from colink import CoLink, byte_to_str
from privshare.database import Database

def client_setup(cl: CoLink, config, provider_names, participants):
    # Load table-schema map and provider-table map to the client server
    for table_name, table_info in config["tables"].items():
        with open(table_info["schema_file_loc"]) as f:
            null_data_db = Database.deserialize_from_json(json.load(f))
            schema = null_data_db[table_name].schema
        cl.create_entry(":".join([table_name, "schema"]), schema.dump())
        providers_id = [provider_names.index(provider_name) 
                        for provider_name in table_info["providers"]]
        cl.create_entry(":".join([table_name, "providers"]), json.dumps(providers_id))
    # Trigger a task to set up the keys
    task_id = cl.run_task("key_setup", json.dumps(config["he"]), participants, True)
    cl.wait_task(task_id)

def client_run_query(cl: CoLink, sql, participants, config):
    cl.create_entry("query", sql)
    task_id = cl.run_task("query", json.dumps(config), participants, True)
    cl.wait_task(task_id)
    result = json.loads(byte_to_str(cl.read_entry("result")))
    cl.delete_entry("query")
    cl.delete_entry("result")
    return result

def start_interactive_client(cl: CoLink, participants, config):
    print("Start Interactive Mode")
    running_config = {
        "compile": config["compile"], 
        "debug": config["debug"], 
        "using_secure_context": True
    }
    while True:
        command = input("> ")
        if command.find("EXIT") != -1:
            return
        elif command.find("USE") != -1:
            if command.find("INSECURE") != -1:
                running_config["using_secure_context"] = False
            elif command.find("SECURE") != -1:
                running_config["using_secure_context"] = True
            result = "Context switched"
        elif command.find("SELECT") != -1:
            result = client_run_query(cl, command, participants, running_config)
        else:
            result = "Invalid command"
        print(result)
    
if __name__ == "__main__":
    addr = sys.argv[1]
    jwt = sys.argv[2]
    provider_names = [sys.argv[3]]
    provider_user_ids = [sys.argv[4]]
    config_file_loc = "./examples/demo/config.json"
    with open(config_file_loc) as f:
        config = json.load(f)
    cl = CoLink(addr, jwt)
    participants = [CL.Participant(user_id=cl.get_user_id(), role="client"),
                    CL.Participant(user_id=provider_user_ids[0], role="provider")]
    client_setup(cl, config, provider_names, participants)
    start_interactive_client(cl, participants, config)