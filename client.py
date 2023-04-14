import sys
import json
import colink as CL
from colink import CoLink
from database import DataBase

def client_setup(cl, config, participants):
    # Trigger a task to set up the keys
    param = {"config": config["he"]}
    task_id = cl.run_task("key_setup", json.dumps(param), participants, True)
    # Load schema info to the client server
    for table_name, table_info in config["tables"].items():
        with open(table_info["schema_file_loc"]) as f:
            null_data_db = DataBase.deserialize_from_json(json.load(f))
            schema = null_data_db[table_name].schema
        cl.create_entry(":".join(["schema", table_name]), schema.dump())

def client_run_task(cl, sql, config, participants, using_secure_context=True):
    cl.create_entry("query", sql)
    param = {"config": config, "using_secure_context"}
    task_id = cl.run_task("query", json.dumps(param), participants, True)



def start_interactive_client(cl, config, participants):
    print("Start Interactive Mode")
    running_config = {"using_secure_context": True}
    while True:
        command = input("> ")
        if command.find("EXIT") != -1:
            print("Exiting")
            return
        elif command.find("USE") != -1:
            if command.find("INSECURE"):
                using_secure_context = False
            if command.find("SECURE"):
                using_secure_context = True
            result = "Context switched"
        else:
            
            task_id = 

    
if __name__ == "__main__":
    addr = sys.argv[1]
    jwt = sys.argv[2]
    provider_user_id = sys.argv[3] # Later: need to specific provider's name and its corresponding user_id
    config_dir = "./examples/demo/config.json"
    with open(config_dir) as f:
        config = json.load(f)
    cl = CoLink(addr, jwt)
    participants = [CL.Participant(user_id=cl.get_user_id(), role="client"),
                    CL.Participant(user_id=provider_user_id, role="provider")]
    client_setup(cl, config, participants)
    start_interactive_client(cl, config, participants)