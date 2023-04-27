import json
import colink as CL
from colink import InstantServer, InstantRegistry
from protocol_query import pop
from provider import provider_setup
from client import client_setup, start_interactive_client

config_file_loc = "./examples/demo/config.json"
with open(config_file_loc) as f:
    config = json.load(f)

provider_names = ["provider_1"]
ir = InstantRegistry()
is_c = InstantServer()
is_p = InstantServer()
cl_c = is_c.get_colink().switch_to_generated_user()
cl_p = is_p.get_colink().switch_to_generated_user()
pop.run_attach(cl_c)
pop.run_attach(cl_p)

provider_setup(cl_p, config["servers"]["provider_1"])

participants = [CL.Participant(user_id=cl_c.get_user_id(), role="client"),
                CL.Participant(user_id=cl_p.get_user_id(), role="provider")]
client_setup(cl_c, config, provider_names, participants)
start_interactive_client(cl_c, participants, config)