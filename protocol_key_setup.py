from typing import List
import json
import colink as CL
from colink import CoLink, ProtocolOperator, byte_to_str
from myutil import create_large_entry
from privshare import he

pop_key_setup = ProtocolOperator(__name__)

@pop_key_setup.handle("key_setup:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    config = json.loads(byte_to_str(param))
    HE = he.create_he_object(config)
    keys_bytes = he.save_to_bytes(HE)
    for i, key_bytes in enumerate(keys_bytes):
        create_large_entry(cl, ":".join(["key", str(i)]), key_bytes)
    pub_keys_bytes = he.save_public_to_bytes(HE)
    for i, key_bytes in enumerate(pub_keys_bytes):
        cl.set_variable(":".join(["key", str(i)]), key_bytes, [participants[1]])
    cl.get_variable("key_saved", participants[1])

@pop_key_setup.handle("key_setup:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    for i in range(4):
        key_bytes = cl.get_variable(":".join(["key", str(i)]), participants[0])
        create_large_entry(cl, ":".join(["key", str(i)]), key_bytes)
    cl.set_variable("key_saved", b"\x01", [participants[0]])