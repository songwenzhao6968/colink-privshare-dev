from typing import List
import json
import colink as CL
from colink import (
    CoLink, 
    byte_to_str,
    ProtocolOperator
)
import he

pop = ProtocolOperator(__name__)

@pop.handle("key_setup:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    param = json.loads(byte_to_str(param))
    HE = he.create_he_object(param["config"])
    keys_bytes = he.save_to_bytes(HE)
    for i, key_bytes in enumerate(keys_bytes):
        cl.create_entry(":".join(["key", str(i)]), key_bytes)
    pub_keys_bytes = he.save_public_to_bytes(HE)
    for i, key_bytes in enumerate(pub_keys_bytes):
        cl.set_variable(":".join(["key", str(i)]), key_bytes, [participants[1]])

@pop.handle("key_setup:provider")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    for i in range(4):
        key_bytes = cl.get_variable(":".join(["key", str(i)]), participants[0])
        cl.create_entry(":".join(["key", str(i)]), key_bytes)