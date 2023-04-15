from typing import List
import colink as CL
from colink import CoLink, ProtocolOperator, InstantServer, InstantRegistry

pop = ProtocolOperator(__name__)

@pop.handle("key_setup:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    keys_bytes = bytes([1]*121), bytes([1]*1028581), bytes([1]*2057011)
    for i, key_bytes in enumerate(keys_bytes):
        print(len(key_bytes))
        if i < 3:
            cl.create_entry(":".join(["key", str(i)]), key_bytes)

@pop.handle("key_setup:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    pass

if __name__ == "__main__":
    ir = InstantRegistry()
    is_c = InstantServer()
    is_p = InstantServer()
    cl_c = is_c.get_colink().switch_to_generated_user()
    cl_p = is_p.get_colink().switch_to_generated_user()
    pop.run_attach(cl_c)
    pop.run_attach(cl_p)

    x = bytes([1]*121)
    cl_c.create_entry("try", x)
    cl_c.delete_entry("try")
    cl_c.create_entry("try", x)
    cl_c.delete_entry("try")
