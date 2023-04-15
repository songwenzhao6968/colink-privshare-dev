import logging
from typing import List
import json
import colink as CL
from colink import CoLink, ProtocolOperator, byte_to_str, byte_to_int
from myutil import read_large_entry
from privshare import he
from privshare.database import Database, Schema, Query
from privshare.secure_database import SecureDatabase, SecureQuery, SecureResult

pop_query = ProtocolOperator(__name__)

@pop_query.handle("query:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    config = json.loads(byte_to_str(param))
    if not config["using_secure_context"]:
        # Send the query to the data provider
        query = Query(byte_to_str(cl.read_entry("query")))
        cl.set_variable("query", query.dump(), participants[1:])
        # Receive query result
        result = cl.get_variable("result", participants[1])
        cl.create_entry("result", result)
    else:
        # Encrypt and send the query to the data provider
        keys_bytes = []
        for i in range(5):
            key_bytes = read_large_entry(cl, ":".join(["key", str(i)]))
            keys_bytes.append(key_bytes)
        HE = he.load_from_bytes(keys_bytes)

        query = Query(byte_to_str(cl.read_entry("query")))
        schema = Schema.from_dump(byte_to_str(cl.read_entry(":".join([query.concerned_table, "schema"]))))
        secure_query = SecureQuery(query, schema, HE, config["compile"], config["debug"])

        secure_query_dump, ciphers_bytes = secure_query.dump()
        cl.set_variable("secure_query", secure_query_dump, participants[1:])
        cl.set_variable("secure_query_n_ciphers", len(ciphers_bytes), participants[1:])
        for i, cipher_bytes in enumerate(ciphers_bytes):
            cl.set_variable(":".join(["secure_query_cipher", str(i)]), cipher_bytes, participants[1:])
        
        # Receive encrypted query result and decrypt
        secure_result_dump = byte_to_str(cl.get_variable("secure_result", participants[1]))
        n_ciphers = byte_to_int(cl.get_variable("secure_result_n_ciphers", participants[1]))
        ciphers_bytes = []
        for i in range(n_ciphers):
            cipher_bytes = cl.get_variable(":".join(["secure_result_cipher", str(i)]), participants[1])
            ciphers_bytes.append(cipher_bytes)
        secure_result = SecureResult.from_dump(secure_result_dump, ciphers_bytes, HE)
        result = secure_result.decrypt(HE)
        cl.create_entry("result", json.dumps(result))

@pop_query.handle("query:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    config = json.loads(byte_to_str(param))
    if not config["using_secure_context"]:
        # Process the query and send the result to the client
        query = Query.from_dump(cl.get_variable("query", participants[0]))
        db = Database.from_dump(byte_to_str(cl.read_entry("database")))
        result = db.process(query)
        cl.set_variable("result", json.dumps(result), [participants[0]])
    else:
        # Process encrypted query and send encrypted result to the client
        keys_bytes = []
        for i in range(4):
            key_bytes = read_large_entry(cl, ":".join(["key", str(i)]))
            keys_bytes.append(key_bytes)
        HE_pub = he.load_public_from_bytes(keys_bytes)

        secure_query_dump = byte_to_str(cl.get_variable("secure_query", participants[0]))
        n_ciphers = byte_to_int(cl.get_variable("secure_query_n_ciphers", participants[0]))
        ciphers_bytes = []
        for i in range(n_ciphers):
            cipher_bytes = cl.get_variable(":".join(["secure_query_cipher", str(i)]), participants[0])
            ciphers_bytes.append(cipher_bytes)
        secure_query = SecureQuery.from_dump(secure_query_dump, ciphers_bytes, HE_pub)

        db = SecureDatabase.from_dump(byte_to_str(cl.read_entry("database")))
        secure_result = db.process(secure_query, HE_pub, config["debug"])

        secure_result_dump, ciphers_bytes = secure_result.dump()
        cl.set_variable("secure_result", secure_result_dump, [participants[0]])
        cl.set_variable("secure_result_n_ciphers", len(ciphers_bytes), [participants[0]])
        for i, cipher_bytes in enumerate(ciphers_bytes):
            cl.set_variable(":".join(["secure_result_cipher", str(i)]), cipher_bytes, [participants[0]])