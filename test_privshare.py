import json
from privshare import myutil, he
from privshare.database import Database, Query
from privshare.secure_database import SecureDatabase, SecureQuery, SecureResult

config_dir = "./examples/demo/config.json"
with open(config_dir) as f:
    config = json.load(f)
debug = config["debug"]

# Client: Set up the keys
HE = he.create_he_object(config["he"])
keys_bytes = he.save_to_bytes(HE)
pub_keys_bytes = he.save_public_to_bytes(HE)

# Client: Encrypt and send the query to the data provider
HE = he.load_from_bytes(keys_bytes)
sql = "SELECT AVG(amount) FROM t_deposit WHERE user_name = \"Daniel\""
query = Query(sql)
with open(config["tables"][query.concerned_table]["schema_file_loc"]) as f:
    null_data_db = Database.deserialize_from_json(json.load(f))
    schema = null_data_db[query.concerned_table].schema
secure_query = SecureQuery(query, schema, HE, config["compile"], debug)
secure_query_dump, ciphers_bytes = secure_query.dump()

if debug["output_secure_execution_tree"]: 
    print(secure_query_dump)

# Data Provider: Process encrypted query
with open(config["servers"]["provider_1"]["database_file_loc"]) as f:
    db = SecureDatabase.deserialize_from_json(json.load(f))
HE_pub = he.load_public_from_bytes(pub_keys_bytes)
secure_query = SecureQuery.from_dump(secure_query_dump, ciphers_bytes, HE_pub)
secure_result = db.process(secure_query, HE_pub, debug)
secure_result_dump, ciphers_bytes = secure_result.dump()

# Client: Receive encrypted query result and decrypt
secure_result = SecureResult.from_dump(secure_result_dump, ciphers_bytes, HE)
result = secure_result.decrypt(HE)
print("Result:", result)

if debug["timing"]:
    myutil.write_event_times()