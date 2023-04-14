import json
from privshare import myutil
import numpy as np
from privshare.he import PyCtxt
from privshare.database import Database, Table, Query, QueryType

class SecureResult():
    def __init__(self, valid_slot_num, query_type, result_cipher=None):
        self.valid_slot_num = valid_slot_num
        self.query_type = query_type
        self.result_cipher = result_cipher

    def serialize_to_json(self):
        return {
            "result_cipher": 0,
            "valid_slot_num": self.valid_slot_num,
            "query_type": self.query_type.value
        }
    
    def dump(self):
        ciphers_bytes = [self.result_cipher.to_bytes()]
        return json.dumps(self.serialize_to_json()), ciphers_bytes
    
    @staticmethod
    def from_dump(secure_result_dump, ciphers_bytes, HE):
        secure_result_json = json.loads(secure_result_dump)
        secure_result = SecureResult(secure_result_json["valid_slot_num"], 
                                     QueryType(secure_result_json["query_type"]))
        cipher = PyCtxt(pyfhel=HE)
        cipher.from_bytes(ciphers_bytes[secure_result_json["result_cipher"]])
        secure_result.result_cipher = cipher
        return secure_result

    def decrypt(self, HE):
        result = HE.decryptInt(self.result_cipher)
        if self.query_type == QueryType.AGGREGATE_AVG:
            return result[1]/result[0]
        else:
            return result[0]

from privshare.execution import ExecutionTree, MatchBitsNode, NodeType
from privshare.execution_pass import Pass

class SecureQuery():
    def __init__(self, query: Query=None, schema=None, HE=None, config=None, debug=None,
                 exe_tree: ExecutionTree=None, mapping_ciphers=None):
        if query == None:
            self.exe_tree = exe_tree
            self.mapping_ciphers = mapping_ciphers
            return
        # Run Pass to transform the execution tree
        if debug["timing"]: 
            myutil.report_time("Secure Query Construction - Transformation", 0)
        assert MatchBitsNode.bit_width == config["basic_block_bit_width"]
        exe_tree = ExecutionTree(query, schema)
        exe_tree = Pass.merge_range(exe_tree)
        exe_tree = Pass.decompose_equal(exe_tree)
        exe_tree = Pass.decompose_range(exe_tree)
        exe_tree = Pass.remove_or(exe_tree)
        if debug["timing"]: 
            myutil.report_time("Secure Query Construction - Transformation", 1)
        # Encrypt the execution tree
        if debug["timing"]: 
            myutil.report_time("Secure Query Construction - Encryption", 0)
        mappings = []
        def group_mappings(node):
            if node.type == NodeType.BASIC:
                if not mappings or len(mappings[-1]) == HE.n:
                    mappings.append([])
                node.mapping_cipher_id = len(mappings) - 1
                node.mapping_cipher_offset = len(mappings[-1])
                mappings[-1] += node.generate_mapping()
                node.values = None
                return
            for child in node.children:
                group_mappings(child)

        group_mappings(exe_tree.root)
        self.exe_tree = exe_tree
        self.mapping_ciphers = []
        for mapping in mappings:
            mapping = np.array(mapping, dtype=np.int64)
            self.mapping_ciphers.append(HE.encryptInt(mapping))
        if debug["timing"]: 
            myutil.report_time("Secure Query Construction - Encryption", 1)

    def get_query_type(self):
        return self.exe_tree.get_query_type()

    def serialize_to_json(self):
        return {
            "exe_tree": self.exe_tree.serialize_to_json(),
            "mapping_ciphers": list(range(len(self.mapping_ciphers)))
        }

    def dump(self): # Return type: str, List[bytes]
        ciphers_bytes = [cipher.to_bytes() 
                         for cipher in self.mapping_ciphers]
        return json.dumps(self.serialize_to_json()), ciphers_bytes
    
    @staticmethod
    def from_dump(secure_query_dump, ciphers_bytes, HE):
        secure_query_json = json.loads(secure_query_dump)
        secure_query = SecureQuery(exe_tree=ExecutionTree.deserialize_from_json(secure_query_json["exe_tree"]))
        secure_query.mapping_ciphers = []
        for id in secure_query_json["mapping_ciphers"]:
            cipher = PyCtxt(pyfhel=HE)
            cipher.from_bytes(ciphers_bytes[id])
            secure_query.mapping_ciphers.append(cipher)
        return secure_query

class SecureDatabase(Database):
    def process(self, secure_query: SecureQuery, HE, debug):
        return secure_query.exe_tree.process(self, secure_query.mapping_ciphers, HE, debug)
    
    @staticmethod
    def deserialize_from_json(db_json):
        tables = {}
        for table_name, table_json in db_json.items():
            tables[table_name] = Table.deserialize_from_json(table_json)
        return SecureDatabase(tables)
    
    @staticmethod
    def from_dump(db_dump):
        return SecureDatabase.deserialize_from_json(json.loads(db_dump))