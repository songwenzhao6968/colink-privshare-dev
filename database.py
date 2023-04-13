import json
from enum import Enum

class DataType(Enum):
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    STR = "str" 

class Schema:
    def __init__(self, fields, key):
        self.fields = fields
        self.key = key

    def get_id(self, name):
        return self.fields[name][0]
    
    def get_type(self, name):
        return self.fields[name][1]

    def serialize_to_json(self):
        fields = [None] * len(self.fields)
        for name, (i, type) in self.fields.items():
            fields[i] = name, type.value
        return {
            "fields": fields,
            "key": self.key
        }

    @staticmethod
    def deserialize_from_json(schema_json):
        fields = {}
        for i, (name, type) in enumerate(schema_json["fields"]):
            fields[name] = i, DataType(type)
        return Schema(fields, schema_json["key"])

    def dump(self):
        return json.dumps(self.serialize_to_json())

    @staticmethod
    def from_dump(schema_dump):
        return Schema.deserialize_from_json(json.loads(schema_dump))

class Table:
    def __init__(self, schema, data):
        self.schema = schema
        self.data = data
    
    def serialize_to_json(self):
        return {
            "schema": self.schema.serialize_to_json(),
            "data": self.data
        }
    
    @staticmethod
    def deserialize_from_json(table_json):
        return Table(Schema.deserialize_from_json(table_json["schema"]),
                     table_json["data"])

class DataBase:
    def __init__(self, tables):
        self.tables = tables

    def __getitem__(self, table_name):
        return self.tables[table_name]
    
    def serialize_to_json(self):
        ret = {}
        for table_name, table in self.tables.items():
            ret[table_name] = table.serialize_to_json()
        return ret
    
    @staticmethod
    def deserialize_from_json(db_json):
        tables = {}
        for table_name, table_json in db_json.items():
            tables[table_name] = Table.deserialize_from_json(table_json)
        return DataBase(tables)
    
    def dump(self):
        return json.dumps(self.serialize_to_json())
    
    @staticmethod
    def from_dump(db_dump):
        return DataBase.deserialize_from_json(json.loads(db_dump))

if __name__ == "__main__":
    # Test
    with open("./examples/demo/provider_1/db.json") as f:
        db = DataBase.deserialize_from_json(json.load(f)) 
    print(db.dump())