import json
from enum import Enum
from privshare import myutil
import numpy as np
from privshare import he
from privshare.database import DataType, Query, QueryType, Predicate
from privshare.secure_database import SecureResult

class NodeType(Enum):
    RETURN = "return"
    RETRIEVAL = "retrieval"
    AGGREGATION = "aggregation"
    AND = "and"
    OR = "or"
    NOT = "not"
    EQUAL = "equal"
    RANGE = "range"
    BASIC = "basic"

class ComputationNode():
    def __init__(self, type=None):
        self.type = type
        self.parent = []
        self.children = []

    def link(self, child, reset_parent=True):
        self.children.append(child)
        if reset_parent: 
            child.parent = [self]
        else:
            child.parent.append(self)

    def serialize_to_json(self, expand=True):
        children_json = []
        if expand:
            for child in self.children:
                children_json.append(child.serialize_to_json())
        return {
            "type": self.type.value,
            "children": children_json
            }

    @staticmethod
    def deserialize_from_json(node_json, expand=True): 
        type = NodeType(node_json["type"])
        if type == NodeType.RETURN:
            node = ReturnNode(node_json["concerned_table"])
        elif type == NodeType.RETRIEVAL:
            node = RetrievalNode(node_json["concerned_columns"])
        elif type == NodeType.AGGREGATION:
            node = AggregationNode(QueryType(node_json["agg_type"]), node_json["concerned_column"])
        elif type == NodeType.AND:
            node = AndNode()
        elif type == NodeType.OR:
            node = OrNode()
        elif type == NodeType.NOT:
            node = NotNode()
        elif type == NodeType.EQUAL:
            node = EqualNode(
                concerned_column=node_json["concerned_column"],
                bit_width=node_json["bit_width"],
                need_int_to_uint_conversion=node_json["need_int_to_uint_conversion"],
                need_str_to_uint_conversion=node_json["need_str_to_uint_conversion"],
                value=node_json["value"],
            )
        elif type == NodeType.RANGE:
            node = RangeNode(
                concerned_column=node_json["concerned_column"],
                bit_width=node_json["bit_width"],
                need_int_to_uint_conversion=node_json["need_int_to_uint_conversion"],
                value_l=node_json["value_l"],
                value_r=node_json["value_r"]
            )
        elif type == NodeType.BASIC:
            node = MatchBitsNode(
                need_int_to_uint_conversion=node_json["need_int_to_uint_conversion"],
                need_str_to_uint_conversion=node_json["need_str_to_uint_conversion"],
                concerned_column=node_json["concerned_column"],
                offset=node_json["offset"],
                values=node_json["values"],
                mapping_cipher_id=node_json["mapping_cipher_id"],
                mapping_cipher_offset=node_json["mapping_cipher_offset"]
            )
        if expand:
            for child_json in node_json["children"]:
                node.link(ComputationNode.deserialize_from_json(child_json))
        return node
    
    def dump(self):
        return json.dumps(self.serialize_to_json())
    
    @staticmethod
    def from_dump(node_dump):
        return ComputationNode.deserialize_from_json(json.loads(node_dump))

class ReturnNode(ComputationNode):
    def __init__(self, concerned_table):
        super().__init__(NodeType.RETURN)
        self.concerned_table = concerned_table
        
    def serialize_to_json(self, expand=True):
        ret = super().serialize_to_json(expand)
        ret["concerned_table"] = self.concerned_table
        return ret
    
    def process(self, db, mapping_ciphers, HE, debug):
        return self.children[0].process(db[self.concerned_table], mapping_ciphers, HE, debug)

class RetrievalNode(ComputationNode):
    def __init__(self, concerned_columns):
        super().__init__(NodeType.RETRIEVAL)
        self.concerned_columns = concerned_columns
    
    def serialize_to_json(self, expand=True):
        ret = super().serialize_to_json(expand)
        ret["concerned_columns"] = self.concerned_columns
        return ret
    
    def process(self, table, mapping_ciphers, HE, debug):
        return self.children[0].process(table, mapping_ciphers, HE, debug)

class AggregationNode(ComputationNode):
    def __init__(self, agg_type: QueryType, concerned_column):
        super().__init__(NodeType.AGGREGATION)
        self.agg_type = agg_type
        self.concerned_column = concerned_column
    
    def serialize_to_json(self, expand=True):
        ret = super().serialize_to_json(expand)
        ret["agg_type"] = self.agg_type.value
        ret["concerned_column"] = self.concerned_column
        return ret
    
    def process(self, table, mapping_ciphers, HE, debug):
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - Indicator Vector Gen (per record)", 0)
        ind_ciphers = self.children[0].process(table, mapping_ciphers, HE, debug)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - Indicator Vector Gen (per record)", 1, len(ind_ciphers)*HE.n)
            myutil.report_time("Secure Query Execution - Aggregation (per record)", 0)
        if self.agg_type == QueryType.AGGREGATE_CNT:
            result_cipher = HE.encryptInt(np.zeros(HE.n, dtype=np.int64))
            for ind_cipher in ind_ciphers:
                result_cipher += he.sum_cipher(ind_cipher, HE)
            return SecureResult(1, QueryType.AGGREGATE_CNT, result_cipher)
        elif self.agg_type == QueryType.AGGREGATE_SUM:
            concerned_column_id = table.schema.get_id(self.concerned_column)
            column, columns = [], []
            for record in table.data:
                value = record[concerned_column_id]
                column.append(value)
                if len(column) == HE.n:
                    columns.append(np.array(column, dtype=np.int64))
                    column = []
            if column:
                columns.append(np.array(column, dtype=np.int64))
            result_cipher = HE.encryptInt(np.zeros(HE.n, dtype=np.int64))
            for ind_cipher, column in zip(ind_ciphers, columns):
                ind_cipher *= HE.encodeInt(column) # Potential bug here: overflow
                result_cipher += he.sum_cipher(ind_cipher, HE)
            return SecureResult(1, QueryType.AGGREGATE_SUM, result_cipher)
        elif self.agg_type == QueryType.AGGREGATE_AVG:
            concerned_column_id = table.schema.get_id(self.concerned_column)
            column, columns = [], []
            for record in table.data:
                value = record[concerned_column_id]
                column.append(value)
                if len(column) == HE.n:
                    columns.append(np.array(column, dtype=np.int64))
                    column = []
            if column:
                columns.append(np.array(column, dtype=np.int64))
            result_cnt_cipher = HE.encryptInt(np.zeros(HE.n, dtype=np.int64))
            result_sum_cipher = HE.encryptInt(np.zeros(HE.n, dtype=np.int64))
            for ind_cipher, column in zip(ind_ciphers, columns):
                tmp_cipher = ind_cipher * HE.encodeInt(column)
                result_cnt_cipher += he.sum_cipher(ind_cipher, HE)
                result_sum_cipher += he.sum_cipher(tmp_cipher, HE)
            mask = np.array([1, 0], dtype=np.int64)
            result_cnt_cipher *= HE.encodeInt(mask)
            mask = np.array([0, 1], dtype=np.int64)
            result_sum_cipher *= HE.encodeInt(mask)
            result_cipher = HE.add(result_sum_cipher, result_cnt_cipher)
            return SecureResult(2, QueryType.AGGREGATE_AVG, result_cipher)
        else:
            raise NotImplementedError

class AndNode(ComputationNode):
    def __init__(self):
        super().__init__(NodeType.AND)
    
    def process(self, table, mapping_ciphers, HE, debug):
        ind_ciphers_a = self.children[0].process(table, mapping_ciphers, HE, debug)
        ind_ciphers_b = self.children[1].process(table, mapping_ciphers, HE, debug)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - AND (per record)", 0)
        ind_ciphers = []
        for ind_cipher_a, ind_cipher_b in zip(ind_ciphers_a, ind_ciphers_b):
            ind_cipher_a *= ind_cipher_b
            ~ind_cipher_a
            ind_ciphers.append(ind_cipher_a)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - AND (per record)", 1, len(ind_ciphers)*HE.n)
        return ind_ciphers

class OrNode(ComputationNode):
    def __init__(self):
        super().__init__(NodeType.OR)

    def process(self, table, mapping_ciphers, HE, debug):
        ind_ciphers_a = self.children[0].process(table, mapping_ciphers, HE, debug)
        ind_ciphers_b = self.children[1].process(table, mapping_ciphers, HE, debug)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - OR (per record)", 0)
        ind_ciphers = []
        for ind_cipher_a, ind_cipher_b in zip(ind_ciphers_a, ind_ciphers_b):
            ind_cipher_c = ind_cipher_a * ind_cipher_b
            ~ind_cipher_c
            ind_cipher_a += ind_cipher_b
            ind_cipher_a -= ind_cipher_c
            ind_ciphers.append(ind_cipher_a)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - OR (per record)", 1, len(ind_ciphers)*HE.n)
        return ind_ciphers

class NotNode(ComputationNode):
    def __init__(self):
        super().__init__(NodeType.NOT)
    
    def process(self, table, mapping_ciphers, HE, debug):
        ind_ciphers = self.children[0].process(table, mapping_ciphers, HE, debug)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - NOT (per record)", 0)
        for ind_cipher in ind_ciphers:
            ind_cipher = HE.negate(ind_cipher, False)
            ind_cipher += HE.encodeInt(np.ones(HE.n, dtype=np.int64))
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - NOT (per record)", 1, len(ind_ciphers)*HE.n)
        return ind_ciphers

class EqualNode(ComputationNode):
    def __init__(self, concerned_column, value, schema=None,
                 bit_width=None, need_int_to_uint_conversion=None, need_str_to_uint_conversion=None): 
        super().__init__(NodeType.EQUAL)
        if schema == None:
            self.concerned_column = concerned_column
            self.bit_width = bit_width
            self.need_int_to_uint_conversion = need_int_to_uint_conversion
            self.need_str_to_uint_conversion = need_str_to_uint_conversion
            self.value = value
            return
        self.concerned_column = concerned_column
        dtype = schema.get_type(concerned_column)
        temp = { # DataType: (bit_width, need_int_to_uint_conversion, need_str_to_uint_conversion)
            DataType.UINT8: (8, False, False),
            DataType.UINT16: (16, False, False),
            DataType.UINT32: (32, False, False),
            DataType.INT8: (8, True, False),
            DataType.INT16: (16, True, False),
            DataType.INT32: (32, True, False),
            DataType.STR: (32, False, True),
        }
        self.bit_width, self.need_int_to_uint_conversion, self.need_str_to_uint_conversion = temp[dtype]
        if self.need_str_to_uint_conversion:
            self.value = myutil.str_to_uint(value)
        elif self.need_int_to_uint_conversion:
            self.value = myutil.int_to_uint(value)
        else:
            self.value = value

    def serialize_to_json(self, expand=True):
        ret = super().serialize_to_json(expand)
        ret["concerned_column"] = self.concerned_column
        ret["bit_width"] = self.bit_width
        ret["need_int_to_uint_conversion"] = self.need_int_to_uint_conversion
        ret["need_str_to_uint_conversion"] = self.need_str_to_uint_conversion
        ret["value"] = self.value
        return ret
        
class RangeNode(ComputationNode):
    def __init__(self, concerned_column, value=None, schema=None, pred_type=None,
                bit_width=None, need_int_to_uint_conversion=None, value_l=None, value_r=None):
        super().__init__(NodeType.RANGE)
        if schema == None:
            self.concerned_column = concerned_column
            self.bit_width = bit_width
            self.need_int_to_uint_conversion = need_int_to_uint_conversion
            self.value_l = value_l
            self.value_r = value_r
            return
        self.concerned_column = concerned_column
        dtype = schema.get_type(concerned_column)
        assert dtype != DataType.STR
        temp = { # DataType: (bit_width, need_int_to_uint_conversion)
            DataType.UINT8: (8, False),
            DataType.UINT16: (16, False),
            DataType.UINT32: (32, False),
            DataType.INT8: (8, True),
            DataType.INT16: (16, True),
            DataType.INT32: (32, True),
        }
        self.bit_width, self.need_int_to_uint_conversion = temp[dtype]
        if self.need_int_to_uint_conversion:
            value = myutil.int_to_uint(value)
        _min, _max = 0, (1 << self.bit_width) - 1
        temp = { # PredicateType: (value_l, value_r)
            "<": (_min, value-1), 
            "<=": (_min, value),
            ">": (value+1, _max),
            ">=": (value, _max)
        }
        self.value_l, self.value_r = temp[pred_type]

    def serialize_to_json(self, expand=True):
        ret = super().serialize_to_json(expand)
        ret["concerned_column"] = self.concerned_column
        ret["bit_width"] = self.bit_width
        ret["need_int_to_uint_conversion"] = self.need_int_to_uint_conversion
        ret["value_l"] = self.value_l
        ret["value_r"] = self.value_r
        return ret
        
class MatchBitsNode(ComputationNode):
    bit_width = 8
    def __init__(self, need_int_to_uint_conversion, need_str_to_uint_conversion, 
                    concerned_column, offset, values=None, mapping_cipher_id=None, mapping_cipher_offset=None):
        super().__init__(NodeType.BASIC)
        self.need_int_to_uint_conversion = need_int_to_uint_conversion
        self.need_str_to_uint_conversion = need_str_to_uint_conversion
        self.concerned_column = concerned_column
        self.offset = offset
        self.values = values
        self.mapping_cipher_id = mapping_cipher_id
        self.mapping_cipher_offset = mapping_cipher_offset
        self.ind_ciphers = None # May be reused when processing

    @staticmethod
    def decompose_equal(node_eq: EqualNode, offset):
        node_mb = MatchBitsNode(
            need_int_to_uint_conversion = node_eq.need_int_to_uint_conversion,
            need_str_to_uint_conversion = node_eq.need_str_to_uint_conversion,
            concerned_column = node_eq.concerned_column,
            offset = offset
        )
        node_mb.values = [(node_eq.value >> offset) & ((1 << MatchBitsNode.bit_width) - 1)]
        return node_mb

    @staticmethod
    def decompose_range(node_rg: RangeNode, offset, is_equal, keep_left, keep_right, is_left_strict=False, is_right_strict=False):
        node_mb = MatchBitsNode(
            need_int_to_uint_conversion = node_rg.need_int_to_uint_conversion,
            need_str_to_uint_conversion = False, 
            concerned_column = node_rg.concerned_column,
            offset = offset
        )
        if is_equal:
            value = node_rg.value_l if keep_left else node_rg.value_r
            node_mb.values = [(value >> offset) & ((1 << MatchBitsNode.bit_width) - 1)]
        else:
            _min, _max = 0, (1 << node_rg.bit_width) - 1
            value_l = node_rg.value_l if keep_left else _min
            value_r = node_rg.value_r if keep_right else _max
            if value_l <= value_r: 
                value_l = (value_l >> offset) & ((1 << MatchBitsNode.bit_width) - 1)
                if is_left_strict:
                    value_l += 1
                value_r = (value_r >> offset) & ((1 << MatchBitsNode.bit_width) - 1)
                if is_right_strict:
                    value_r -= 1
                node_mb.values = [value for value in range(value_l, value_r+1)]
            else:
                node_mb.values = []
        return node_mb

    def generate_mapping(self):
        mapping = [0] * (1 << MatchBitsNode.bit_width)
        for value in self.values:
            mapping[value] = 1
        return mapping

    def serialize_to_json(self, expand=True):
        ret = super().serialize_to_json(expand)
        ret["need_int_to_uint_conversion"] = self.need_int_to_uint_conversion
        ret["need_str_to_uint_conversion"] = self.need_str_to_uint_conversion
        ret["concerned_column"] = self.concerned_column
        ret["offset"] = self.offset
        ret["values"] = self.values
        ret["mapping_cipher_id"] = self.mapping_cipher_id
        ret["mapping_cipher_offset"] = self.mapping_cipher_offset
        return ret
    
    def process(self, table, mapping_ciphers, HE, debug):
        if self.ind_ciphers != None:
            return he.copy_cipher_list(self.ind_ciphers)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - BASIC (per record)", 0)
        concerned_column_id = table.schema.get_id(self.concerned_column)
        column, ind_ciphers = [], []
        for record in table.data:
            value = record[concerned_column_id]
            if self.need_str_to_uint_conversion:
                value = myutil.str_to_uint(value)
            elif self.need_int_to_uint_conversion:
                value = myutil.int_to_uint(value)
            value = (value >> self.offset) & ((1 << MatchBitsNode.bit_width) - 1)
            column.append(value)
            if len(column) == HE.n:
                x = np.array(column, dtype=np.int64)
                y = he.apply_elementwise_mapping(mapping_ciphers[self.mapping_cipher_id], self.mapping_cipher_offset, 
                                             MatchBitsNode.bit_width, x, HE)
                ind_ciphers.append(y)
                column = []
        if column:
            x = np.array(column, dtype=np.int64)
            y = he.apply_elementwise_mapping(mapping_ciphers[self.mapping_cipher_id], self.mapping_cipher_offset, 
                                             MatchBitsNode.bit_width, x, HE)
            ind_ciphers.append(y)
        if debug["timing"]: 
            myutil.report_time("Secure Query Execution - BASIC (per record)", 1, len(ind_ciphers)*HE.n)
        if len(self.parent) > 1:
            self.ind_ciphers = he.copy_cipher_list(ind_ciphers)
        return ind_ciphers

class ExecutionTree():
    def __init__(self, query: Query=None, schema=None, root=None):
        if query == None:
            self.root = root
            return
        self.root = ReturnNode(query.concerned_table)
        if query.is_retrieve():
            node_op = RetrievalNode(query.concerned_columns)
        elif query.is_aggregate():
            node_op = AggregationNode(query.type, query.concerned_column)
        self.root.link(node_op)

        def add_predicates(pred: Predicate, schema):
            if pred.type == "AND":
                node = AndNode()
            elif pred.type == "OR":
                node = OrNode()
            elif pred.type == "=":
                node = EqualNode(pred.concerned_column, pred.value, schema)
            elif pred.type == "!=":
                node_eq = EqualNode(pred.concerned_column, pred.value, schema)
                node = NotNode()
                node.link(node_eq)
            elif pred.type in ["<", "<=", ">", ">="]:
                node = RangeNode(pred.concerned_column, pred.value, schema, pred.type)
            if not pred.is_leaf:
                node.link(add_predicates(pred.left_child, schema))
                node.link(add_predicates(pred.right_child, schema))
            return node
        
        node_op.link(add_predicates(query.pred, schema))
    
    def get_query_type(self):
        if self.root.children[0].type == NodeType.RETRIEVAL:
            return QueryType.RETRIEVE
        elif self.root.children[0].type == NodeType.AGGREGATION:
            return self.root.children[0].agg_type
    
    def serialize_to_json(self):
        return {"root": self.root.serialize_to_json()}
    
    @staticmethod
    def deserialize_from_json(exe_tree_json):
        return ExecutionTree(root=ComputationNode.deserialize_from_json(exe_tree_json["root"]))
    
    def dump(self):
        return json.dumps(self.serialize_to_json())
    
    @staticmethod
    def from_dump(exe_tree_dump):
        return ExecutionTree.deserialize_from_json(json.loads(exe_tree_dump))
    
    def process(self, db, mapping_ciphers, HE, debug):
        return self.root.process(db, mapping_ciphers, HE, debug)