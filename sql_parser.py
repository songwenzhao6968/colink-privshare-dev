import json
from enum import Enum

class Predicate:
    def __init__(self, tokens=None, type=None, is_leaf=None, left_child=None, right_child=None):
        if tokens == None:
            self.type = type
            self.is_leaf = is_leaf
            self.left_child = left_child
            self.right_child = right_child
            return
        self.type = tokens[1]
        self.is_leaf = True
        self.concerned_column = tokens[0]
        self.value = tokens[2]
        if self.value[0] != '\"':
            if self.value == "TRUE":
                self.value = True
            elif self.value == "FALSE":
                self.value == False
            else:
                self.value = int(self.value)
        else:
            self.value = self.value.replace('\"', '') 
    
    @staticmethod
    def generate_predicates(tokens):
        cnt_bracket, ever_zero = 0, False
        for i, token in enumerate(tokens):
            if token == '(':
                cnt_bracket += 1
            elif token == ')':
                cnt_bracket -= 1
            if i < len(tokens)-1 and cnt_bracket == 0:
                ever_zero = True
                break
        if not ever_zero: 
            return Predicate.generate_predicates(tokens[1:-1])
        
        for i, token in enumerate(tokens):
            if token == '(':
                cnt_bracket += 1
            elif token == ')':
                cnt_bracket -= 1
            elif cnt_bracket == 0 and (token == "OR" or token == "AND"):
                return Predicate(
                    type = token,
                    is_leaf = False,
                    left_child = Predicate.generate_predicates(tokens[:i]),
                    right_child = Predicate.generate_predicates(tokens[i+1:])
                )
        # Tokens should now be (column, op, value)
        return Predicate(tokens)
        

    def check(self, record, schema):
        if self.type == "AND":
            return self.left_child.check(record) and self.right_child.check(record)
        elif self.type == "OR":
            return self.left_child.check(record) or self.right_child.check(record)
        elif self.type == "<":
            return record[schema.get_id(self.concerned_column)] < self.value
        elif self.type == "<=":
            return record[schema.get_id(self.concerned_column)] <= self.value
        elif self.type == ">":
            return record[schema.get_id(self.concerned_column)] > self.value
        elif self.type == ">=":
            return record[schema.get_id(self.concerned_column)] >= self.value
        elif self.type == "=":
            return record[schema.get_id(self.concerned_column)] == self.value
        elif self.type == "!=":
            return record[schema.get_id(self.concerned_column)] != self.value

    def serialize_to_json(self):
        if self.is_leaf:
            return {
                "is_leaf": self.is_leaf,
                "type": self.type,
                "concerned_column": self.concerned_column,
                "value": self.value
            }
        return {
            "is_leaf": self.is_leaf,
            "type": self.type,
            "children": [
                self.left_child.serialize_to_json(),
                self.right_child.serialize_to_json()
            ]
        }

    @staticmethod
    def deserialize_from_json(pred_json):
        pred = Predicate(type=pred_json["type"], is_leaf=pred_json["is_leaf"])
        if pred.is_leaf:
            pred.concerned_column = pred_json["concerned_column"]
            pred.value = pred_json["value"]
        else:
            pred.left_child = Predicate.deserialize_from_json(pred_json["children"][0])
            pred.right_child = Predicate.deserialize_from_json(pred_json["children"][1])
        return pred
    
class QueryType(Enum):
    RETRIEVE = "retrieve"
    AGGREGATE_EXIST = "aggregate_exist"
    AGGREGATE_CNT = "aggregate_count"
    AGGREGATE_SUM = "aggregate_sum"
    AGGREGATE_AVG = "aggregate_average"
    AGGREGATE_CNT_UNQ = "aggregate_count_unique"

class Query:
    def __init__(self, sql=None, type=None, concerned_column=None, concerned_table=None, pred=None):
        def get_tokens(sql):
            tokens, i, pre = [], 0, 0
            while i < len(sql):
                if sql[i] == '\"':
                    i += 1
                    while sql[i] != '\"':
                        i += 1
                elif sql[i] == ' ':
                    tokens.append(sql[pre:i])
                    pre = i+1
                i += 1
            tokens.append(sql[pre:])
            return [token for token in tokens if token != '']

        if sql == None:
            self.type = type
            self.concerned_column = concerned_column
            self.concerned_table = concerned_table
            self.pred = pred
            return
        # Seperate different parts
        sql = sql.replace('\n', ' ').strip()
        sql = sql.replace('(', ' ( ').replace(')', ' ) ')
        tokens = get_tokens(sql)
        for i, token in enumerate(tokens):
            if token == "SELECT":
                select_l = i+1
            if token == "FROM":
                select_r, from_l = i, i+1
            if token == "WHERE":
                from_r, where_l = i, i+1
        tokens_select = tokens[select_l:select_r]

        if '(' in tokens_select:
            func, column = tokens_select[0], tokens_select[2]
            if func == "EXIST":
                self.type = QueryType.AGGREGATE_EXIST
            elif func == "COUNT":
                self.type = QueryType.AGGREGATE_CNT
            elif func == "SUM":
                self.type = QueryType.AGGREGATE_SUM
            elif func == "AVG":
                self.type = QueryType.AGGREGATE_AVG
            elif func == "COUNT_UNIQUE":
                self.type = QueryType.AGGREGATE_CNT_UNQ
            self.concerned_column = column.replace(')', '')
        else:
            self.type = QueryType.RETRIEVE
            self.concerned_column = []
            for token in tokens_select:
                self.concerned_column.append(token.replace(',', ''))
        self.concerned_table = tokens[from_l:from_r][0]
        self.pred = Predicate.generate_predicates(tokens[where_l:])

    def is_retrieve(self):
        return self.type == QueryType.RETRIEVE

    def is_aggregate(self):
        return self.type.value.find("aggregate") != -1
    
    def serialize_to_json(self):
        return {
            "type": self.type.value,
            "concerned_column": self.concerned_column,
            "concerned_table": self.concerned_table,
            "predicate": self.pred.serialize_to_json()
        }

    @staticmethod
    def deserialize_from_json(query_json):
        return Query(
            type=QueryType(query_json["type"]),
            concerned_column=query_json["concerned_column"],
            concerned_table=query_json["concerned_table"],
            pred=Predicate.deserialize_from_json(query_json["predicate"])
        )
    
    def dump(self):
        return json.dumps(self.serialize_to_json())
    
    @staticmethod
    def from_dump(query_dump):
        return Query.deserialize_from_json(json.loads(query_dump))


if __name__ == "__main__":
    # Test
    sql = "SELECT SUM(deposit) FROM t_deposit WHERE (user_name = \"Robert\" AND id < 16) OR id >= 8"
    query_dump = Query(sql).dump()
    print(query_dump)
    query = Query.from_dump(query_dump)
    print(query.dump())