import logging
from typing import List
from collections import Counter
import json
from sql_parser import Query
import colink as CL
from colink import (
    CoLink, 
    byte_to_str,
    ProtocolOperator
)

pop = ProtocolOperator(__name__)

@pop.handle("query:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    def merge_results(results, query): # Note that both the input and the output results are strings
        if query.is_retrieve():
            merged_results = []
            for result in results:
                merged_results += json.loads(result)
            return json.dumps(merged_results)
        elif query.is_aggregate_bool():
            total = False
            for result in results:
                total |= json.loads(result)
            return json.dumps(total)
        elif query.is_aggregate():
            total = 0
            for result in results:
                total += json.loads(result)
            return json.dumps(total)

    logging.info(f"query:client protocol operator! {cl.jwt}")

    # Initiate the query
    sql = byte_to_str(cl.read_entry(byte_to_str(param)))
    query = Query(sql)
    cl.set_variable("query", bytes(query.dumps(), 'utf-8'), participants[1:])
    
    # Receive query results
    results = []
    for participant in participants[1:]:
        result = byte_to_str(cl.get_variable("result", participant))
        if result != "Table not found.":
            results.append(result)
    cl.create_entry("output", merge_results(results, query))


@pop.handle("query:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    def run_query(table, schema, query):
        schema_name, schema_type = schema
        if query.type == "Q_RETRIEVE":
            result = []
            for row in table:
                record = {}
                for i, value in enumerate(row):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    result.append([record[col] for col in query.concerned_column])
        elif query.type in ["Q_AGGREGATE_EXIST", "Q_AGGREGATE_CNT", "Q_AGGREGATE_SUM", "Q_AGGREGATE_AVG"]:
            sum, cnt = 0, 0
            for row in table:
                record = {"*": 0}
                for i, value in enumerate(row):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    sum += record[query.concerned_column]
                    cnt += 1
            if query.type == "Q_AGGREGATE_EXIST":
                result = cnt > 0
            elif query.type == "Q_AGGREGATE_CNT":
                result = cnt
            elif query.type == "Q_AGGREGATE_SUM":
                result = sum
            elif query.type == "Q_AGGREGATE_AVG":
                result = int(sum/cnt)
        elif query.type == "Q_AGGREGATE_CNT_UNQ":
            result_list = []
            for row in table:
                record = {}
                for i, value in enumerate(row):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    result_list.append(record[query.concerned_column])
            result = len(Counter(result_list).keys())
        return json.dumps(result)

    logging.info(f"query:client protocol operator! {cl.jwt}")

    query_str = byte_to_str(cl.get_variable("query", participants[0]))
    query = Query.from_json(json.loads(query_str))
    table_keys = cl.read_keys(":".join([f"{cl.get_user_id()}:", "database", query.concerned_table, "data"]), False)
    if not table_keys:
        cl.set_variable("result", bytes("Table not found.", 'utf-8'), [participants[0]])
        return
    table = []
    for key in table_keys:
        table.append(json.loads(byte_to_str(cl.read_entry(key.key_path))))
    schema_type = cl.read_entry(":".join(["database", query.concerned_table, "schema", "type"]))
    schema_name = cl.read_entry(":".join(["database", query.concerned_table, "schema", "name"]))
    schema_type = json.loads(byte_to_str(schema_type))
    schema_name = json.loads(byte_to_str(schema_name))

    result = run_query(table, (schema_name, schema_type), query)
    cl.set_variable("result", bytes(result, 'utf-8'), [participants[0]])