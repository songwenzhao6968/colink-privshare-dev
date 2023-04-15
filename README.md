# CoLink Privshare Dev

Distributed private database management system built with CoLink.

## Requirements

+ Python 3.9
+ Pyfhel 3.4.1
+ CoLink 0.3.4

## Usage

Create two new terminals and start protocol operators for the client and the server separately.

```shell
python protocol_key_setup.py --addr <addr> --jwt <user_jwt>
python protocol_query.py --addr <addr> --jwt <user_jwt>
```

Server runs a script to read and upload the database.

```shell
python provider.py <addr> <user_jwt>
```

Client runs a script that create an interactive interface to read a query and issue the query with a task.

```shell
python client.py <addr> <user_jwt> \
    <server user_id>
# Example query: SELECT AVG(amount) FROM t_deposit WHERE user_name = "Daniel"
```

### Instant Server Simulation

```shell
python run_with_instant_server.py
```