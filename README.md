# Availability

An availability monitor for websites that submits metrics to a Kafka topic and
loads them into a Postgres database.

![Python package](https://github.com/chornsby/availability/workflows/Python%20package/badge.svg)

## Development setup

As a prerequisite you will need to install
[Docker Compose](https://docs.docker.com/compose/install/) and
[Tox](https://tox.readthedocs.io/en/latest/install.html).

To run the Kafka and Postgres services locally:

```sh
docker-compose up -d
```

To set up a virtual environment for the Python code:

```sh
tox -e venv
```

The Python code relies on environment variables to know where to find the Kafka
and Postgres services.

```sh
# Modify these if you would like to run the code against a remote service
export KAFKA_BROKERS=localhost:9092
export POSTGRES_URI=postgres://postgres:donotuseinproduction123@localhost:5432
```

## Development

To run tests locally:

```sh
tox
```

To run the website monitor component:

```sh
source .venv/bin/activate
availability-monitor example.tsv
```

To run the database writer component (in another terminal session):

```sh
source .venv/bin/activate
availability-writer
```

Note that if you wish to run the code using remote services you will need to
update the environment variables and potentially pass additional configuration
parameters to the scripts.

For example, if your Kafka service requires keyfile authentication then that can
be passed via environment variable or CLI option. These options can be found by
passing the `--help` option to the scripts.

e.g.

```sh
availability-monitor \
  example.tsv \
  --brokers <KAFKA_URL> \
  --security-protocol SSL \
  --ssl-cafile ca.pem \
  --ssl-certfile service.cert \
  --ssl-keyfile service.key

availability-writer \
  --brokers <KAFKA_URI> \
  --security-protocol SSL \
  --ssl-cafile ca.pem \
  --ssl-certfile service.cert \
  --ssl-keyfile service.key \
  --db-uri <POSTGRES_URI>
```

### Dependencies

Python dependencies can be managed using the `.in` files in the `requirements/`
folder. These define the direct dependencies of the project and then the `.txt`
lock files are constructed from them using
[pip-tools](https://github.com/jazzband/pip-tools).

### CI

Tests run automatically and calculate code coverage on push using GitHub
Actions.

### Known issues

The `checks` table in Postgres will quickly grow large if many websites are
checked with a high frequency. This could be improved by using table
partitioning by date or by truncating or summarising old data as it becomes less
relevant.

Tests require running services for integration style tests which can be slow to
run and harder to make reliable.

## Acknowledgements

- https://stackoverflow.com/questions/51398235/specify-ssl-details-for-kafka-brokers-in-python
- https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
- https://blog.ionelmc.ro/2014/05/25/python-packaging/
- https://www.caktusgroup.com/blog/2018/09/18/python-dependency-management-pip-tools/
