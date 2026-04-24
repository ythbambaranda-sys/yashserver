from __future__ import annotations

import importlib
import sqlite3
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus, urlencode


class DatabaseError(RuntimeError):
    """Base yserver database error."""


class UnsupportedDatabaseError(DatabaseError):
    """Raised when a database backend is unknown."""


class MissingDependencyError(DatabaseError):
    """Raised when an optional database dependency is not installed."""


class DatabaseConfigError(DatabaseError):
    """Raised when required connection settings are missing."""


@dataclass(frozen=True, slots=True)
class DatabaseSupport:
    key: str
    name: str
    category: str
    aliases: tuple[str, ...]
    powered_by: str


_DATABASE_SUPPORT: tuple[DatabaseSupport, ...] = (
    DatabaseSupport("mysql", "MySQL", "sql", ("my sql",), "sqlalchemy"),
    DatabaseSupport("postgresql", "PostgreSQL", "sql", ("postgres", "postgresql"), "sqlalchemy"),
    DatabaseSupport("mssql", "Microsoft SQL Server", "sql", ("sql server", "microsoft sql server"), "sqlalchemy"),
    DatabaseSupport("oracle", "Oracle Database", "sql", ("oracle database",), "sqlalchemy"),
    DatabaseSupport("sqlite", "SQLite", "sql", ("sqlite3",), "sqlite3"),
    DatabaseSupport("mariadb", "MariaDB", "sql", ("maria db",), "sqlalchemy"),
    DatabaseSupport("mongodb", "MongoDB", "document", ("mongo", "mongo db"), "pymongo"),
    DatabaseSupport("redis", "Redis", "key-value", tuple(), "redis-py"),
    DatabaseSupport("cassandra", "Cassandra", "wide-column", tuple(), "cassandra-driver"),
    DatabaseSupport("dynamodb", "DynamoDB", "key-value", ("dynamo db",), "boto3"),
    DatabaseSupport(
        "firebase_realtime_database",
        "Firebase Realtime Database",
        "key-value",
        ("firebase", "firebase realtime database", "firebase realtime"),
        "firebase-admin",
    ),
    DatabaseSupport("couchbase", "Couchbase", "document", tuple(), "couchbase"),
    DatabaseSupport("snowflake", "Snowflake", "warehouse", tuple(), "sqlalchemy"),
    DatabaseSupport("bigquery", "Google BigQuery", "warehouse", ("google bigquery",), "sqlalchemy"),
    DatabaseSupport("redshift", "Amazon Redshift", "warehouse", ("amazon redshift",), "sqlalchemy"),
    DatabaseSupport("clickhouse", "ClickHouse", "analytics", tuple(), "sqlalchemy"),
    DatabaseSupport("elasticsearch", "Elasticsearch", "search", ("elastic",), "elasticsearch"),
    DatabaseSupport("neo4j", "Neo4j", "graph", tuple(), "neo4j"),
    DatabaseSupport("influxdb", "InfluxDB", "time-series", ("influx",), "influxdb-client"),
    DatabaseSupport("duckdb", "DuckDB", "analytics", tuple(), "duckdb"),
)


def _normalize(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


_ALIAS_TO_KEY: dict[str, str] = {}
for _spec in _DATABASE_SUPPORT:
    _ALIAS_TO_KEY[_normalize(_spec.key)] = _spec.key
    _ALIAS_TO_KEY[_normalize(_spec.name)] = _spec.key
    for _alias in _spec.aliases:
        _ALIAS_TO_KEY[_normalize(_alias)] = _spec.key


def _normalize_backend(backend: str) -> str:
    normalized = _normalize(backend)
    if normalized in _ALIAS_TO_KEY:
        return _ALIAS_TO_KEY[normalized]
    raise UnsupportedDatabaseError(
        f"Unsupported database backend: {backend!r}. "
        f"Supported: {', '.join(spec.name for spec in _DATABASE_SUPPORT)}"
    )


def _load_optional_module(module_name: str, package_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ImportError as error:
        raise MissingDependencyError(
            f"{module_name!r} is required for this backend. Install with: pip install {package_name}"
        ) from error


class DatabaseClient:
    """
    Unified database client wrapper.

    Methods are capability-based:
    - SQL backends: execute, fetch_one, fetch_all
    - Document/search backends: insert_one, find
    - Key-value backends: set_value, get_value, delete_key
    - Graph backends: run_cypher
    - Time-series backends: write_point, query_flux
    """

    def __init__(self, backend: str, raw: Any) -> None:
        self.backend = backend
        self._raw = raw

    def raw(self) -> Any:
        return self._raw

    def close(self) -> None:  # pragma: no cover - overridden by backend clients
        return None

    def ping(self) -> bool:
        return True

    def execute(self, statement: str, params: Any = None) -> int:
        raise DatabaseError(f"{self.backend} does not support SQL execute()")

    def fetch_one(self, statement: str, params: Any = None) -> dict[str, Any] | None:
        raise DatabaseError(f"{self.backend} does not support SQL fetch_one()")

    def fetch_all(self, statement: str, params: Any = None) -> list[dict[str, Any]]:
        raise DatabaseError(f"{self.backend} does not support SQL fetch_all()")

    def insert_one(self, collection_or_table: str, document: dict[str, Any]) -> Any:
        raise DatabaseError(f"{self.backend} does not support insert_one()")

    def find(
        self,
        collection_or_table: str,
        query: dict[str, Any] | None = None,
        *,
        limit: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        raise DatabaseError(f"{self.backend} does not support find()")

    def set_value(self, key_or_path: str, value: Any, *, expire_seconds: int | None = None) -> Any:
        raise DatabaseError(f"{self.backend} does not support set_value()")

    def get_value(self, key_or_path: str) -> Any:
        raise DatabaseError(f"{self.backend} does not support get_value()")

    def delete_key(self, key_or_path: str) -> int:
        raise DatabaseError(f"{self.backend} does not support delete_key()")

    def run_cypher(self, statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        raise DatabaseError(f"{self.backend} does not support run_cypher()")

    def write_point(
        self,
        measurement: str,
        *,
        fields: dict[str, Any],
        tags: dict[str, str] | None = None,
        timestamp: Any = None,
        bucket: str | None = None,
    ) -> bool:
        raise DatabaseError(f"{self.backend} does not support write_point()")

    def query_flux(self, query: str) -> list[dict[str, Any]]:
        raise DatabaseError(f"{self.backend} does not support query_flux()")

    def __enter__(self) -> "DatabaseClient":
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        self.close()


class _SqliteClient(DatabaseClient):
    def __init__(self, database: str = ":memory:", *, timeout: float = 5.0) -> None:
        connection = sqlite3.connect(database, timeout=timeout, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        super().__init__("sqlite", connection)
        self._connection: sqlite3.Connection = connection

    def close(self) -> None:
        self._connection.close()

    def ping(self) -> bool:
        cursor = self._connection.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        finally:
            cursor.close()

    def execute(self, statement: str, params: Any = None) -> int:
        cursor = self._connection.cursor()
        try:
            cursor.execute(statement, params or ())
            self._connection.commit()
            return int(cursor.rowcount if cursor.rowcount is not None else 0)
        finally:
            cursor.close()

    def fetch_one(self, statement: str, params: Any = None) -> dict[str, Any] | None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(statement, params or ())
            row = cursor.fetchone()
            if row is None:
                return None
            return {key: row[key] for key in row.keys()}
        finally:
            cursor.close()

    def fetch_all(self, statement: str, params: Any = None) -> list[dict[str, Any]]:
        cursor = self._connection.cursor()
        try:
            cursor.execute(statement, params or ())
            rows = cursor.fetchall()
            return [{key: row[key] for key in row.keys()} for row in rows]
        finally:
            cursor.close()


class _DuckDbClient(DatabaseClient):
    def __init__(self, database: str = ":memory:") -> None:
        duckdb = _load_optional_module("duckdb", "duckdb")
        connection = duckdb.connect(database=database)
        super().__init__("duckdb", connection)
        self._connection = connection

    def close(self) -> None:
        self._connection.close()

    def ping(self) -> bool:
        self._connection.execute("SELECT 1").fetchone()
        return True

    def execute(self, statement: str, params: Any = None) -> int:
        cursor = self._connection.execute(statement, params or [])
        rowcount = getattr(cursor, "rowcount", None)
        return int(rowcount if rowcount is not None else 0)

    def fetch_one(self, statement: str, params: Any = None) -> dict[str, Any] | None:
        cursor = self._connection.execute(statement, params or [])
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [item[0] for item in (cursor.description or [])]
        return {columns[index]: value for index, value in enumerate(row)}

    def fetch_all(self, statement: str, params: Any = None) -> list[dict[str, Any]]:
        cursor = self._connection.execute(statement, params or [])
        rows = cursor.fetchall()
        columns = [item[0] for item in (cursor.description or [])]
        return [{columns[index]: value for index, value in enumerate(row)} for row in rows]


class _SqlAlchemyClient(DatabaseClient):
    def __init__(
        self,
        backend: str,
        url: str,
        *,
        connect_args: dict[str, Any] | None = None,
        engine_options: dict[str, Any] | None = None,
    ) -> None:
        sqlalchemy = _load_optional_module("sqlalchemy", "sqlalchemy")
        create_engine = getattr(sqlalchemy, "create_engine")
        text = getattr(sqlalchemy, "text")
        options = dict(engine_options or {})
        if connect_args:
            options["connect_args"] = dict(connect_args)
        engine = create_engine(url, future=True, pool_pre_ping=True, **options)
        super().__init__(backend, engine)
        self._engine = engine
        self._text = text

    def close(self) -> None:
        self._engine.dispose()

    def ping(self) -> bool:
        with self._engine.connect() as connection:
            connection.exec_driver_sql("SELECT 1")
        return True

    def execute(self, statement: str, params: Any = None) -> int:
        with self._engine.begin() as connection:
            result = self._execute(connection, statement, params)
            rowcount = getattr(result, "rowcount", None)
            return int(rowcount if rowcount is not None else 0)

    def fetch_one(self, statement: str, params: Any = None) -> dict[str, Any] | None:
        with self._engine.connect() as connection:
            result = self._execute(connection, statement, params)
            row = result.mappings().first()
            return None if row is None else dict(row)

    def fetch_all(self, statement: str, params: Any = None) -> list[dict[str, Any]]:
        with self._engine.connect() as connection:
            result = self._execute(connection, statement, params)
            return [dict(row) for row in result.mappings().all()]

    def _execute(self, connection: Any, statement: str, params: Any) -> Any:
        if params is None:
            return connection.exec_driver_sql(statement)
        if isinstance(params, dict):
            return connection.execute(self._text(statement), params)
        if isinstance(params, (tuple, list)):
            return connection.exec_driver_sql(statement, tuple(params))
        return connection.exec_driver_sql(statement, params)


class _MongoDbClient(DatabaseClient):
    def __init__(self, uri: str, database: str, **kwargs: Any) -> None:
        pymongo = _load_optional_module("pymongo", "pymongo")
        client = pymongo.MongoClient(uri, **kwargs)
        db = client[database]
        super().__init__("mongodb", client)
        self._client = client
        self._db = db

    def close(self) -> None:
        self._client.close()

    def ping(self) -> bool:
        self._client.admin.command("ping")
        return True

    def insert_one(self, collection_or_table: str, document: dict[str, Any]) -> str:
        result = self._db[collection_or_table].insert_one(document)
        return str(result.inserted_id)

    def find(
        self,
        collection_or_table: str,
        query: dict[str, Any] | None = None,
        *,
        limit: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        cursor = self._db[collection_or_table].find(query or {}, **kwargs)
        cursor = cursor.limit(max(1, int(limit)))
        output: list[dict[str, Any]] = []
        for item in cursor:
            row = dict(item)
            if "_id" in row:
                row["_id"] = str(row["_id"])
            output.append(row)
        return output


class _RedisClient(DatabaseClient):
    def __init__(self, url: str, **kwargs: Any) -> None:
        redis = _load_optional_module("redis", "redis")
        client = redis.Redis.from_url(url, decode_responses=True, **kwargs)
        super().__init__("redis", client)
        self._client = client

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    def ping(self) -> bool:
        return bool(self._client.ping())

    def set_value(self, key_or_path: str, value: Any, *, expire_seconds: int | None = None) -> bool:
        return bool(self._client.set(key_or_path, value, ex=expire_seconds))

    def get_value(self, key_or_path: str) -> Any:
        return self._client.get(key_or_path)

    def delete_key(self, key_or_path: str) -> int:
        return int(self._client.delete(key_or_path))


class _CassandraClient(DatabaseClient):
    def __init__(self, contact_points: list[str], *, port: int = 9042, keyspace: str | None = None, **kwargs: Any) -> None:
        cassandra_cluster = _load_optional_module("cassandra.cluster", "cassandra-driver")
        cluster = cassandra_cluster.Cluster(contact_points=contact_points, port=port, **kwargs)
        session = cluster.connect(keyspace) if keyspace else cluster.connect()
        super().__init__("cassandra", cluster)
        self._cluster = cluster
        self._session = session

    def close(self) -> None:
        self._cluster.shutdown()

    def ping(self) -> bool:
        self._session.execute("SELECT now() FROM system.local")
        return True

    def execute(self, statement: str, params: Any = None) -> int:
        self._session.execute(statement, params)
        return 0

    def fetch_one(self, statement: str, params: Any = None) -> dict[str, Any] | None:
        result = self._session.execute(statement, params)
        row = result.one()
        if row is None:
            return None
        if hasattr(row, "_asdict"):
            return dict(row._asdict())
        return dict(row)

    def fetch_all(self, statement: str, params: Any = None) -> list[dict[str, Any]]:
        result = self._session.execute(statement, params)
        output: list[dict[str, Any]] = []
        for row in result:
            if hasattr(row, "_asdict"):
                output.append(dict(row._asdict()))
            else:
                output.append(dict(row))
        return output


class _DynamoDbClient(DatabaseClient):
    def __init__(self, *, region_name: str | None = None, endpoint_url: str | None = None, **kwargs: Any) -> None:
        boto3 = _load_optional_module("boto3", "boto3")
        resource = boto3.resource("dynamodb", region_name=region_name, endpoint_url=endpoint_url, **kwargs)
        super().__init__("dynamodb", resource)
        self._resource = resource

    def close(self) -> None:
        return None

    def ping(self) -> bool:
        list(self._resource.tables.limit(1))
        return True

    def insert_one(self, collection_or_table: str, document: dict[str, Any]) -> Any:
        table = self._resource.Table(collection_or_table)
        table.put_item(Item=document)
        return document

    def find(
        self,
        collection_or_table: str,
        query: dict[str, Any] | None = None,
        *,
        limit: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        table = self._resource.Table(collection_or_table)
        scan_kwargs = dict(kwargs)
        if query and "FilterExpression" not in scan_kwargs:
            scan_kwargs["FilterExpression"] = query  # expects boto3 condition expression when used
        scan_kwargs.setdefault("Limit", max(1, int(limit)))
        response = table.scan(**scan_kwargs)
        return list(response.get("Items", []))

    def get_value(self, key_or_path: str) -> Any:
        raise DatabaseError("dynamodb get_value() requires table and key dict; use find()/insert_one() or raw()")


class _FirebaseRealtimeClient(DatabaseClient):
    def __init__(
        self,
        *,
        database_url: str,
        credentials_path: str | None = None,
        app_name: str = "yserver-firebase",
    ) -> None:
        firebase_admin = _load_optional_module("firebase_admin", "firebase-admin")
        firebase_db = _load_optional_module("firebase_admin.db", "firebase-admin")
        if not database_url:
            raise DatabaseConfigError("firebase_realtime_database requires database_url")

        try:
            app = firebase_admin.get_app(app_name)
        except ValueError:
            if credentials_path:
                credentials = firebase_admin.credentials.Certificate(credentials_path)
                app = firebase_admin.initialize_app(credentials, {"databaseURL": database_url}, name=app_name)
            else:
                app = firebase_admin.initialize_app(options={"databaseURL": database_url}, name=app_name)

        super().__init__("firebase_realtime_database", app)
        self._app = app
        self._db = firebase_db

    def close(self) -> None:
        return None

    def ping(self) -> bool:
        self._db.reference("/", app=self._app)
        return True

    def set_value(self, key_or_path: str, value: Any, *, expire_seconds: int | None = None) -> Any:
        if expire_seconds is not None:
            raise DatabaseError("firebase_realtime_database does not support key TTL in set_value()")
        self._db.reference(key_or_path, app=self._app).set(value)
        return value

    def get_value(self, key_or_path: str) -> Any:
        return self._db.reference(key_or_path, app=self._app).get()

    def insert_one(self, collection_or_table: str, document: dict[str, Any]) -> Any:
        result = self._db.reference(collection_or_table, app=self._app).push(document)
        return result.key


class _CouchbaseClient(DatabaseClient):
    def __init__(
        self,
        *,
        connection_string: str,
        username: str,
        password: str,
        bucket: str,
    ) -> None:
        couchbase_cluster = _load_optional_module("couchbase.cluster", "couchbase")
        couchbase_auth = _load_optional_module("couchbase.auth", "couchbase")
        authenticator = couchbase_auth.PasswordAuthenticator(username, password)
        cluster = couchbase_cluster.Cluster(connection_string, couchbase_cluster.ClusterOptions(authenticator))
        bucket_ref = cluster.bucket(bucket)
        collection = bucket_ref.default_collection()
        super().__init__("couchbase", cluster)
        self._cluster = cluster
        self._collection = collection

    def close(self) -> None:
        close = getattr(self._cluster, "close", None)
        if callable(close):
            close()

    def ping(self) -> bool:
        self._cluster.ping()
        return True

    def set_value(self, key_or_path: str, value: Any, *, expire_seconds: int | None = None) -> bool:
        kwargs: dict[str, Any] = {}
        if expire_seconds is not None:
            kwargs["ttl"] = int(expire_seconds)
        self._collection.upsert(key_or_path, value, **kwargs)
        return True

    def get_value(self, key_or_path: str) -> Any:
        result = self._collection.get(key_or_path)
        return result.content_as[dict]

    def insert_one(self, collection_or_table: str, document: dict[str, Any]) -> Any:
        document_id = document.get("id") or document.get("_id")
        if not document_id:
            raise DatabaseConfigError("couchbase insert_one requires document id in 'id' or '_id'")
        self._collection.upsert(str(document_id), document)
        return str(document_id)

    def find(
        self,
        collection_or_table: str,
        query: dict[str, Any] | None = None,
        *,
        limit: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        statement = kwargs.get("statement")
        if not statement:
            raise DatabaseConfigError("couchbase find() requires statement='SELECT ...' in kwargs")
        result = self._cluster.query(statement)
        output: list[dict[str, Any]] = []
        for index, row in enumerate(result):
            if index >= max(1, int(limit)):
                break
            output.append(dict(row))
        return output


class _ElasticsearchClient(DatabaseClient):
    def __init__(self, *, hosts: list[str] | str, **kwargs: Any) -> None:
        elasticsearch = _load_optional_module("elasticsearch", "elasticsearch")
        client = elasticsearch.Elasticsearch(hosts=hosts, **kwargs)
        super().__init__("elasticsearch", client)
        self._client = client

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    def ping(self) -> bool:
        return bool(self._client.ping())

    def insert_one(self, collection_or_table: str, document: dict[str, Any]) -> Any:
        index = collection_or_table
        document_id = document.get("id") or document.get("_id")
        response = self._client.index(index=index, id=document_id, document=document, refresh="wait_for")
        return response.get("_id")

    def find(
        self,
        collection_or_table: str,
        query: dict[str, Any] | None = None,
        *,
        limit: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        index = collection_or_table
        size = max(1, int(limit))
        try:
            response = self._client.search(index=index, query=query or {"match_all": {}}, size=size, **kwargs)
        except TypeError:
            response = self._client.search(index=index, body={"query": query or {"match_all": {}}, "size": size}, **kwargs)
        hits = response.get("hits", {}).get("hits", [])
        output: list[dict[str, Any]] = []
        for item in hits:
            source = dict(item.get("_source", {}))
            if "_id" in item:
                source["_id"] = item["_id"]
            output.append(source)
        return output


class _Neo4jClient(DatabaseClient):
    def __init__(self, *, uri: str, username: str, password: str, database: str | None = None, **kwargs: Any) -> None:
        neo4j = _load_optional_module("neo4j", "neo4j")
        driver = neo4j.GraphDatabase.driver(uri, auth=(username, password), **kwargs)
        super().__init__("neo4j", driver)
        self._driver = driver
        self._database = database

    def close(self) -> None:
        self._driver.close()

    def ping(self) -> bool:
        self._driver.verify_connectivity()
        return True

    def run_cypher(self, statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        with self._driver.session(database=self._database) as session:
            result = session.run(statement, params or {})
            return [record.data() for record in result]


class _InfluxDbClient(DatabaseClient):
    def __init__(self, *, url: str, token: str, org: str, bucket: str | None = None, **kwargs: Any) -> None:
        influx_module = _load_optional_module("influxdb_client", "influxdb-client")
        client = influx_module.InfluxDBClient(url=url, token=token, org=org, **kwargs)
        write_options = getattr(influx_module, "SYNCHRONOUS", None)
        write_api = client.write_api(write_options=write_options) if write_options is not None else client.write_api()
        query_api = client.query_api()
        super().__init__("influxdb", client)
        self._client = client
        self._module = influx_module
        self._write_api = write_api
        self._query_api = query_api
        self._bucket = bucket
        self._org = org

    def close(self) -> None:
        self._client.close()

    def ping(self) -> bool:
        health = self._client.health()
        status = str(getattr(health, "status", "")).lower()
        return status in ("pass", "healthy", "ok")

    def write_point(
        self,
        measurement: str,
        *,
        fields: dict[str, Any],
        tags: dict[str, str] | None = None,
        timestamp: Any = None,
        bucket: str | None = None,
    ) -> bool:
        if not measurement:
            raise DatabaseConfigError("influxdb write_point requires measurement")
        target_bucket = bucket or self._bucket
        if not target_bucket:
            raise DatabaseConfigError("influxdb write_point requires bucket (constructor or method arg)")

        point = self._module.Point(measurement)
        for key, value in (tags or {}).items():
            point = point.tag(key, value)
        for key, value in fields.items():
            point = point.field(key, value)
        if timestamp is not None:
            point = point.time(timestamp)
        self._write_api.write(bucket=target_bucket, org=self._org, record=point)
        return True

    def query_flux(self, query: str) -> list[dict[str, Any]]:
        tables = self._query_api.query(query=query, org=self._org)
        output: list[dict[str, Any]] = []
        for table in tables:
            for record in table.records:
                output.append(dict(record.values))
        return output


_SQLALCHEMY_BACKENDS = {
    "mysql",
    "postgresql",
    "mssql",
    "oracle",
    "mariadb",
    "snowflake",
    "bigquery",
    "redshift",
    "clickhouse",
}


def build_sqlalchemy_url(
    backend: str,
    *,
    username: str | None = None,
    password: str | None = None,
    host: str = "127.0.0.1",
    port: int | None = None,
    database: str | None = None,
    driver: str | None = None,
    query: dict[str, Any] | None = None,
) -> str:
    backend_key = _normalize_backend(backend)
    if backend_key not in _SQLALCHEMY_BACKENDS:
        raise DatabaseConfigError(f"{backend} does not use SQLAlchemy URL builder")

    dialect = backend_key
    if backend_key == "redshift":
        dialect = "redshift"
        if driver is None:
            driver = "redshift_connector"
    elif backend_key == "clickhouse":
        dialect = "clickhouse"
        if driver is None:
            driver = "native"

    driver_part = f"+{driver}" if driver else ""
    auth_part = ""
    if username:
        auth_part = quote_plus(username)
        if password is not None:
            auth_part += f":{quote_plus(password)}"
        auth_part += "@"
    host_part = host or ""
    if port is not None:
        host_part += f":{int(port)}"
    database_part = f"/{database}" if database else ""
    query_part = f"?{urlencode(query, doseq=True)}" if query else ""
    return f"{dialect}{driver_part}://{auth_part}{host_part}{database_part}{query_part}"


def connect_database(backend: str, **config: Any) -> DatabaseClient:
    """
    Connect to a supported database and return a unified client.

    Quick examples:
    - SQLite:
      `connect_database("sqlite", database="app.db")`
    - PostgreSQL/MySQL/etc.:
      `connect_database("postgresql", url="postgresql+psycopg://user:pass@localhost/app")`
    - MongoDB:
      `connect_database("mongodb", uri="mongodb://localhost:27017", database="app")`
    """

    backend_key = _normalize_backend(backend)
    cfg = dict(config)

    if backend_key == "sqlite":
        return _SqliteClient(database=str(cfg.pop("database", ":memory:")), timeout=float(cfg.pop("timeout", 5.0)))

    if backend_key == "duckdb":
        return _DuckDbClient(database=str(cfg.pop("database", ":memory:")))

    if backend_key in _SQLALCHEMY_BACKENDS:
        url = str(cfg.pop("url", "") or cfg.pop("dsn", ""))
        if not url:
            url = build_sqlalchemy_url(
                backend_key,
                username=cfg.pop("username", cfg.pop("user", None)),
                password=cfg.pop("password", None),
                host=str(cfg.pop("host", "127.0.0.1")),
                port=cfg.pop("port", None),
                database=cfg.pop("database", None),
                driver=cfg.pop("driver", None),
                query=cfg.pop("query", None),
            )
        connect_args = cfg.pop("connect_args", None)
        engine_options = cfg
        return _SqlAlchemyClient(
            backend_key,
            url,
            connect_args=connect_args if isinstance(connect_args, dict) else None,
            engine_options=engine_options,
        )

    if backend_key == "mongodb":
        uri = str(cfg.pop("uri", "mongodb://127.0.0.1:27017"))
        database = str(cfg.pop("database", "test"))
        return _MongoDbClient(uri=uri, database=database, **cfg)

    if backend_key == "redis":
        url = str(cfg.pop("url", cfg.pop("uri", "redis://127.0.0.1:6379/0")))
        return _RedisClient(url=url, **cfg)

    if backend_key == "cassandra":
        contact_points = cfg.pop("contact_points", ["127.0.0.1"])
        if isinstance(contact_points, str):
            contact_points = [part.strip() for part in contact_points.split(",") if part.strip()]
        return _CassandraClient(
            contact_points=list(contact_points),
            port=int(cfg.pop("port", 9042)),
            keyspace=cfg.pop("keyspace", None),
            **cfg,
        )

    if backend_key == "dynamodb":
        return _DynamoDbClient(
            region_name=cfg.pop("region_name", None),
            endpoint_url=cfg.pop("endpoint_url", None),
            **cfg,
        )

    if backend_key == "firebase_realtime_database":
        return _FirebaseRealtimeClient(
            database_url=str(cfg.pop("database_url", "")),
            credentials_path=cfg.pop("credentials_path", None),
            app_name=str(cfg.pop("app_name", "yserver-firebase")),
        )

    if backend_key == "couchbase":
        return _CouchbaseClient(
            connection_string=str(cfg.pop("connection_string", "couchbase://127.0.0.1")),
            username=str(cfg.pop("username", "")),
            password=str(cfg.pop("password", "")),
            bucket=str(cfg.pop("bucket", "")),
        )

    if backend_key == "elasticsearch":
        hosts = cfg.pop("hosts", ["http://127.0.0.1:9200"])
        return _ElasticsearchClient(hosts=hosts, **cfg)

    if backend_key == "neo4j":
        return _Neo4jClient(
            uri=str(cfg.pop("uri", "bolt://127.0.0.1:7687")),
            username=str(cfg.pop("username", "neo4j")),
            password=str(cfg.pop("password", "")),
            database=cfg.pop("database", None),
            **cfg,
        )

    if backend_key == "influxdb":
        return _InfluxDbClient(
            url=str(cfg.pop("url", "http://127.0.0.1:8086")),
            token=str(cfg.pop("token", "")),
            org=str(cfg.pop("org", "")),
            bucket=cfg.pop("bucket", None),
            **cfg,
        )

    raise UnsupportedDatabaseError(f"Unsupported backend: {backend}")


def list_supported_databases() -> list[str]:
    return [spec.name for spec in _DATABASE_SUPPORT]


def database_support_matrix() -> list[dict[str, str]]:
    return [
        {
            "key": spec.key,
            "name": spec.name,
            "category": spec.category,
            "powered_by": spec.powered_by,
        }
        for spec in _DATABASE_SUPPORT
    ]
