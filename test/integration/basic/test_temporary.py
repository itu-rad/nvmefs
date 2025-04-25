import pytest
import duckdb
from decimal import Decimal

@pytest.fixture(scope="module")
def tpch_database_connection(device):
    # Setup

    con = duckdb.connect(config={"allow_unsigned_extensions": "true", "memory_limit": "50MB", "threads": 1})
    con.load_extension("nvmefs")
    con.load_extension("tpch")

    con.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{device.device_path}',
                        fdp_plhdls       '{7}',
                        backend          'nvme',
                    );""")

    con.execute("ATTACH DATABASE 'nvmefs:///tpch.db' AS test (READ_WRITE);")
    con.execute("USE test;")

    con.execute("CALL dbgen(sf=1);")

    yield con

    # Teardown the database and data
    con.close()

tpchqueries = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
]

@pytest.mark.parametrize("query", tpchqueries)
def test_large_query_and_spilling_to_disk(query, tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]
    expected_query_results = [(columns[0], columns[1], int(columns[2]), Decimal(columns[3])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query7_results