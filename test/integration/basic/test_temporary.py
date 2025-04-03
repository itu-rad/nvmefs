import pytest
import duckdb
from decimal import Decimal

@pytest.fixture(scope="module")
def tpch_database_connection(device):
    # Setup

    con = duckdb.connect(config={"allow_unsigned_extensions": "true", "memory_limit": "500MB"})
    con.load_extension("nvmefs")
    con.load_extension("tpch")

    con.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{device.device_path}',
                        fdp_plhdls       '{7}'
                    );""")

    con.execute("ATTACH DATABASE 'nvmefs:///tpch.db' AS test (READ_WRITE);")
    con.execute("USE test;")

    con.execute("CALL dbgen(sf=1);")

    yield con

    # Teardown the database and data
    con.close()

def test_large_query_and_spilling_to_disk(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    connection = tpch_database_connection
    result = connection.execute("SELECT answer FROM tpch_answers() WHERE query_nr = 7 AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]
    expected_query7_results = [(columns[0], columns[1], int(columns[2]), Decimal(columns[3])) for columns in 
                               [line.split("|") for line in result_rows]]


    query7_result = connection.execute("PRAGMA tpch(7);").fetchall()

    assert query7_result == expected_query7_results