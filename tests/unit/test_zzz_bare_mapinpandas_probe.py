"""Environment smoke test: bare mapInPandas, zero framework code involved."""
import pandas as pd


def test_bare_mapinpandas(spark):
    df = spark.createDataFrame([("a", bytes([1]))], "path string, content binary")

    def f(it):
        for b in it:
            yield pd.DataFrame({"n": [len(b)]})

    assert df.mapInPandas(f, schema="n long").collect()
