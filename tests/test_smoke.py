"""M0 smoke test: proves local Spark can write, MERGE into and time-travel a Delta table."""


def test_delta_write_merge_and_time_travel(spark, tmp_path):
    path = str(tmp_path / "smoke")

    # Version 0: two rows.
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]).write.format("delta").save(path)

    # Version 1: MERGE updates id=2 and inserts id=3.
    spark.createDataFrame([(2, "B"), (3, "c")], ["id", "val"]).createOrReplaceTempView("updates")
    spark.sql(f"""
        MERGE INTO delta.`{path}` t USING updates u ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    current = {r.id: r.val for r in spark.read.format("delta").load(path).collect()}
    assert current == {1: "a", 2: "B", 3: "c"}

    # Time travel back to version 0.
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    assert v0.count() == 2
