def test_spark_session_creates_dataframe(spark):
    # Arrange
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, value string")

    # Act
    rows = df.count()

    # Assert
    assert rows == 2