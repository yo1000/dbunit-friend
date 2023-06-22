:teddy_bear: dbunit-friend
================================================================================

Convert user-friendly inline CSV into table data using DBUnit.

Dependencies
--------------------------------------------------------------------------------

```xml
<dependency>
    <groupId>com.yo1000</groupId>
    <artifactId>dbunit-friend</artifactId>
    <version>1.0.0</version>
</dependency>
```

Examples
--------------------------------------------------------------------------------

Details refer to `src/test/kotlin/com/yo1000/dbunit/friend/NamedCsvStringDataSetTest.kt`

### for Kotlin

```kotlin
dataSource.connection.use { conn ->
    val dbConn = DatabaseConnection(conn).also { dbConn ->
        dbConn.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
    }

    val dataSet = NamedCsvStringDataSet.builder()
        .table("owners")
        .csv(
            """
            id | name  | Age | BLooD   | BIRTH_DATE
            10 | Alice | 20  | A       | 2000-03-05
            20 | Bob   | 18  |         | 2002-01-02
            """
        )
        .table("PETS")
        .csv(
            """
            ID   | NAME   | PRICE     | CATEGORY | OWNERS_ID
            1000 | Max    | 500000    | dogs     | 10
            1001 | Bella  | 500000    | dogs     | 10
            1002 |        |           | dogs     | 10
            1003 |        |           | dogs     | 10
            1004 |        |           | dogs     | 10
            1005 | ''     | 123456.70 | dogs     | 10
            1006 | null   | 123456.70 | dogs     | 10
            2000 | Tama   | 200000    | cats     | 20
            2001 | '  '   | 200000.1  | cats     | 20
            9000 |        |           | dogs     | 
            """
        )
        .build()

    DatabaseOperation.CLEAN_INSERT.execute(dbConn, dataSet)

    Assertion.assertEquals(dataSet, dbConn.createDataSet())
}
```

### for Java13+

```java
try (Connection conn = dataSource.getConnection()) {
    DatabaseConnection dbConn = new DatabaseConnection(conn);
    dbConn.getConfig().setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true);

    IDataSet dataSet = NamedCsvStringDataSet.builder()
        .table("owners")
        .csv(
            """
            id | name  | Age | BLooD   | BIRTH_DATE
            10 | Alice | 20  | A       | 2000-03-05
            20 | Bob   | 18  |         | 2002-01-02
            """
        )
        .table("PETS")
        .csv(
            """
            ID   | NAME   | PRICE     | CATEGORY | OWNERS_ID
            1000 | Max    | 500000    | dogs     | 10
            1001 | Bella  | 500000    | dogs     | 10
            1002 |        |           | dogs     | 10
            1003 |        |           | dogs     | 10
            1004 |        |           | dogs     | 10
            1005 | ''     | 123456.70 | dogs     | 10
            1006 | null   | 123456.70 | dogs     | 10
            2000 | Tama   | 200000    | cats     | 20
            2001 | '  '   | 200000.1  | cats     | 20
            9000 |        |           | dogs     | 
            """
        )
        .build();

    DatabaseOperation.CLEAN_INSERT.execute(dbConn, dataSet);

    Assertion.assertEquals(dataSet, dbConn.createDataSet());
}
```
