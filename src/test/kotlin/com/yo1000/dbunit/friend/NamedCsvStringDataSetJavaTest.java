package com.yo1000.dbunit.friend;

import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class NamedCsvStringDataSetJavaTest {
    @Test
    void test_csv_to_data_setup() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:testdb;DATABASE_TO_UPPER=TRUE")) {
            setupTables(conn);
//            conn.commit();

            DatabaseConnection dbConn = new DatabaseConnection(conn);
            dbConn.getConfig().setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true);

            IDataSet dataSet = NamedCsvStringDataSet.builder()
                    .table("owners")
                    .csv(
                            "id | name  | Age | BLooD   | BIRTH_DATE\n" +
                            "10 | Alice | 20  | A       | 2000-03-05\n" +
                            "20 | Bob   | 18  |         | 2002-01-02 "
                    )
                    .table("PETS")
                    .csv(
                            "ID   | NAME   | PRICE     | CATEGORY | OWNERS_ID\n" +
                            "1000 | Max    | 500000    | dogs     | 10\n" +
                            "1001 | Bella  | 500000    | dogs     | 10\n" +
                            "1002 |        |           | dogs     | 10\n" +
                            "1003 |        |           | dogs     | 10\n" +
                            "1004 |        |           | dogs     | 10\n" +
                            "1005 | ''     | 123456.70 | dogs     | 10\n" +
                            "1006 | null   | 123456.70 | dogs     | 10\n" +
                            "2000 | Tama   | 200000    | cats     | 20\n" +
                            "2001 | '  '   | 200000.1  | cats     | 20\n" +
                            "9000 |        |           | dogs     | \n"
                    )
                    .build();

            DatabaseOperation.CLEAN_INSERT.execute(dbConn, dataSet);

            Assertion.assertEquals(dataSet, dbConn.createDataSet());
        }
    }

    void setupTables(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS owners ( " +
                    "    id          varchar(40) NOT NULL    PRIMARY KEY , " +
                    "    name        varchar(40) NOT NULL                , " +
                    "    age         int                                 , " +
                    "    blood       varchar(1)                          , " +
                    "    birth_date  date                                  " +
                    "); " +
                    "CREATE TABLE IF NOT EXISTS pets ( " +
                    "    id          varchar(40) NOT NULL    PRIMARY KEY , " +
                    "    name        varchar(40)                         , " +
                    "    price       number(8,2)                         , " +
                    "    category    varchar(20) NOT NULL                , " +
                    "    owners_id   varchar(40)                           " +
                    "); "
            );

            stmt.execute(
                    "TRUNCATE TABLE owners; " +
                    "TRUNCATE TABLE pets; "
            );
        }
    }
}
