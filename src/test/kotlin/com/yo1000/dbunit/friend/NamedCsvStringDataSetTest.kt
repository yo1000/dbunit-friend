package com.yo1000.dbunit.friend

import org.assertj.core.api.Assertions.assertThat
import org.dbunit.Assertion
import org.dbunit.database.DatabaseConfig
import org.dbunit.database.DatabaseConnection
import org.dbunit.operation.DatabaseOperation
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager

class NamedCsvStringDataSetTest {
    @Test
    fun test_csv_to_data_setup() {
        DriverManager.getConnection("jdbc:h2:mem:testdb;DATABASE_TO_UPPER=TRUE").use {
            setupTables(it)
//            it.commit()

            val conn = DatabaseConnection(it).also { conn ->
                conn.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
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

            DatabaseOperation.CLEAN_INSERT.execute(conn, dataSet)

            Assertion.assertEquals(dataSet, conn.createDataSet())

            it.createStatement().use {
                it.executeQuery("""
                    SELECT
                        id, name, age, blood, birth_date
                    FROM
                        owners
                    ORDER BY
                        id
                """.trimIndent()).use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("10")
                    assertThat(it.getString("name")).isEqualTo("Alice")
                    assertThat(it.getInt("age")).isEqualTo(20)
                    assertThat(it.getString("blood")).isEqualTo("A")
                    assertThat(it.getDate("birth_date")).isEqualTo(Date.valueOf("2000-03-05"))

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("20")
                    assertThat(it.getString("name")).isEqualTo("Bob")
                    assertThat(it.getInt("age")).isEqualTo(18)
                    assertThat(it.getString("blood")).isEqualTo(null)
                    assertThat(it.getDate("birth_date")).isEqualTo(Date.valueOf("2002-01-02"))

                    assertThat(it.next()).isFalse()
                }
            }

            it.createStatement().use {
                it.executeQuery("""
                    SELECT
                        id, name, price, category, owners_id
                    FROM
                        pets
                    ORDER BY
                        id
                """.trimIndent()).use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1000")
                    assertThat(it.getString("name")).isEqualTo("Max")
                    assertThat(it.getBigDecimal("price")).isEqualTo(BigDecimal("500000.00"))
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1001")
                    assertThat(it.getString("name")).isEqualTo("Bella")
                    assertThat(it.getBigDecimal("price")).isEqualTo(BigDecimal("500000.00"))
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1002")
                    assertThat(it.getString("name")).isEqualTo(null)
                    assertThat(it.getBigDecimal("price")).isNull()
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1003")
                    assertThat(it.getString("name")).isEqualTo(null)
                    assertThat(it.getBigDecimal("price")).isNull()
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1004")
                    assertThat(it.getString("name")).isEqualTo(null)
                    assertThat(it.getBigDecimal("price")).isNull()
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1005")
                    assertThat(it.getString("name")).isEqualTo("")
                    assertThat(it.getBigDecimal("price")).isEqualTo(BigDecimal("123456.70"))
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("1006")
                    assertThat(it.getString("name")).isEqualTo("null")
                    assertThat(it.getBigDecimal("price")).isEqualTo(BigDecimal("123456.70"))
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo("10")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("2000")
                    assertThat(it.getString("name")).isEqualTo("Tama")
                    assertThat(it.getBigDecimal("price")).isEqualTo(BigDecimal("200000.00"))
                    assertThat(it.getString("category")).isEqualTo("cats")
                    assertThat(it.getString("owners_id")).isEqualTo("20")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("2001")
                    assertThat(it.getString("name")).isEqualTo("  ")
                    assertThat(it.getBigDecimal("price")).isEqualTo(BigDecimal("200000.10"))
                    assertThat(it.getString("category")).isEqualTo("cats")
                    assertThat(it.getString("owners_id")).isEqualTo("20")

                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("9000")
                    assertThat(it.getString("name")).isEqualTo(null)
                    assertThat(it.getBigDecimal("price")).isNull()
                    assertThat(it.getString("category")).isEqualTo("dogs")
                    assertThat(it.getString("owners_id")).isEqualTo(null)

                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun test_existed_data_expectation() {
        DriverManager.getConnection("jdbc:h2:mem:testdb;DATABASE_TO_UPPER=TRUE").use {
            setupTables(it)
//            it.commit()

            it.createStatement().use { listOf(
                "INSERT INTO owners (id, name, age, blood, birth_date) VALUES ('10', 'Alice', 20, 'A' , '2000-03-05')",
                "INSERT INTO owners (id, name, age, blood, birth_date) VALUES ('20', 'Bob'  , 18, null, '2002-01-02')",

                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1000', 'Max'  , 500000  , 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1001', 'Bella', 500000  , 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1002', null   , null    , 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1003', null   , null    , 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1004', null   , null    , 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1005', ''     , 123456.7, 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('1006', 'null' , 123456.7, 'dogs', '10')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('2000', 'Tama' , 200000  , 'cats', '20')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('2001', '  '   , 200000.1, 'cats', '20')",
                "INSERT INTO pets (id, name, price, category, owners_id) VALUES ('9000', null   , null    , 'dogs', null)"
            ).forEach(it::addBatch)
                it.executeBatch()
            }

            val conn = DatabaseConnection(it).also { conn ->
                conn.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
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

            Assertion.assertEquals(dataSet, conn.createDataSet())
        }
    }

    @Test
    fun test_markdown_to_data_setup1() {
        DriverManager.getConnection("jdbc:h2:mem:testdb;DATABASE_TO_UPPER=TRUE").use {
            setupTables(it)
//            it.commit()

            val conn = DatabaseConnection(it).also { conn ->
                conn.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
            }

            val dataSet = NamedCsvStringDataSet.builder(delimiter = ',', enclosure = '"')
                .table("owners")
                .csv(
                    """
                    id , name  , Age , BLooD   , BIRTH_DATE
                    10 , Alice , 20  , A       , 2000-03-05
                    20 , Bob   , 18  ,         , 2002-01-02
                    """
                )
                .table("PETS")
                .csv(
                    """
                    ID   , NAME   , PRICE     , CATEGORY , OWNERS_ID
                    1000 , Max    , 500000    , dogs     , 10
                    1001 , Bella  , 500000    , dogs     , 10
                    1002 ,        ,           , dogs     , 10
                    1003 ,        ,           , dogs     , 10
                    1004 ,        ,           , dogs     , 10
                    1005 , ""     , 123456.70 , dogs     , 10
                    1006 , null   , 123456.70 , dogs     , 10
                    2000 , Tama   , 200000    , cats     , 20
                    2001 , "  "   , 200000.1  , cats     , 20
                    9000 ,        ,           , dogs     , 
                    """
                )
                .build()

            DatabaseOperation.CLEAN_INSERT.execute(conn, dataSet)

            Assertion.assertEquals(dataSet, conn.createDataSet())
        }
    }

    private fun setupTables(conn: Connection) {
        conn.createStatement().use {
            it.execute(
                """
                CREATE TABLE IF NOT EXISTS owners (
                    id          varchar(40) NOT NULL    PRIMARY KEY ,
                    name        varchar(40) NOT NULL                ,
                    age         int                                 ,
                    blood       varchar(1)                          ,
                    birth_date  date
                );
                
                CREATE TABLE IF NOT EXISTS pets (
                    id          varchar(40) NOT NULL    PRIMARY KEY ,
                    name        varchar(40)                         ,
                    price       number(8,2)                         ,
                    category    varchar(20) NOT NULL                ,
                    owners_id   varchar(40)
                );
                """.trimIndent())
        }

        conn.createStatement().use {
            it.execute(
                """
                TRUNCATE TABLE owners;
                TRUNCATE TABLE pets;
                """.trimIndent())
        }
    }
}
