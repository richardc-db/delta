/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.IOException
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.collection.mutable.{Seq => MutableSeq}

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.actions.{Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.StructType

class DeltaVariantSuite
    extends QueryTest
    with DeltaSQLCommandTest {
  private def getProtocolForTable(table: String): Protocol = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    deltaLog.unsafeVolatileSnapshot.protocol
  }

  // JSON string used to validate golden file testing for variant-writer-one and variant-writer-two.
  private val goldenFileJsonString = """
  {
    "arr": [5, 10, 15],
    "numeric": 5,
    "decimal": 4.4,
    "str": "test",
    "struct": {
      "child": 10
    },
    "ts": "2024-03-01",
    "arrayOfStructs": [{"b": "str"}, {"b": null}, {"diffKey": null}, {"diffKey": 5}]
  }
  """.filterNot(_.isWhitespace)


  private def copyDirectory(sourceDir: Path, targetDir: Path): Unit = {
    Files.walk(sourceDir).forEach { sourcePath =>
      val targetPath = targetDir.resolve(sourceDir.relativize(sourcePath))
      try {
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
      } catch {
        case e: IOException => throw new IOException(s"Error copying file: ${e.getMessage}")
      }
    }
  }


  test("create a new table with Variant, higher protocol and feature should be picked.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
      assert(
        getProtocolForTable("tbl") ==
        VariantTypeTableFeature.minProtocolVersion.withFeature(VariantTypeTableFeature)
      )
    }
  }

  test("creating a table without Variant should use the usual minimum protocol") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      assert(
        !deltaLog.unsafeVolatileSnapshot.protocol.isFeatureSupported(VariantTypeTableFeature),
        s"Table tbl contains VariantTypeFeature descriptor when its not supposed to"
      )
    }
  }

  test("add a new Variant column should upgrade to the correct protocol versions") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING) USING delta")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      // Should throw error
      val e = intercept[SparkThrowable] {
        sql("ALTER TABLE tbl ADD COLUMN v VARIANT")
      }
      // capture the existing protocol here.
      // we will check the error message later in this test as we need to compare the
      // expected schema and protocol
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val currentProtocol = deltaLog.unsafeVolatileSnapshot.protocol
      val currentFeatures = currentProtocol.implicitlyAndExplicitlySupportedFeatures
        .map(_.name)
        .toSeq
        .sorted
        .mkString(", ")

      // add table feature
      sql(
        s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('delta.feature.variantType-dev' = 'supported')"
      )

      sql("ALTER TABLE tbl ADD COLUMN v VARIANT")

      // check previously thrown error message
      checkError(
        e,
        errorClass = "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT",
        parameters = Map(
          "unsupportedFeatures" -> VariantTypeTableFeature.name,
          "supportedFeatures" -> currentFeatures
        )
      )

      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))

      assert(
        getProtocolForTable("tbl") ==
        VariantTypeTableFeature.minProtocolVersion
          .withFeature(VariantTypeTableFeature)
          .withFeature(InvariantsTableFeature)
          .withFeature(AppendOnlyTableFeature)
      )
    }
  }

  test("Zorder is not supported for Variant") {
    withTable("tbl") {
      sql("CREATE TABLE tbl USING DELTA AS SELECT id, cast(null as variant) v from range(100)")
      val e = intercept[SparkException](sql("optimize tbl zorder by (v)"))
      checkError(
        e.getCause.asInstanceOf[SparkThrowable],
        "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
        parameters = Map(
          "msg" -> "cannot sort data type variant",
          "hint" -> "",
          "sqlExpr" -> "\"rangepartitionid(v)\""))
    }
  }

  test("streaming variant delta table") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(100)
        .selectExpr("parse_json(cast(id as string)) v")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val streamingDf = spark.readStream
        .format("delta")
        .load(path)
        .selectExpr("v::int as extractedVal")

      val q = streamingDf.writeStream
        .format("memory")
        .queryName("test_table")
        .start()
      
      q.processAllAvailable()
      q.stop()

      val actual = spark.sql("select extractedVal from test_table")
      val expected = spark.sql("select id from range(100)")
      checkAnswer(actual, expected.collect())
    }
  }

  Seq("variant-writer-one", "variant-writer-two").foreach { tableName =>
    test(s"variant written by databricks $tableName can be read") {
      val path = s"src/test/resources/delta/$tableName"
      val expected = spark.range(0, 10000, 1, 1).selectExpr(
        "id % 3 as id",
        s"parse_json('$goldenFileJsonString') v",
        s"""array(
          parse_json('$goldenFileJsonString'),
          null,
          parse_json('$goldenFileJsonString'),
          null,
          parse_json('$goldenFileJsonString')) as array_of_variants""",
        s"named_struct('v', parse_json('$goldenFileJsonString')) struct_of_variants",
        s"map(id, parse_json('$goldenFileJsonString'), 'nullKey', null) map_of_variants"
      )

      checkAnswer(spark.read.format("delta").load(path), expected.collect())
    }
  }

  Seq("variant-writer-one", "variant-writer-two").foreach { tableName =>
    test(s"appending to delta table written by databricks with variants works - $tableName") {
      withTempDir { dir =>
        val sourceDirPath = Paths.get(s"src/test/resources/delta/$tableName")
        val targetDirPath = Paths.get(dir.getAbsolutePath)

        try {
          // Copy golden file to a temporary directory so the test can append to it freely.
          copyDirectory(sourceDirPath, targetDirPath)
        } catch {
          case e: IOException => println(s"Error copying directory: ${e.getMessage}")
        }

        spark.range(10000, 15000, 1, 1).selectExpr(
          "id % 3 as id",
          s"parse_json('$goldenFileJsonString') v",
          s"""array(
            parse_json('$goldenFileJsonString'),
            null,
            parse_json('$goldenFileJsonString'),
            null,
            parse_json('$goldenFileJsonString')) as array_of_variants""",
          s"named_struct('v', parse_json('$goldenFileJsonString')) struct_of_variants",
          s"map(id, parse_json('$goldenFileJsonString'), 'nullKey', null) map_of_variants"
        ).write.format("delta").mode("append").save(dir.getAbsolutePath)

        val expected = spark.range(0, 15000, 1, 1).selectExpr(
          "id % 3 as id",
          s"parse_json('$goldenFileJsonString') v",
          s"""array(
            parse_json('$goldenFileJsonString'),
            null,
            parse_json('$goldenFileJsonString'),
            null,
            parse_json('$goldenFileJsonString')) as array_of_variants""",
          s"named_struct('v', parse_json('$goldenFileJsonString')) struct_of_variants",
          s"map(id, parse_json('$goldenFileJsonString'), 'nullKey', null) map_of_variants"
        )
        checkAnswer(spark.read.format("delta").load(dir.getAbsolutePath), expected.collect())
      }
    }
  }

  test("variant works with schema evolution") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100, 1, 1)
        .selectExpr("id", "parse_json(cast(id as string)) v")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      spark.range(100, 200, 1, 1)
        .selectExpr(
          "id",
          "parse_json(cast(id as string)) v",
          "parse_json(cast(id as string)) v_two"
        )
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)

      val expected = spark.range(0, 200, 1, 1).selectExpr(
        "id",
        "parse_json(cast(id as string)) v",
        "case when id >= 100 then parse_json(cast(id as string)) else null end v_two"
      )

      val read = spark.read.format("delta").load(path)
      checkAnswer(read, expected.collect())
    }
  }

  test("variant cannot be used as a clustering column") {
    withTable("tbl") {
      val e = intercept[DeltaAnalysisException] {
        sql("CREATE TABLE tbl(v variant) USING DELTA CLUSTER BY (v)")
      }
      checkError(
        e,
        "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
        parameters = Map(
          "columns" -> "v",
          "schema" -> """#root
                         # |-- v: variant (nullable = true)
                         #""".stripMargin('#')
        )
      )
    }
  }

  test("describe history works with variant column") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      // Create and insert should result in two table versions.
      assert(sql("DESCRIBE HISTORY tbl").count() == 2)
    }
  }

  test("describe detail works with variant column") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")

      val tableFeatures = sql("DESCRIBE DETAIL tbl")
        .selectExpr("tableFeatures")
        .collect()(0)
        .getAs[MutableSeq[String]](0)
      assert(tableFeatures.find(f => f == "variantType-dev").nonEmpty)
    }
  }
}
