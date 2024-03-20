/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet

import java.math.BigDecimal

import org.apache.spark.sql.DataFrame

import io.delta.golden.GoldenTableUtils.goldenTableFile
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite
class ParquetFileReaderSuite extends AnyFunSuite
  with ParquetSuiteBase with VectorTestUtils with ExpressionTestUtils {

  test("decimals encoded using dictionary encoding ") {
    // Below golden tables contains three decimal columns
    // each stored in a different physical format: int32, int64 and fixed binary
    val decimalDictFileV1 = goldenTableFile("parquet-decimal-dictionaries-v1").getAbsolutePath
    val decimalDictFileV2 = goldenTableFile("parquet-decimal-dictionaries-v2").getAbsolutePath

    val expResult = (0 until 1000000).map { i =>
      TestRow(i, BigDecimal.valueOf(i%5), BigDecimal.valueOf(i%6), BigDecimal.valueOf(i%2))
    }

    val readSchema = tableSchema(decimalDictFileV1)

    for (file <- Seq(decimalDictFileV1, decimalDictFileV2)) {
      val actResult = readParquetFilesUsingKernel(file, readSchema)

      checkAnswer(actResult, expResult)
    }
  }

  test("large scale decimal type file") {
    val largeScaleDecimalTypesFile = goldenTableFile("parquet-decimal-type").getAbsolutePath

    def expand(n: BigDecimal): BigDecimal = {
      n.scaleByPowerOfTen(5).add(n)
    }

    val expResult = (0 until 99998).map { i =>
      if (i % 85 == 0) {
        val n = BigDecimal.valueOf(i)
        TestRow(i, n.movePointLeft(1).setScale(1), n.setScale(5), n.setScale(5))
      } else {
        val negation = if (i % 33 == 0) -1 else 1
        val n = BigDecimal.valueOf(i*negation)
        TestRow(
          i,
          n.movePointLeft(1),
          expand(n).movePointLeft(5),
          expand(expand(expand(n))).movePointLeft(5)
        )
      }
    }

    val readSchema = tableSchema(largeScaleDecimalTypesFile)

    val actResult = readParquetFilesUsingKernel(largeScaleDecimalTypesFile, readSchema)

    checkAnswer(actResult, expResult)
  }

  private val ALL_TYPES_FILE = goldenTableFile("parquet-all-types").getAbsolutePath

  test("read all types of data") {
    val readSchema = tableSchema(ALL_TYPES_FILE)

    val actResult = readParquetFilesUsingKernel(ALL_TYPES_FILE, readSchema)

    val expResult = readParquetFilesUsingSpark(ALL_TYPES_FILE, readSchema)

    checkAnswer(actResult, expResult)
  }

  test("read subset of columns") {
    val readSchema = new StructType()
      .add("byteType", ByteType.BYTE)
      .add("booleanType", BooleanType.BOOLEAN)
      .add("stringType", StringType.STRING)
      .add("dateType", DateType.DATE)
      .add("nested_struct", new StructType()
        .add("aa", StringType.STRING)
        .add("ac", new StructType().add("aca", IntegerType.INTEGER)))
      .add("array_of_prims", new ArrayType(IntegerType.INTEGER, true))

    val actResult = readParquetFilesUsingKernel(ALL_TYPES_FILE, readSchema)

    val expResult = readParquetFilesUsingSpark(ALL_TYPES_FILE, readSchema)

    checkAnswer(actResult, expResult)
  }

  test("read subset of columns with missing columns in file") {
    val readSchema = new StructType()
      .add("booleanType", BooleanType.BOOLEAN)
      .add("integerType", IntegerType.INTEGER)
      .add("missing_column_struct", new StructType().add("ab", IntegerType.INTEGER))
      .add("longType", LongType.LONG)
      .add("missing_column_primitive", DateType.DATE)
      .add("nested_struct", new StructType()
        .add("aa", StringType.STRING)
        .add("ac", new StructType().add("aca", IntegerType.INTEGER)))

    val actResult = readParquetFilesUsingKernel(ALL_TYPES_FILE, readSchema)

    val expResult = readParquetFilesUsingSpark(ALL_TYPES_FILE, readSchema)

    checkAnswer(actResult, expResult)
  }

  test("request row indices") {
    val readSchema = new StructType()
      .add("id", LongType.LONG)
      .add(StructField.METADATA_ROW_INDEX_COLUMN)

    val path = getTestResourceFilePath("parquet-basic-row-indexes")
    val actResult1 = readParquetFilesUsingKernel(path, readSchema)
    val expResult1 = (0L until 30L)
      .map(i => TestRow(i, if (i < 10) i else if (i < 20) i - 10L else i - 20L))

    checkAnswer(actResult1, expResult1)

    // File with multiple row-groups [0, 20000) where rowIndex = id
    val filePath = getTestResourceFilePath("parquet/")
    val actResult2 = readParquetFilesUsingKernel(filePath, readSchema)
    val expResult2 = (0L until 20000L).map(i => TestRow(i, i))

    checkAnswer(actResult2, expResult2)
  }

  /**
   * Writes a table using Spark, reads it back using the Delta Kernel implementation, and asserts
   * that the results are the same.
   */
  private def testRead(testName: String)(df: => DataFrame): Unit = {
    test(testName) {
      withTable("test_table") {
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable("test_table")
        val path = spark.sql("describe table extended `test_table`")
          .where("col_name = 'Location'")
          .collect()(0)
          .getString(1)
          .replace("file:", "")

        val kernelSchema = tableSchema(path)
        val actResult = readParquetFilesUsingKernel(path, kernelSchema)
        val expResult = readParquetFilesUsingSpark(path, kernelSchema)
        checkAnswer(actResult, expResult)
      }
    }
  }

  testRead("basic read variant") {
    spark.range(0, 10, 1, 1).selectExpr(
      "parse_json(cast(id as string)) as basic_v",
      "named_struct('v', parse_json(cast(id as string))) as struct_v",
      """array(
        parse_json(cast(id as string)),
        parse_json(cast(id as string)),
        parse_json(cast(id as string))
      ) as array_v""",
      "map('test', parse_json(cast(id as string))) as map_value_v",
      "map(parse_json(cast(id as string)), parse_json(cast(id as string))) as map_key_v"
    )
  }

  testRead("basic null variant") {
    spark.range(0, 10, 1, 1).selectExpr(
      "cast(null as variant) basic_v",
      "named_struct('v', cast(null as variant)) as struct_v",
      """array(
        parse_json(cast(id as string)),
        parse_json(cast(id as string)),
        null
      ) as array_v""",
      "map('test', cast(null as variant)) as map_value_v"
    )
  }
}
