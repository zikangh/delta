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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.{DeltaCDCStreamSuiteBase, DeltaOptions}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.DataFrame
import io.delta.tables._

/**
 * Test suite that runs CDC streaming tests using the V2 connector.
 *
 * Note: Tests inherited from DeltaCDCStreamSuiteBase use .format("delta").load()
 * which doesn't route through V2. Those tests are marked as shouldFail.
 * New tests in this class use loadCDCStream() which routes through V2.
 */
class DeltaV2CDCStreamSuite extends DeltaCDCStreamSuiteBase with V2ForceTest {

  import testImplicits._

  override protected def useDsv2: Boolean = true

  /** Load CDC stream using table format (routes through V2 via ApplyV2Streaming rule) */
  protected def loadCDCStream(
      path: String,
      options: Map[String, String] = Map.empty): DataFrame = {
    val reader = spark.readStream
    (options + (DeltaOptions.CDC_READ_OPTION -> "true")).foreach {
      case (k, v) => reader.option(k, v)
    }
    reader.table(s"delta.`$path`")
  }

  // ============================================================================
  // New V2-specific CDC tests using loadCDCStream()
  // ============================================================================

  test("V2 CDC - initial snapshot with inserts") {
    withTempDir { inputDir =>
      // Create table with CDC enabled
      Seq(1, 2, 3).toDF("value").write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .save(inputDir.getAbsolutePath)

      val df = loadCDCStream(inputDir.getCanonicalPath)
      // Debug: print the schema to see if CDC columns are present
      // scalastyle:off println
      println(s"=== CDC Stream Schema: ${df.schema.treeString} ===")
      df.schema.fields.foreach(f => println(s"  Field: ${f.name} - ${f.dataType}"))
      // scalastyle:on println

      val dfWithoutTimestamp = df.drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(dfWithoutTimestamp)(
        ProcessAllAvailable(),
        CheckAnswer(
          (1, "insert", 0L),
          (2, "insert", 0L),
          (3, "insert", 0L)
        )
      )
    }
  }

  test("V2 CDC - delete generates delete records") {
    withTempDir { inputDir =>
      // Create table with CDC enabled
      Seq(1, 2, 3).toDF("value").write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .save(inputDir.getAbsolutePath)

      val deltaTable = DeltaTable.forPath(inputDir.getAbsolutePath)

      // Delete a row
      deltaTable.delete("value = 2")  // version 1

      val df = loadCDCStream(inputDir.getCanonicalPath, Map("startingVersion" -> "1"))
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer(
          (2, "delete", 1L)
        )
      )
    }
  }

  test("V2 CDC - startingVersion = latest") {
    withTempDir { inputDir =>
      // Create table with CDC enabled
      Seq(1, 2).toDF("value").write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .save(inputDir.getAbsolutePath)

      val df = loadCDCStream(inputDir.getCanonicalPath, Map("startingVersion" -> "latest"))
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer(), // No data since we're starting from latest
        Execute { _ =>
          Seq(3).toDF("value").write
            .format("delta")
            .mode("append")
            .save(inputDir.getAbsolutePath)  // version 1
        },
        ProcessAllAvailable(),
        CheckAnswer((3, "insert", 1L))
      )
    }
  }

  // ============================================================================
  // shouldFail configuration for inherited tests
  // ============================================================================

  // Tests expected to pass with V2 connector via path-based streaming.
  private lazy val shouldPassTests: Set[String] = Set(
    // ========== Core CDC streaming ==========
    "startingVersion = latest",
    "user provided startingVersion",
    "cdc streams should respect checkpoint",
    "cdc streams should work starting from AddCDCFile",

    // ========== CDC with RemoveFile / inferred CDC ==========
    "cdc streams with noop merge",
    "cdc streams should be able to get offset when there only RemoveFiles",
    "cdc streams should work starting from RemoveFile",

    // ========== Rate limiting ==========
    "rateLimit - maxFilesPerTrigger - starting from initial snapshot",
    "rateLimit - maxBytesPerTrigger - starting from initial snapshot",
    "rateLimit - maxFilesPerTrigger - overall",
    "rateLimit - maxBytesPerTrigger - overall",
    "rateLimit - maxFilesPerTrigger - should not deadlock",
    "rateLimit - maxBytesPerTrigger - should not deadlock",
    "maxFilesPerTrigger - 2 successive AddCDCFile commits",
    "maxFilesPerTrigger with Trigger.AvailableNow respects read limits"
  )

  // Tests expected to fail with V2 connector.
  private lazy val shouldFailTests: Set[String] = Set(
    // ========== Unsupported option: startingTimestamp ==========
    "user provided startingTimestamp",
    "starting[Version/Timestamp] > latest version",
    "check starting[Version/Timestamp] > latest version without error",
    "startingVersion and startingTimestamp are both set",

    // ========== Unsupported option: excludeRegex ==========
    "excludeRegex works with cdc",
    "excludeRegex on cdcPath should not return Add/RemoveFiles",

    // ========== V1 internals (DeltaSourceOffset, ManualUpdate, etc.) ==========
    "CDC initial snapshot should end at base index of next version",
    "streams updating latest offset with readChangeFeed=true",
    "streams updating latest offset with readChangeFeed=false",
    "should not attempt to read a non exist version",
    "schema check for cdc stream",

    // ========== ALTER TABLE not supported in STRICT V2 mode ==========
    "no startingVersion should result fetch the entire snapshot",

    // ========== SparkTable does not support deletes ==========
    "double delete-only on the same file"
  )

  override protected def shouldFail(testName: String): Boolean = {
    // New V2-specific tests added in this class should always pass
    if (testName.startsWith("V2 CDC")) return false

    val inPass = shouldPassTests.contains(testName)
    val inFail = shouldFailTests.contains(testName)
    assert(inPass || inFail,
      s"Test '$testName' not in shouldPassTests or shouldFailTests. Please categorize it.")
    assert(!(inPass && inFail),
      s"Test '$testName' in both shouldPassTests and shouldFailTests")
    inFail
  }
}
