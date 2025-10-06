/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.spark.SparkDsv2TestBase;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.IndexedFile;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.collection.immutable.Map$;

public class SparkMicroBatchStreamTest extends SparkDsv2TestBase {

  private SparkMicroBatchStream microBatchStream;

  @BeforeEach
  void setUp() {
    microBatchStream = new SparkMicroBatchStream(null);
  }

  @Test
  public void testLatestOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.latestOffset());
    assertEquals("latestOffset is not supported", exception.getMessage());
  }

  @Test
  public void testPlanInputPartitions_throwsUnsupportedOperationException() {
    Offset start = null;
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> microBatchStream.planInputPartitions(start, end));
    assertEquals("planInputPartitions is not supported", exception.getMessage());
  }

  @Test
  public void testCreateReaderFactory_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.createReaderFactory());
    assertEquals("createReaderFactory is not supported", exception.getMessage());
  }

  @Test
  public void testInitialOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.initialOffset());
    assertEquals("initialOffset is not supported", exception.getMessage());
  }

  @Test
  public void testDeserializeOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.deserializeOffset("{}"));
    assertEquals("deserializeOffset is not supported", exception.getMessage());
  }

  @Test
  public void testCommit_throwsUnsupportedOperationException() {
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.commit(end));
    assertEquals("commit is not supported", exception.getMessage());
  }

  @Test
  public void testStop_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.stop());
    assertEquals("stop is not supported", exception.getMessage());
  }

  /** Helper method to format IndexedFile for debugging */
  private String formatIndexedFile(IndexedFile file) {
    return String.format(
        "IndexedFile(version=%d, index=%d, hasAdd=%b, shouldSkip=%b)",
        file.version(), file.index(), file.add() != null, file.shouldSkip());
  }

  /** Helper method to format KernelIndexedFile for debugging */
  private String formatKernelIndexedFile(KernelIndexedFile file) {
    return String.format(
        "KernelIndexedFile(version=%d, index=%d, hasAdd=%b, shouldSkip=%b)",
        file.getVersion(), file.getIndex(), file.getAddFile() != null, file.shouldSkip());
  }

  /** Helper method to compare two lists of file changes */
  private void compareFileChanges(
      List<IndexedFile> deltaSourceFiles, List<KernelIndexedFile> kernelFiles) {
    assertEquals(
        deltaSourceFiles.size(),
        kernelFiles.size(),
        String.format(
            "Number of file changes should match between dsv1 (%d) and dsv2 (%d)",
            deltaSourceFiles.size(), kernelFiles.size()));

    for (int i = 0; i < deltaSourceFiles.size(); i++) {
      IndexedFile deltaFile = deltaSourceFiles.get(i);
      KernelIndexedFile kernelFile = kernelFiles.get(i);

      assertEquals(
          deltaFile.version(),
          kernelFile.getVersion(),
          String.format(
              "Version mismatch at index %d: dsv1=%d, dsv2=%d",
              i, deltaFile.version(), kernelFile.getVersion()));

      assertEquals(
          deltaFile.index(),
          kernelFile.getIndex(),
          String.format(
              "Index mismatch at index %d: dsv1=%d, dsv2=%d",
              i, deltaFile.index(), kernelFile.getIndex()));

      String deltaPath = deltaFile.add() != null ? deltaFile.add().path() : null;
      String kernelPath =
          kernelFile.getAddFile() != null ? kernelFile.getAddFile().getPath() : null;

      if (deltaPath != null || kernelPath != null) {
        assertEquals(
            deltaPath,
            kernelPath,
            String.format(
                "AddFile path mismatch at index %d: dsv1=%s, dsv2=%s", i, deltaPath, kernelPath));
      }

      assertEquals(
          deltaFile.shouldSkip(),
          kernelFile.shouldSkip(),
          String.format(
              "shouldSkip flag mismatch at index %d: dsv1=%b, dsv2=%b",
              i, deltaFile.shouldSkip(), kernelFile.shouldSkip()));
    }
  }

  /**
   * Parameterized test that verifies parity between DSv1 DeltaSource.getFileChanges and DSv2
   * SparkMicroBatchStream.getFileChanges using Delta Kernel APIs.
   */
  @ParameterizedTest
  @MethodSource("getFileChangesParameters")
  public void testGetFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<Long> endVersion,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    // Use unique table name per test instance to avoid conflicts
    String testTableName =
        "test_file_changes_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions of data (versions 1-5, version 0 is the CREATE TABLE)
    // Insert 100 rows per commit to potentially trigger multiple batches
    for (int i = 0; i < 5; i++) {
      StringBuilder insertValues = new StringBuilder();
      for (int j = 0; j < 100; j++) {
        if (j > 0) insertValues.append(", ");
        int id = i * 100 + j;
        insertValues.append(String.format("(%d, 'User%d')", id, id));
      }
      spark.sql(String.format("INSERT INTO %s VALUES %s", testTableName, insertValues.toString()));

      // Add protocol change after version 2 (will be version 3)
      if (i == 2) {
        spark.sql(
            String.format(
                "ALTER TABLE %s SET TBLPROPERTIES ('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
                testTableName));
      }

      // Add metadata change after version 4 (will be version 5)
      if (i == 4) {
        spark.sql(String.format("ALTER TABLE %s ADD COLUMN (age INT)", testTableName));
      }
    }
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath);

    // dsv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.sources.DeltaSource deltaSource =
        createDeltaSource(deltaLog, testTablePath);

    scala.Option<DeltaSourceOffset> scalaEndOffset = scala.Option.empty();
    if (endVersion.isPresent()) {
      scalaEndOffset =
          scala.Option.apply(
              new DeltaSourceOffset(
                  deltaLog.tableId(), // reservoirId
                  endVersion.get(), // reservoirVersion
                  DeltaSourceOffset.END_INDEX(), // index (use END_INDEX for version-end offset)
                  false)); // isInitialSnapshot
    }
    ClosableIterator<IndexedFile> deltaChanges =
        deltaSource.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, scalaEndOffset, true);
    List<IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // dsv2 SparkMicroBatchStream
    Option<DeltaSourceOffset> endOffsetOption = scalaEndOffset;
    try (CloseableIterator<KernelIndexedFile> kernelChanges =
        stream.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, endOffsetOption, true)) {
      List<KernelIndexedFile> kernelFilesList = new ArrayList<>();
      while (kernelChanges.hasNext()) {
        kernelFilesList.add(kernelChanges.next());
      }

      // Print indexed files for reference
      System.out.println(
          String.format(
              "\n=== Test: %s (fromVersion=%d, fromIndex=%d, endVersion=%s) ===",
              testDescription,
              fromVersion,
              fromIndex,
              endVersion.map(String::valueOf).orElse("none")));
      System.out.println(String.format("DSv1 DeltaSource: %d files", deltaFilesList.size()));
      for (int i = 0; i < deltaFilesList.size(); i++) {
        IndexedFile f = deltaFilesList.get(i);
        String path = f.add() != null ? f.add().path() : null;
        System.out.println(
            String.format(
                "  [%d] version=%d, index=%d, hasAdd=%b, shouldSkip=%b, path=%s",
                i, f.version(), f.index(), f.add() != null, f.shouldSkip(), path));
      }
      System.out.println(
          String.format("DSv2 SparkMicroBatchStream: %d files", kernelFilesList.size()));
      for (int i = 0; i < kernelFilesList.size(); i++) {
        KernelIndexedFile f = kernelFilesList.get(i);
        String path = f.getAddFile() != null ? f.getAddFile().getPath() : null;
        System.out.println(
            String.format(
                "  [%d] version=%d, index=%d, hasAdd=%b, shouldSkip=%b, path=%s",
                i, f.getVersion(), f.getIndex(), f.getAddFile() != null, f.shouldSkip(), path));
      }

      // Compare results
      compareFileChanges(deltaFilesList, kernelFilesList);
    }
  }

  /** Provides test parameters for the parameterized getFileChanges test. */
  private static Stream<Arguments> getFileChangesParameters() {
    boolean isInitialSnapshot = false;
    return Stream.of(
        // Arguments: fromVersion, fromIndex, isInitialSnapshot, endVersion, testDescription
        // Basic cases: fromIndex = -1, no endVersion
        Arguments.of(0L, -1L, isInitialSnapshot, Optional.empty(), "Basic: from v0"),
        Arguments.of(1L, -1L, isInitialSnapshot, Optional.empty(), "Basic: from v1"),
        Arguments.of(3L, -1L, isInitialSnapshot, Optional.empty(), "Basic: from middle v3"),

        // With fromIndex > -1
        Arguments.of(0L, 0L, isInitialSnapshot, Optional.empty(), "With fromIndex: v0 id:0"),
        Arguments.of(1L, 1L, isInitialSnapshot, Optional.empty(), "With fromIndex: v1 id:1"),

        // With endVersion
        Arguments.of(0L, -1L, isInitialSnapshot, Optional.of(2L), "With endVersion: v0 to v2"),
        Arguments.of(1L, -1L, isInitialSnapshot, Optional.of(3L), "With endVersion: v1 to v3"),
        Arguments.of(1L, 1L, isInitialSnapshot, Optional.of(3L), "Complex: v1 id:1 to v3"),
        Arguments.of(2L, 0L, isInitialSnapshot, Optional.of(4L), "Complex: v2 id:0 to v4"),

        // FromVersion/fromIndex boundary filtering
        Arguments.of(
            1L,
            0L,
            isInitialSnapshot,
            Optional.empty(),
            "Edge: start at v1 id:0 (excludes id:0, includes id:1+)"),
        Arguments.of(
            2L,
            5L,
            isInitialSnapshot,
            Optional.empty(),
            "Edge: start at v2 id:5 (high index, should handle gracefully)"),

        // EndVersion/endIndex boundary filtering
        Arguments.of(
            1L, -1L, isInitialSnapshot, Optional.of(3L), "Edge: end at v3 (includes all of v3)"),
        Arguments.of(
            0L, -1L, isInitialSnapshot, Optional.of(1L), "Edge: end at v1 (includes all of v1)"),

        // Combining both boundaries
        Arguments.of(
            1L, 0L, isInitialSnapshot, Optional.of(2L), "Edge: from v1 id:0 to v2 (tight range)"),
        Arguments.of(2L, 2L, isInitialSnapshot, Optional.of(3L), "Edge: from v2 id:2 to v3"),

        // Same version start and end (narrow range within single version)
        Arguments.of(
            2L,
            -1L,
            isInitialSnapshot,
            Optional.of(2L),
            "Edge: single version v2 (start and end at same version)"));
  }

  /** Helper method to create a DeltaSource for testing */
  private org.apache.spark.sql.delta.sources.DeltaSource createDeltaSource(
      DeltaLog deltaLog, String tablePath) {
    DeltaOptions options = new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
    scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.expressions.Expression> emptySeq =
        scala.collection.JavaConverters.asScalaBuffer(
                new java.util.ArrayList<org.apache.spark.sql.catalyst.expressions.Expression>())
            .toList();
    return new org.apache.spark.sql.delta.sources.DeltaSource(
        spark,
        deltaLog,
        Option.empty(),
        options,
        deltaLog.update(false, Option.empty(), Option.empty()),
        tablePath + "/_checkpoint",
        Option.empty(),
        emptySeq);
  }

  @Test
  public void testErrorOnRemoveFileWithDataChange(@TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_delete_error_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create initial data (version 1)
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", testTableName));

    // Add more data (version 2)
    spark.sql(String.format("INSERT INTO %s VALUES (3, 'User3'), (4, 'User4')", testTableName));

    // Delete some data (version 3) - this creates RemoveFile actions with dataChange=true
    spark.sql(String.format("DELETE FROM %s WHERE id = 1", testTableName));

    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath);

    // Try to read from version 0, which should include the DELETE commit at version 3
    long fromVersion = 0L;
    long fromIndex = -1L;
    boolean isInitialSnapshot = false;
    scala.Option<DeltaSourceOffset> endOffset = scala.Option.empty();

    // Should throw UnsupportedOperationException when it encounters the DELETE
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              try (CloseableIterator<KernelIndexedFile> kernelChanges =
                  stream.getFileChanges(
                      fromVersion, fromIndex, isInitialSnapshot, endOffset, true)) {
                // Consume the iterator to trigger validation
                while (kernelChanges.hasNext()) {
                  KernelIndexedFile file = kernelChanges.next();
                  System.out.println(
                      String.format(
                          "  Read: version=%d, index=%d, hasAdd=%b",
                          file.getVersion(), file.getIndex(), file.getAddFile() != null));
                }
              }
            });
  }
}
