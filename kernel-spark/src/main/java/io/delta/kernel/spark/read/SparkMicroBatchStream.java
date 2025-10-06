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

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import scala.Option;

public class SparkMicroBatchStream implements MicroBatchStream {

  private final Engine engine;
  private final String tablePath;

  public SparkMicroBatchStream(String tablePath) {
    this.tablePath = tablePath;
    Configuration hadoopConf = new Configuration();
    this.engine = DefaultEngine.create(hadoopConf);
  }

  ////////////
  // offset //
  ////////////

  @Override
  public Offset initialOffset() {
    throw new UnsupportedOperationException("initialOffset is not supported");
  }

  @Override
  public Offset latestOffset() {
    throw new UnsupportedOperationException("latestOffset is not supported");
  }

  @Override
  public Offset deserializeOffset(String json) {
    throw new UnsupportedOperationException("deserializeOffset is not supported");
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    throw new UnsupportedOperationException("planInputPartitions is not supported");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("createReaderFactory is not supported");
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    throw new UnsupportedOperationException("commit is not supported");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop is not supported");
  }

  ////////////////////
  // getFileChanges //
  ////////////////////

  /**
   * Get file changes between fromVersion/fromIndex and endOffset. This is the Kernel-based
   * implementation of DeltaSource.getFileChanges.
   *
   * <p>Package-private for testing.
   *
   * @param fromVersion Starting version (inclusive)
   * @param fromIndex Starting index within fromVersion
   * @param isInitialSnapshot Whether we're reading the initial snapshot
   * @param endOffset Optional end offset to bound the read
   * @param verifyMetadataAction Whether to verify metadata actions
   * @return Iterator of KernelIndexedFile representing the changes
   */
  CloseableIterator<KernelIndexedFile> getFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Option<DeltaSourceOffset> endOffset,
      boolean verifyMetadataAction) {

    CloseableIterator<KernelIndexedFile> result;

    if (isInitialSnapshot) {
      // TODO(M1): Implement initial snapshot
      throw new UnsupportedOperationException("initial snapshot is not supported yet");
    } else {
      result = filterDeltaLogs(fromVersion, endOffset, verifyMetadataAction);
    }

    // Start from fromVersion & fromIndex
    // Skip files before fromVersion or at fromVersion but with index <= fromIndex
    result =
        result.filter(
            file ->
                file.getVersion() > fromVersion
                    || (file.getVersion() == fromVersion && file.getIndex() > fromIndex));

    // Stop at endOffset
    if (endOffset.isDefined()) {
      DeltaSourceOffset bound = endOffset.get();
      result =
          result.takeWhile(
              file ->
                  file.getVersion() < bound.reservoirVersion()
                      || (file.getVersion() == bound.reservoirVersion()
                          && file.getIndex() <= bound.index()));
    }

    return result;
  }

  private CloseableIterator<KernelIndexedFile> filterDeltaLogs(
      long startVersion, Option<DeltaSourceOffset> endOffset, boolean verifyMetadataAction) {
    List<KernelIndexedFile> allIndexedFiles = new ArrayList<>();
    CommitRangeBuilder builder =
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(startVersion));
    if (endOffset.isDefined()) {
      builder =
          builder.withEndBoundary(
              CommitRangeBuilder.CommitBoundary.atVersion(endOffset.get().reservoirVersion()));
    }
    CommitRange commitRange = builder.build(engine);

    // Load the start snapshot for protocol validation
    Snapshot startSnapshot =
        TableManager.loadSnapshot(tablePath).atVersion(startVersion).build(engine);

    Set<DeltaAction> actionSet = new HashSet<>();
    actionSet.add(DeltaAction.ADD);
    actionSet.add(DeltaAction.REMOVE);
    actionSet.add(DeltaAction.METADATA);
    actionSet.add(DeltaAction.PROTOCOL);
    try (CloseableIterator<ColumnarBatch> actionsIter =
        commitRange.getActions(engine, startSnapshot, actionSet)) {
      // Each batch is guaranteed to have the same commit version.
      // A version can be split across multiple batches.
      long currentVersion = -1;
      long currentIndex = 0;
      List<KernelIndexedFile> currentVersionFiles = new ArrayList<>();

      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        if (batch.getSize() == 0) {
          continue;
        }
        long version = batch.getColumnVector(0).getLong(0);
        validateCommit(batch, version, endOffset, verifyMetadataAction);

        if (currentVersion != -1 && version != currentVersion) {
          // Process the previous version's files
          List<KernelIndexedFile> versionFilesWithSentinels =
              addBeginAndEndIndexOffsetsForVersion(currentVersion, currentVersionFiles);
          allIndexedFiles.addAll(versionFilesWithSentinels);

          // Reset for the new version
          currentVersionFiles = new ArrayList<>();
          currentIndex = 0;
        }

        // Update current version tracking
        currentVersion = version;
        List<KernelIndexedFile> batchFiles =
            extractIndexedFilesFromBatch(batch, version, currentIndex, false);

        // Update currentIndex to continue from where this batch left off
        // This ensures indices are sequential across multiple batches for the same version
        currentIndex += batchFiles.size();
        currentVersionFiles.addAll(batchFiles);
      }

      // Process the last version's files if any
      if (currentVersion != -1) {
        List<KernelIndexedFile> versionFilesWithSentinels =
            addBeginAndEndIndexOffsetsForVersion(currentVersion, currentVersionFiles);
        allIndexedFiles.addAll(versionFilesWithSentinels);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read commit range", e);
    }
    return Utils.toCloseableIterator(allIndexedFiles.iterator());
  }

  /**
   * Validate a commit and decide if it should be skipped. This mimics
   * DeltaSource.validateCommitAndDecideSkipping in Scala.
   *
   * @throws RuntimeException if the commit is invalid
   */
  private void validateCommit(
      ColumnarBatch batch,
      long version,
      Option<DeltaSourceOffset> endOffsetOpt,
      boolean verifyMetadataAction) {
    // If endOffset is at the beginning of this version, exit early.
    if (endOffsetOpt.isDefined()) {
      DeltaSourceOffset endOffset = endOffsetOpt.get();
      if (endOffset.reservoirVersion() == version
          && endOffset.index() == DeltaSourceOffset.BASE_INDEX()) {
        return;
      }
    }

    Optional<Metadata> metadataAction = Optional.empty();

    int numRows = batch.getSize();
    // TODO(M2): Implement ignoreChanges & skipChangeCommits & ignoreDeletes (legacy)
    for (int rowId = 0; rowId < numRows; rowId++) {
      // RULE 1: If commit has RemoveFile(dataChange=true), fail this stream.
      Optional<RemoveFile> removeOpt = DeltaActionUtils.getDataChangeRemove(batch, rowId);
      if (removeOpt.isPresent()) {
        RemoveFile removeFile = removeOpt.get();
        Throwable error =
            DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFile.getPath(), tablePath);
        if (error instanceof RuntimeException) {
          throw (RuntimeException) error;
        } else {
          throw new RuntimeException(error);
        }
      } else if (!metadataAction.isPresent()) {
        Optional<Metadata> metadataOpt = DeltaActionUtils.getMetadata(batch, rowId);
        if (metadataOpt.isPresent()) {
          // RULE 2: If commit has a read-incompatible Metadata action, fail this stream.
          // TODO(M1): Implement checkReadIncompatibleSchemaChanges
          metadataAction = metadataOpt;
        }
      }
    }
  }

  /**
   * Extract KernelIndexedFiles from a batch of actions for a given version. Converts Kernel Rows to
   * Kernel AddFile and RemoveFile objects.
   */
  private List<KernelIndexedFile> extractIndexedFilesFromBatch(
      ColumnarBatch batch, long version, long startIndex, boolean shouldSkipCommit) {
    List<KernelIndexedFile> indexedFiles = new ArrayList<>();
    if (shouldSkipCommit) {
      return indexedFiles;
    }

    long index = startIndex;
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      Optional<AddFile> addOpt = DeltaActionUtils.getDataChangeAdd(batch, rowId);
      if (addOpt.isPresent()) {
        AddFile addFile = addOpt.get();
        indexedFiles.add(
            new KernelIndexedFile(
                version, index++, addFile, /* remove= */ null, /* shouldSkip= */ false));
      }
    }

    return indexedFiles;
  }

  /**
   * Adds BEGIN and END sentinel KernelIndexedFiles for a version.
   *
   * <p>This mimics DeltaSource.addBeginAndEndIndexOffsetsForVersion and
   * getMetadataOrProtocolChangeIndexedFileIterator.
   */
  private List<KernelIndexedFile> addBeginAndEndIndexOffsetsForVersion(
      long version, List<KernelIndexedFile> dataFiles) {

    List<KernelIndexedFile> result = new ArrayList<>();

    // Add BEGIN sentinel
    result.add(new KernelIndexedFile(version, DeltaSourceOffset.BASE_INDEX(), null, null, false));

    // TODO(M2): For now, we don't track metadata changes (trackingMetadataChange=false by default)
    // Full implementation should check trackingMetadataChange flag and compare with stream
    // metadata.

    result.addAll(dataFiles);

    // Add END sentinel
    result.add(new KernelIndexedFile(version, DeltaSourceOffset.END_INDEX(), null, null, false));

    return result;
  }
}
