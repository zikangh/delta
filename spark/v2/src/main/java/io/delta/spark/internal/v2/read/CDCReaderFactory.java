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
package io.delta.spark.internal.v2.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function1;
import scala.collection.Iterator;

/**
 * PartitionReaderFactory for CDC reads that dispatches to type-specific ReadFuncs based on the
 * {@link CDCInputPartition} subtype.
 *
 * <p>Holds three ReadFuncs (one per CDC type: explicit, inferred insert, inferred delete). When
 * {@link #createReader} is called, it downcasts the {@link InputPartition} to the appropriate
 * CDCInputPartition subtype and selects the matching ReadFunc.
 *
 * <p>This replaces the single {@link CDCReadFunc} that had to inspect {@link
 * PartitionedFile#otherConstantMetadataColumnValues()} at read time to determine which columns to
 * override.
 */
public class CDCReaderFactory implements PartitionReaderFactory {
  private final Function1<PartitionedFile, Iterator<InternalRow>> explicitReadFunc;
  private final Function1<PartitionedFile, Iterator<InternalRow>> inferredInsertReadFunc;
  private final Function1<PartitionedFile, Iterator<InternalRow>> inferredDeleteReadFunc;
  private final boolean supportsColumnar;

  public CDCReaderFactory(
      Function1<PartitionedFile, Iterator<InternalRow>> explicitReadFunc,
      Function1<PartitionedFile, Iterator<InternalRow>> inferredInsertReadFunc,
      Function1<PartitionedFile, Iterator<InternalRow>> inferredDeleteReadFunc,
      boolean supportsColumnar) {
    this.explicitReadFunc = java.util.Objects.requireNonNull(explicitReadFunc, "explicitReadFunc");
    this.inferredInsertReadFunc =
        java.util.Objects.requireNonNull(inferredInsertReadFunc, "inferredInsertReadFunc");
    this.inferredDeleteReadFunc =
        java.util.Objects.requireNonNull(inferredDeleteReadFunc, "inferredDeleteReadFunc");
    this.supportsColumnar = supportsColumnar;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    CDCInputPartition cdcPartition = (CDCInputPartition) partition;
    return new SparkPartitionReader<>(selectReadFunc(cdcPartition), cdcPartition.filePartition());
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    CDCInputPartition cdcPartition = (CDCInputPartition) partition;
    return new SparkPartitionReader<>(selectReadFunc(cdcPartition), cdcPartition.filePartition());
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return supportsColumnar;
  }

  private Function1<PartitionedFile, Iterator<InternalRow>> selectReadFunc(
      CDCInputPartition partition) {
    if (partition instanceof CDCInputPartition.Explicit) {
      return explicitReadFunc;
    } else if (partition instanceof CDCInputPartition.InferredInsert) {
      return inferredInsertReadFunc;
    } else if (partition instanceof CDCInputPartition.InferredDelete) {
      return inferredDeleteReadFunc;
    } else {
      throw new IllegalArgumentException(
          "Unknown CDCInputPartition type: " + partition.getClass().getName());
    }
  }
}
