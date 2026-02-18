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

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FilePartition;

/**
 * Typed CDC InputPartition wrappers that encode the CDC file type at planning time.
 *
 * <p>Instead of inspecting {@link
 * org.apache.spark.sql.execution.datasources.PartitionedFile#otherConstantMetadataColumnValues()}
 * at read time to determine how to override CDC columns, the file-type decision is made during
 * {@link SparkMicroBatchStream#planInputPartitions} by wrapping each {@link FilePartition} in a
 * typed subclass. {@link CDCReaderFactory} then dispatches to the appropriate type-specific
 * ReadFunc based on the partition type.
 *
 * <p>Three subclasses correspond to the three CDC file types:
 *
 * <ul>
 *   <li>{@link Explicit} — AddCDCFile with _change_type in Parquet data
 *   <li>{@link InferredInsert} — AddFile with changeType="insert"
 *   <li>{@link InferredDelete} — RemoveFile with changeType="delete"
 * </ul>
 */
public abstract class CDCInputPartition implements InputPartition {
  private final FilePartition filePartition;

  CDCInputPartition(FilePartition filePartition) {
    this.filePartition = java.util.Objects.requireNonNull(filePartition, "filePartition");
  }

  /** Returns the wrapped FilePartition for SparkPartitionReader to iterate files. */
  public FilePartition filePartition() {
    return filePartition;
  }

  /** Partition for explicit CDC files (AddCDCFile). _change_type is read from Parquet data. */
  public static final class Explicit extends CDCInputPartition {
    public Explicit(FilePartition filePartition) {
      super(filePartition);
    }
  }

  /** Partition for inferred CDC inserts (AddFile with dataChange=true). */
  public static final class InferredInsert extends CDCInputPartition {
    public InferredInsert(FilePartition filePartition) {
      super(filePartition);
    }
  }

  /** Partition for inferred CDC deletes (RemoveFile with dataChange=true). */
  public static final class InferredDelete extends CDCInputPartition {
    public InferredDelete(FilePartition filePartition) {
      super(filePartition);
    }
  }
}
