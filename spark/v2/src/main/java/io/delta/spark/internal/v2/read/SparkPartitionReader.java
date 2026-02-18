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

import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;

public class SparkPartitionReader<T> implements PartitionReader<T> {
  // Function that produces a record iterator for a given file.
  // May be a base Parquet ReadFunc, or a decorated one (e.g., CDCReadFunc for CDC reads).
  private final Function1<PartitionedFile, Iterator<InternalRow>> readFunc;
  private final FilePartition partition;

  // Index of the next file to read within the partition.
  private int currentFileIndex = 0;

  // Iterator returned by readFunc.apply(file). May be a CDCOverrideIterator (for CDC)
  // or a RecordReaderIterator (for non-CDC). We track Iterator<Object> because the actual
  // element type is either InternalRow or ColumnarBatch depending on vectorization.
  private Iterator<Object> currentIterator = null;

  public SparkPartitionReader(
      Function1<PartitionedFile, Iterator<InternalRow>> readFunc, FilePartition partition) {
    this.readFunc = java.util.Objects.requireNonNull(readFunc, "readFunc");
    this.partition = java.util.Objects.requireNonNull(partition, "partition");
  }

  @Override
  public boolean next() throws IOException {
    // Advance to the next available record, opening readers as needed and closing exhausted ones.
    while (true) {
      if (currentIterator != null && currentIterator.hasNext()) {
        return true;
      }

      closeCurrentIterator();

      if (currentFileIndex >= partition.files().length) {
        return false;
      }

      final PartitionedFile file = partition.files()[currentFileIndex++];

      @SuppressWarnings("unchecked")
      Iterator<Object> it = (Iterator<Object>) (Iterator<?>) readFunc.apply(file);
      currentIterator = it;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get() {
    if (currentIterator == null) {
      throw new IllegalStateException("No current record. Call next() before get().");
    }
    return (T) currentIterator.next();
  }

  @Override
  public void close() throws IOException {
    closeCurrentIterator();
  }

  /** Close the current iterator if it implements AutoCloseable. */
  private void closeCurrentIterator() throws IOException {
    if (currentIterator != null) {
      if (currentIterator instanceof AutoCloseable) {
        try {
          ((AutoCloseable) currentIterator).close();
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      currentIterator = null;
    }
  }
}
