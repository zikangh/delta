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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * A ReadFunc decorator that overrides CDC metadata columns with per-file constants.
 *
 * <p>Wraps the base Parquet ReadFunc to intercept each file's iterator and override the CDC columns
 * (_change_type, _commit_version, _commit_timestamp) with constants extracted from {@link
 * PartitionedFile#otherConstantMetadataColumnValues()}. This follows the same decorator pattern
 * used for Deletion Vectors in DeltaParquetFileFormatBase.
 *
 * <p>For each file, the decorator:
 *
 * <ol>
 *   <li>Calls the delegate ReadFunc to get the base iterator
 *   <li>Extracts per-file CDC constants from the PartitionedFile metadata
 *   <li>Returns a CDCOverrideIterator that wraps each record with CDC column overrides
 * </ol>
 */
public class CDCReadFunc extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>>
    implements java.io.Serializable {
  private final Function1<PartitionedFile, Iterator<InternalRow>> delegate;
  private final int changeTypeColIndex;
  private final int commitVersionColIndex;
  private final int commitTimestampColIndex;

  public CDCReadFunc(
      Function1<PartitionedFile, Iterator<InternalRow>> delegate,
      int changeTypeColIndex,
      int commitVersionColIndex,
      int commitTimestampColIndex) {
    this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
    this.changeTypeColIndex = changeTypeColIndex;
    this.commitVersionColIndex = commitVersionColIndex;
    this.commitTimestampColIndex = commitTimestampColIndex;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    Iterator<InternalRow> baseIterator = delegate.apply(file);
    try {
      // Extract per-file CDC constants from PartitionedFile metadata
      scala.collection.immutable.Map<String, Object> constants =
          file.otherConstantMetadataColumnValues();

      Object changeType = constants.getOrElse(SparkMicroBatchStream.CDC_TYPE_COLUMN, () -> null);
      UTF8String cdcChangeType =
          changeType != null ? UTF8String.fromString(changeType.toString()) : null;

      Object commitVersion =
          constants.getOrElse(SparkMicroBatchStream.CDC_COMMIT_VERSION, () -> null);
      long cdcCommitVersion = commitVersion != null ? ((Number) commitVersion).longValue() : 0;

      Object commitTimestamp =
          constants.getOrElse(SparkMicroBatchStream.CDC_COMMIT_TIMESTAMP, () -> null);
      long cdcCommitTimestampMicros =
          commitTimestamp instanceof Timestamp
              ? DateTimeUtils.fromJavaTimestamp((Timestamp) commitTimestamp)
              : 0;

      // CDCOverrideIterator uses Object as its element type to avoid JVM checkcast instructions
      // that would fail when the base iterator produces ColumnarBatch (vectorized mode).
      // The cast is safe due to type erasure — at runtime Iterator<InternalRow> and
      // Iterator<Object> are the same type.
      @SuppressWarnings("unchecked")
      Iterator<InternalRow> result =
          (Iterator<InternalRow>)
              (Iterator<?>)
                  new CDCOverrideIterator(
                      baseIterator,
                      changeTypeColIndex,
                      commitVersionColIndex,
                      commitTimestampColIndex,
                      cdcChangeType,
                      cdcCommitVersion,
                      cdcCommitTimestampMicros);
      return result;
    } catch (Exception e) {
      // On error during setup, close base iterator (following DV pattern's try-catch)
      if (baseIterator instanceof AutoCloseable) {
        try {
          ((AutoCloseable) baseIterator).close();
        } catch (Exception closeEx) {
          e.addSuppressed(closeEx);
        }
      }
      throw e;
    }
  }

  /**
   * Iterator wrapper that overrides CDC columns in each record with per-file constants. Handles
   * both row-based (InternalRow) and columnar (ColumnarBatch) records.
   *
   * <p>Uses Object as element type to avoid JVM checkcast instructions — the ReadFunc signature
   * uses Iterator&lt;InternalRow&gt; but vectorized mode actually produces ColumnarBatch elements.
   */
  static class CDCOverrideIterator extends scala.collection.AbstractIterator<Object>
      implements Closeable {
    private final Iterator<?> base;
    private final int changeTypeColIndex;
    private final int commitVersionColIndex;
    private final int commitTimestampColIndex;
    private final UTF8String cdcChangeType;
    private final long cdcCommitVersion;
    private final long cdcCommitTimestampMicros;

    // Reusable wrapper row for overriding CDC columns (avoids per-row allocation)
    private CDCOverrideRow cdcOverrideRow;

    // Track constant vectors we create so we can close them (but NOT the reader's vectors)
    private ColumnVector prevChangeTypeVector;
    private ColumnVector prevCommitVersionVector;
    private ColumnVector prevCommitTimestampVector;

    CDCOverrideIterator(
        Iterator<?> base,
        int changeTypeColIndex,
        int commitVersionColIndex,
        int commitTimestampColIndex,
        UTF8String cdcChangeType,
        long cdcCommitVersion,
        long cdcCommitTimestampMicros) {
      this.base = base;
      this.changeTypeColIndex = changeTypeColIndex;
      this.commitVersionColIndex = commitVersionColIndex;
      this.commitTimestampColIndex = commitTimestampColIndex;
      this.cdcChangeType = cdcChangeType;
      this.cdcCommitVersion = cdcCommitVersion;
      this.cdcCommitTimestampMicros = cdcCommitTimestampMicros;
    }

    @Override
    public boolean hasNext() {
      return base.hasNext();
    }

    @Override
    public Object next() {
      Object record = base.next();
      if (record instanceof ColumnarBatch) {
        return overrideCDCColumnarBatch((ColumnarBatch) record);
      }
      if (record instanceof InternalRow) {
        return overrideCDCRow((InternalRow) record);
      }
      return record;
    }

    @Override
    public void close() throws IOException {
      closeConstantVectors();
      if (base instanceof AutoCloseable) {
        try {
          ((AutoCloseable) base).close();
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }

    /** Close any constant vectors we created. Does NOT close the reader's internal vectors. */
    private void closeConstantVectors() {
      if (prevChangeTypeVector != null) {
        prevChangeTypeVector.close();
        prevChangeTypeVector = null;
      }
      if (prevCommitVersionVector != null) {
        prevCommitVersionVector.close();
        prevCommitVersionVector = null;
      }
      if (prevCommitTimestampVector != null) {
        prevCommitTimestampVector.close();
        prevCommitTimestampVector = null;
      }
    }

    private InternalRow overrideCDCRow(InternalRow dataRow) {
      if (cdcOverrideRow == null) {
        cdcOverrideRow =
            new CDCOverrideRow(changeTypeColIndex, commitVersionColIndex, commitTimestampColIndex);
      }
      return cdcOverrideRow.withRow(
          dataRow, cdcChangeType, cdcCommitVersion, cdcCommitTimestampMicros);
    }

    /**
     * Override CDC columns in a columnar batch with per-file constants.
     *
     * <p>Replaces null CDC ColumnVectors with constant-value OnHeapColumnVectors. The original
     * vectors from the batch are NOT closed — they are owned by VectorizedParquetRecordReader which
     * reuses them across batches. We only close the constant vectors we previously created.
     */
    private ColumnarBatch overrideCDCColumnarBatch(ColumnarBatch batch) {
      int numRows = batch.numRows();
      int numCols = batch.numCols();

      ColumnVector[] vectors = new ColumnVector[numCols];
      for (int i = 0; i < numCols; i++) {
        vectors[i] = batch.column(i);
      }

      // Close constant vectors from the previous batch before creating new ones
      closeConstantVectors();

      if (changeTypeColIndex >= 0 && cdcChangeType != null) {
        prevChangeTypeVector = createConstantStringVector(numRows, cdcChangeType);
        vectors[changeTypeColIndex] = prevChangeTypeVector;
      }

      if (commitVersionColIndex >= 0) {
        prevCommitVersionVector = createConstantLongVector(numRows, cdcCommitVersion);
        vectors[commitVersionColIndex] = prevCommitVersionVector;
      }

      if (commitTimestampColIndex >= 0) {
        prevCommitTimestampVector = createConstantLongVector(numRows, cdcCommitTimestampMicros);
        vectors[commitTimestampColIndex] = prevCommitTimestampVector;
      }

      return new ColumnarBatch(vectors, numRows);
    }
  }

  /** Create a constant Long ColumnVector with the given value for all rows. */
  static ColumnVector createConstantLongVector(int numRows, long value) {
    org.apache.spark.sql.execution.vectorized.OnHeapColumnVector vec =
        new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(
            numRows, org.apache.spark.sql.types.DataTypes.LongType);
    for (int i = 0; i < numRows; i++) {
      vec.putLong(i, value);
    }
    return vec;
  }

  /** Create a constant UTF8String ColumnVector with the given value for all rows. */
  static ColumnVector createConstantStringVector(int numRows, UTF8String value) {
    org.apache.spark.sql.execution.vectorized.OnHeapColumnVector vec =
        new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(
            numRows, org.apache.spark.sql.types.DataTypes.StringType);
    byte[] bytes = value.getBytes();
    for (int i = 0; i < numRows; i++) {
      vec.putByteArray(i, bytes);
    }
    return vec;
  }

  /**
   * A zero-copy InternalRow wrapper that overrides all 3 CDC columns with per-file constants.
   *
   * <p>The Parquet reader produces rows with the full readDataSchema including CDC columns (filled
   * with null via schema evolution). This wrapper overrides those null values with the per-file
   * constants (_change_type, _commit_version, _commit_timestamp).
   *
   * <p>Key advantage over JoinedRow: same numFields as the delegate -- no column count mismatch.
   *
   * <p>This is reusable: call {@link #withRow} to set the delegate and values for each row.
   */
  static class CDCOverrideRow extends InternalRow {
    private final int changeTypeOrdinal;
    private final int commitVersionOrdinal;
    private final int commitTimestampOrdinal;
    private InternalRow delegate;
    private UTF8String changeTypeValue;
    private long commitVersionValue;
    private long commitTimestampMicrosValue;

    CDCOverrideRow(int changeTypeOrdinal, int commitVersionOrdinal, int commitTimestampOrdinal) {
      this.changeTypeOrdinal = changeTypeOrdinal;
      this.commitVersionOrdinal = commitVersionOrdinal;
      this.commitTimestampOrdinal = commitTimestampOrdinal;
    }

    CDCOverrideRow withRow(
        InternalRow delegate,
        UTF8String changeTypeValue,
        long commitVersionValue,
        long commitTimestampMicrosValue) {
      this.delegate = delegate;
      this.changeTypeValue = changeTypeValue;
      this.commitVersionValue = commitVersionValue;
      this.commitTimestampMicrosValue = commitTimestampMicrosValue;
      return this;
    }

    @Override
    public int numFields() {
      return delegate.numFields();
    }

    @Override
    public boolean isNullAt(int ordinal) {
      if (ordinal == changeTypeOrdinal) return changeTypeValue == null;
      if (ordinal == commitVersionOrdinal) return false;
      if (ordinal == commitTimestampOrdinal) return false;
      return delegate.isNullAt(ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      if (ordinal == changeTypeOrdinal) return changeTypeValue;
      if (ordinal == commitVersionOrdinal) return commitVersionValue;
      if (ordinal == commitTimestampOrdinal) return commitTimestampMicrosValue;
      return delegate.get(ordinal, dataType);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      if (ordinal == changeTypeOrdinal) return changeTypeValue;
      return delegate.getUTF8String(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      if (ordinal == commitVersionOrdinal) return commitVersionValue;
      if (ordinal == commitTimestampOrdinal) return commitTimestampMicrosValue;
      return delegate.getLong(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return delegate.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      return delegate.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
      return delegate.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
      return delegate.getInt(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return delegate.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return delegate.getDouble(ordinal);
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int ordinal, int precision, int scale) {
      return delegate.getDecimal(ordinal, precision, scale);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return delegate.getBinary(ordinal);
    }

    @Override
    public org.apache.spark.unsafe.types.CalendarInterval getInterval(int ordinal) {
      return delegate.getInterval(ordinal);
    }

    @Override
    public org.apache.spark.unsafe.types.VariantVal getVariant(int ordinal) {
      return delegate.getVariant(ordinal);
    }

    @Override
    public org.apache.spark.unsafe.types.GeographyVal getGeography(int ordinal) {
      return delegate.getGeography(ordinal);
    }

    @Override
    public org.apache.spark.unsafe.types.GeometryVal getGeometry(int ordinal) {
      return delegate.getGeometry(ordinal);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return delegate.getStruct(ordinal, numFields);
    }

    @Override
    public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal) {
      return delegate.getArray(ordinal);
    }

    @Override
    public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal) {
      return delegate.getMap(ordinal);
    }

    @Override
    public void update(int ordinal, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setNullAt(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow copy() {
      CDCOverrideRow copied =
          new CDCOverrideRow(changeTypeOrdinal, commitVersionOrdinal, commitTimestampOrdinal);
      copied.withRow(
          delegate.copy(), changeTypeValue, commitVersionValue, commitTimestampMicrosValue);
      return copied;
    }
  }
}
