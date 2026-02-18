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
 * CDC ReadFunc implementations that append CDC metadata columns to base Parquet reader output.
 *
 * <p>Contains two ReadFunc types:
 *
 * <ul>
 *   <li>{@link ExplicitCDCReadFunc} for explicit CDC files (AddCDCFile) where {@code _change_type}
 *       is physically present in the Parquet data. Reorders {@code _change_type} to the end and
 *       appends {@code _commit_version} and {@code _commit_timestamp}.
 *   <li>{@link InferredCDCReadFunc} for inferred CDC (AddFile/RemoveFile with dataChange=true).
 *       Appends all 3 CDC columns as constants.
 * </ul>
 *
 * <p>Each ReadFunc wraps a base Parquet ReadFunc built with a type-appropriate schema and appends
 * CDC columns at the end of each row/batch. This eliminates the dependency on Parquet schema
 * evolution to populate null CDC column values.
 */
public final class CDCReadFunc {

  private CDCReadFunc() {} // utility/container class

  /////////////////////////
  // ReadFunc decorators //
  /////////////////////////

  /**
   * ReadFunc for explicit CDC files (AddCDCFile).
   *
   * <p>The base ReadFunc reads {@code [data_cols, _change_type, partition_cols]} because {@code
   * _change_type} IS physically present in AddCDCFile Parquet files. This ReadFunc reorders {@code
   * _change_type} to the end of the output and appends {@code _commit_version} and {@code
   * _commit_timestamp}.
   *
   * <p>Output: {@code [data_cols, partition_cols, _change_type, _commit_version,
   * _commit_timestamp]}
   */
  public static class ExplicitCDCReadFunc
      extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>>
      implements java.io.Serializable {
    private final Function1<PartitionedFile, Iterator<InternalRow>> delegate;
    private final int changeTypeIndexInBase;
    private final int numBaseFields;

    public ExplicitCDCReadFunc(
        Function1<PartitionedFile, Iterator<InternalRow>> delegate,
        int changeTypeIndexInBase,
        int numBaseFields) {
      this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
      this.changeTypeIndexInBase = changeTypeIndexInBase;
      this.numBaseFields = numBaseFields;
    }

    @Override
    public Iterator<InternalRow> apply(PartitionedFile file) {
      Iterator<InternalRow> baseIterator = delegate.apply(file);
      try {
        scala.collection.immutable.Map<String, Object> constants =
            file.otherConstantMetadataColumnValues();

        Object commitVersion =
            constants.getOrElse(SparkMicroBatchStream.CDC_COMMIT_VERSION, () -> null);
        long cdcCommitVersion = commitVersion != null ? ((Number) commitVersion).longValue() : 0;

        Object commitTimestamp =
            constants.getOrElse(SparkMicroBatchStream.CDC_COMMIT_TIMESTAMP, () -> null);
        long cdcCommitTimestampMicros =
            commitTimestamp instanceof Timestamp
                ? DateTimeUtils.fromJavaTimestamp((Timestamp) commitTimestamp)
                : 0;

        @SuppressWarnings("unchecked")
        Iterator<InternalRow> result =
            (Iterator<InternalRow>)
                (Iterator<?>)
                    new CDCReorderAppendIterator(
                        baseIterator,
                        changeTypeIndexInBase,
                        numBaseFields,
                        cdcCommitVersion,
                        cdcCommitTimestampMicros);
        return result;
      } catch (Exception e) {
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
  }

  /**
   * ReadFunc for inferred CDC files (AddFile/RemoveFile with dataChange=true).
   *
   * <p>The base ReadFunc reads {@code [data_cols, partition_cols]} with no CDC columns at all. This
   * ReadFunc appends all 3 CDC columns as constants.
   *
   * <p>Output: {@code [data_cols, partition_cols, _change_type, _commit_version,
   * _commit_timestamp]}
   */
  public static class InferredCDCReadFunc
      extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>>
      implements java.io.Serializable {
    private final Function1<PartitionedFile, Iterator<InternalRow>> delegate;
    private final int numBaseFields;
    private final UTF8String fixedChangeType;

    public InferredCDCReadFunc(
        Function1<PartitionedFile, Iterator<InternalRow>> delegate,
        int numBaseFields,
        String fixedChangeType) {
      this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
      this.numBaseFields = numBaseFields;
      this.fixedChangeType =
          UTF8String.fromString(
              java.util.Objects.requireNonNull(fixedChangeType, "fixedChangeType"));
    }

    @Override
    public Iterator<InternalRow> apply(PartitionedFile file) {
      Iterator<InternalRow> baseIterator = delegate.apply(file);
      try {
        scala.collection.immutable.Map<String, Object> constants =
            file.otherConstantMetadataColumnValues();

        Object commitVersion =
            constants.getOrElse(SparkMicroBatchStream.CDC_COMMIT_VERSION, () -> null);
        long cdcCommitVersion = commitVersion != null ? ((Number) commitVersion).longValue() : 0;

        Object commitTimestamp =
            constants.getOrElse(SparkMicroBatchStream.CDC_COMMIT_TIMESTAMP, () -> null);
        long cdcCommitTimestampMicros =
            commitTimestamp instanceof Timestamp
                ? DateTimeUtils.fromJavaTimestamp((Timestamp) commitTimestamp)
                : 0;

        @SuppressWarnings("unchecked")
        Iterator<InternalRow> result =
            (Iterator<InternalRow>)
                (Iterator<?>)
                    new CDCAppendIterator(
                        baseIterator,
                        numBaseFields,
                        fixedChangeType,
                        cdcCommitVersion,
                        cdcCommitTimestampMicros);
        return result;
      } catch (Exception e) {
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
  }

  ///////////////
  // Iterators //
  ///////////////

  /**
   * Iterator for explicit CDC that reorders {@code _change_type} to the end and appends 2 constant
   * columns ({@code _commit_version}, {@code _commit_timestamp}).
   *
   * <p>Base output: {@code [data_cols, _change_type, partition_cols]} (N fields)
   *
   * <p>This output: {@code [data_cols, partition_cols, _change_type, _commit_version,
   * _commit_timestamp]} (N+2 fields)
   */
  static class CDCReorderAppendIterator extends scala.collection.AbstractIterator<Object>
      implements Closeable {
    private final Iterator<?> base;
    private final int changeTypeIndex;
    private final int numBaseFields;
    private final long cdcCommitVersion;
    private final long cdcCommitTimestampMicros;

    private CDCReorderAppendRow reorderAppendRow;
    private ColumnVector prevCommitVersionVector;
    private ColumnVector prevCommitTimestampVector;

    CDCReorderAppendIterator(
        Iterator<?> base,
        int changeTypeIndex,
        int numBaseFields,
        long cdcCommitVersion,
        long cdcCommitTimestampMicros) {
      this.base = base;
      this.changeTypeIndex = changeTypeIndex;
      this.numBaseFields = numBaseFields;
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
        return reorderAppendColumnarBatch((ColumnarBatch) record);
      }
      if (record instanceof InternalRow) {
        return reorderAppendRow((InternalRow) record);
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

    private void closeConstantVectors() {
      if (prevCommitVersionVector != null) {
        prevCommitVersionVector.close();
        prevCommitVersionVector = null;
      }
      if (prevCommitTimestampVector != null) {
        prevCommitTimestampVector.close();
        prevCommitTimestampVector = null;
      }
    }

    private InternalRow reorderAppendRow(InternalRow dataRow) {
      if (reorderAppendRow == null) {
        reorderAppendRow = new CDCReorderAppendRow(changeTypeIndex, numBaseFields);
      }
      return reorderAppendRow.withRow(dataRow, cdcCommitVersion, cdcCommitTimestampMicros);
    }

    /**
     * Reorder + append for columnar batch.
     *
     * <p>Base: {@code [vecs 0..C-1, vec_C(_change_type), vecs C+1..N-1]}
     *
     * <p>Output: {@code [vecs 0..C-1, vecs C+1..N-1, vec_C, const_cv_vec, const_cts_vec]}
     */
    private ColumnarBatch reorderAppendColumnarBatch(ColumnarBatch batch) {
      int numRows = batch.numRows();
      int numOutputCols = numBaseFields + 2;
      ColumnVector[] vectors = new ColumnVector[numOutputCols];

      // Data cols before _change_type
      int outIdx = 0;
      for (int i = 0; i < changeTypeIndex; i++) {
        vectors[outIdx++] = batch.column(i);
      }
      // Partition cols (after _change_type in base)
      for (int i = changeTypeIndex + 1; i < numBaseFields; i++) {
        vectors[outIdx++] = batch.column(i);
      }
      // _change_type from Parquet (just referenced at new position, not copied)
      vectors[outIdx++] = batch.column(changeTypeIndex);

      // Close previous constant vectors before creating new ones
      closeConstantVectors();

      prevCommitVersionVector = createConstantLongVector(numRows, cdcCommitVersion);
      vectors[outIdx++] = prevCommitVersionVector;
      prevCommitTimestampVector = createConstantLongVector(numRows, cdcCommitTimestampMicros);
      vectors[outIdx] = prevCommitTimestampVector;

      return new ColumnarBatch(vectors, numRows);
    }
  }

  /**
   * Iterator for inferred CDC that appends 3 constant columns ({@code _change_type}, {@code
   * _commit_version}, {@code _commit_timestamp}).
   *
   * <p>Base output: {@code [data_cols, partition_cols]} (N fields)
   *
   * <p>This output: {@code [data_cols, partition_cols, _change_type, _commit_version,
   * _commit_timestamp]} (N+3 fields)
   */
  static class CDCAppendIterator extends scala.collection.AbstractIterator<Object>
      implements Closeable {
    private final Iterator<?> base;
    private final int numBaseFields;
    private final UTF8String cdcChangeType;
    private final long cdcCommitVersion;
    private final long cdcCommitTimestampMicros;

    private CDCAppendRow appendRow;
    private ColumnVector prevChangeTypeVector;
    private ColumnVector prevCommitVersionVector;
    private ColumnVector prevCommitTimestampVector;

    CDCAppendIterator(
        Iterator<?> base,
        int numBaseFields,
        UTF8String cdcChangeType,
        long cdcCommitVersion,
        long cdcCommitTimestampMicros) {
      this.base = base;
      this.numBaseFields = numBaseFields;
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
        return appendColumnarBatch((ColumnarBatch) record);
      }
      if (record instanceof InternalRow) {
        return appendRow((InternalRow) record);
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

    private InternalRow appendRow(InternalRow dataRow) {
      if (appendRow == null) {
        appendRow = new CDCAppendRow(numBaseFields);
      }
      return appendRow.withRow(dataRow, cdcChangeType, cdcCommitVersion, cdcCommitTimestampMicros);
    }

    /** Append 3 constant CDC columns to a columnar batch. */
    private ColumnarBatch appendColumnarBatch(ColumnarBatch batch) {
      int numRows = batch.numRows();
      int numOutputCols = numBaseFields + 3;
      ColumnVector[] vectors = new ColumnVector[numOutputCols];

      // Copy all base vectors
      for (int i = 0; i < numBaseFields; i++) {
        vectors[i] = batch.column(i);
      }

      // Close previous constant vectors before creating new ones
      closeConstantVectors();

      prevChangeTypeVector = createConstantStringVector(numRows, cdcChangeType);
      vectors[numBaseFields] = prevChangeTypeVector;
      prevCommitVersionVector = createConstantLongVector(numRows, cdcCommitVersion);
      vectors[numBaseFields + 1] = prevCommitVersionVector;
      prevCommitTimestampVector = createConstantLongVector(numRows, cdcCommitTimestampMicros);
      vectors[numBaseFields + 2] = prevCommitTimestampVector;

      return new ColumnarBatch(vectors, numRows);
    }
  }

  //////////////////
  // Row wrappers //
  //////////////////

  /**
   * Zero-copy InternalRow wrapper for inferred CDC: appends 3 constant CDC columns.
   *
   * <p>Ordinal mapping (N = numBaseFields):
   *
   * <pre>
   * ordinal &lt; N    -> delegate.get(ordinal)       // data + partition cols
   * ordinal == N   -> _change_type constant
   * ordinal == N+1 -> _commit_version constant
   * ordinal == N+2 -> _commit_timestamp constant
   * </pre>
   */
  static class CDCAppendRow extends InternalRow {
    private final int numBaseFields;
    private InternalRow delegate;
    private UTF8String changeTypeValue;
    private long commitVersionValue;
    private long commitTimestampMicrosValue;

    CDCAppendRow(int numBaseFields) {
      this.numBaseFields = numBaseFields;
    }

    CDCAppendRow withRow(
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
      return numBaseFields + 3;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      if (ordinal < numBaseFields) return delegate.isNullAt(ordinal);
      if (ordinal == numBaseFields) return changeTypeValue == null;
      return false; // _commit_version and _commit_timestamp are never null
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      if (ordinal < numBaseFields) return delegate.get(ordinal, dataType);
      if (ordinal == numBaseFields) return changeTypeValue;
      if (ordinal == numBaseFields + 1) return commitVersionValue;
      if (ordinal == numBaseFields + 2) return commitTimestampMicrosValue;
      throw new ArrayIndexOutOfBoundsException(ordinal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      if (ordinal == numBaseFields) return changeTypeValue;
      return delegate.getUTF8String(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      if (ordinal == numBaseFields + 1) return commitVersionValue;
      if (ordinal == numBaseFields + 2) return commitTimestampMicrosValue;
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
      CDCAppendRow copied = new CDCAppendRow(numBaseFields);
      copied.withRow(
          delegate.copy(), changeTypeValue, commitVersionValue, commitTimestampMicrosValue);
      return copied;
    }
  }

  /**
   * Zero-copy InternalRow wrapper for explicit CDC: reorders {@code _change_type} to the end and
   * appends 2 constant columns.
   *
   * <p>The delegate has N fields with {@code _change_type} at index C. The output has N+2 fields:
   *
   * <pre>
   * ordinal &lt; C       -> delegate.get(ordinal)      // data cols before _change_type
   * C &lt;= ordinal &lt; N-1 -> delegate.get(ordinal + 1)  // partition cols, shifted past _change_type
   * ordinal == N-1     -> delegate.get(C)            // _change_type from Parquet
   * ordinal == N       -> _commit_version constant
   * ordinal == N+1     -> _commit_timestamp constant
   * </pre>
   */
  static class CDCReorderAppendRow extends InternalRow {
    private final int changeTypeIndex; // C
    private final int numBaseFields; // N
    private InternalRow delegate;
    private long commitVersionValue;
    private long commitTimestampMicrosValue;

    CDCReorderAppendRow(int changeTypeIndex, int numBaseFields) {
      this.changeTypeIndex = changeTypeIndex;
      this.numBaseFields = numBaseFields;
    }

    CDCReorderAppendRow withRow(
        InternalRow delegate, long commitVersionValue, long commitTimestampMicrosValue) {
      this.delegate = delegate;
      this.commitVersionValue = commitVersionValue;
      this.commitTimestampMicrosValue = commitTimestampMicrosValue;
      return this;
    }

    @Override
    public int numFields() {
      return numBaseFields + 2;
    }

    /**
     * Maps an output ordinal to the corresponding delegate ordinal. Returns -1 for appended CDC
     * constant columns ({@code _commit_version}, {@code _commit_timestamp}) which are not in the
     * delegate.
     */
    private int mapOrdinal(int ordinal) {
      if (ordinal < changeTypeIndex) {
        // Data cols: pass through directly
        return ordinal;
      } else if (ordinal < numBaseFields - 1) {
        // Partition cols: shift +1 to skip _change_type in delegate
        return ordinal + 1;
      } else if (ordinal == numBaseFields - 1) {
        // _change_type: map to its original position in delegate
        return changeTypeIndex;
      } else {
        // CDC constants (_commit_version, _commit_timestamp): not in delegate
        return -1;
      }
    }

    @Override
    public boolean isNullAt(int ordinal) {
      int mapped = mapOrdinal(ordinal);
      if (mapped >= 0) return delegate.isNullAt(mapped);
      return false; // _commit_version and _commit_timestamp are never null
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      int mapped = mapOrdinal(ordinal);
      if (mapped >= 0) return delegate.get(mapped, dataType);
      if (ordinal == numBaseFields) return commitVersionValue;
      if (ordinal == numBaseFields + 1) return commitTimestampMicrosValue;
      throw new ArrayIndexOutOfBoundsException(ordinal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      int mapped = mapOrdinal(ordinal);
      if (mapped >= 0) return delegate.getUTF8String(mapped);
      throw new ArrayIndexOutOfBoundsException(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      if (ordinal == numBaseFields) return commitVersionValue;
      if (ordinal == numBaseFields + 1) return commitTimestampMicrosValue;
      int mapped = mapOrdinal(ordinal);
      return delegate.getLong(mapped);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return delegate.getBoolean(mapOrdinal(ordinal));
    }

    @Override
    public byte getByte(int ordinal) {
      return delegate.getByte(mapOrdinal(ordinal));
    }

    @Override
    public short getShort(int ordinal) {
      return delegate.getShort(mapOrdinal(ordinal));
    }

    @Override
    public int getInt(int ordinal) {
      return delegate.getInt(mapOrdinal(ordinal));
    }

    @Override
    public float getFloat(int ordinal) {
      return delegate.getFloat(mapOrdinal(ordinal));
    }

    @Override
    public double getDouble(int ordinal) {
      return delegate.getDouble(mapOrdinal(ordinal));
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int ordinal, int precision, int scale) {
      return delegate.getDecimal(mapOrdinal(ordinal), precision, scale);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return delegate.getBinary(mapOrdinal(ordinal));
    }

    @Override
    public org.apache.spark.unsafe.types.CalendarInterval getInterval(int ordinal) {
      return delegate.getInterval(mapOrdinal(ordinal));
    }

    @Override
    public org.apache.spark.unsafe.types.VariantVal getVariant(int ordinal) {
      return delegate.getVariant(mapOrdinal(ordinal));
    }

    @Override
    public org.apache.spark.unsafe.types.GeographyVal getGeography(int ordinal) {
      return delegate.getGeography(mapOrdinal(ordinal));
    }

    @Override
    public org.apache.spark.unsafe.types.GeometryVal getGeometry(int ordinal) {
      return delegate.getGeometry(mapOrdinal(ordinal));
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return delegate.getStruct(mapOrdinal(ordinal), numFields);
    }

    @Override
    public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal) {
      return delegate.getArray(mapOrdinal(ordinal));
    }

    @Override
    public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal) {
      return delegate.getMap(mapOrdinal(ordinal));
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
      CDCReorderAppendRow copied = new CDCReorderAppendRow(changeTypeIndex, numBaseFields);
      copied.withRow(delegate.copy(), commitVersionValue, commitTimestampMicrosValue);
      return copied;
    }
  }

  //////////////////////
  // Utility methods  //
  //////////////////////

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
}
