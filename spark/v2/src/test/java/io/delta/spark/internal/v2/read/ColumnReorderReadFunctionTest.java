/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

public class ColumnReorderReadFunctionTest {

  // Reader emits (data ++ partition): (id, col3) ++ (part).
  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.LongType).add("col3", DataTypes.IntegerType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("part", DataTypes.LongType);

  /** When the target schema matches the reader's natural data ++ partition layout, wrap is noop. */
  @Test
  public void testWrap_identityMapping_returnsBaseReaderUnchanged() {
    StructType natural =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("col3", DataTypes.IntegerType)
            .add("part", DataTypes.LongType);
    Function1<PartitionedFile, Iterator<InternalRow>> base = constantReader(row(1L, 100, 10L));
    Function1<PartitionedFile, Iterator<InternalRow>> wrapped =
        ColumnReorderReadFunction.wrap(
            base, /* isVectorizedReader= */ false, DATA_SCHEMA, PARTITION_SCHEMA, natural);
    assertSame(base, wrapped, "Identity mapping should skip the reorder wrapper");
  }

  /** Row-mode reorder: target advertises DDL order (id, part, col3). */
  @Test
  public void testWrap_rowMode_reordersFieldsByName() {
    StructType ddlOrder =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("part", DataTypes.LongType)
            .add("col3", DataTypes.IntegerType);
    InternalRow input = row(1L, 100, 10L);
    Function1<PartitionedFile, Iterator<InternalRow>> base = constantReader(input);
    Function1<PartitionedFile, Iterator<InternalRow>> wrapped =
        ColumnReorderReadFunction.wrap(
            base, /* isVectorizedReader= */ false, DATA_SCHEMA, PARTITION_SCHEMA, ddlOrder);

    Iterator<InternalRow> it = wrapped.apply(/* file= */ null);
    InternalRow out = it.next();
    assertEquals(1L, out.getLong(0), "id stays at ordinal 0");
    assertEquals(10L, out.getLong(1), "part moved to ordinal 1");
    assertEquals(100, out.getInt(2), "col3 moved to ordinal 2");
  }

  /**
   * Tail fields in target that aren't in data or partition (e.g. CDC) are inferred to live at the
   * reader's tail and identity-map; data/partition still permute into DDL order.
   */
  @Test
  public void testWrap_rowMode_targetTailFieldsAreIdentityMapped() {
    // Reader emits (id, col3, part, _change_type). Target = (id, part, col3, _change_type).
    StructType targetWithCdcTail =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("part", DataTypes.LongType)
            .add("col3", DataTypes.IntegerType)
            .add("_change_type", DataTypes.StringType);
    GenericInternalRow input = new GenericInternalRow(4);
    input.setLong(0, 1L);
    input.setInt(1, 100);
    input.setLong(2, 10L);
    input.update(3, org.apache.spark.unsafe.types.UTF8String.fromString("insert"));

    Function1<PartitionedFile, Iterator<InternalRow>> wrapped =
        ColumnReorderReadFunction.wrap(
            constantReader(input),
            /* isVectorizedReader= */ false,
            DATA_SCHEMA,
            PARTITION_SCHEMA,
            targetWithCdcTail);

    InternalRow out = wrapped.apply(/* file= */ null).next();
    assertEquals(1L, out.getLong(0));
    assertEquals(10L, out.getLong(1));
    assertEquals(100, out.getInt(2));
    assertEquals("insert", out.getUTF8String(3).toString());
  }

  private static InternalRow row(long id, int col3, long part) {
    GenericInternalRow r = new GenericInternalRow(3);
    r.setLong(0, id);
    r.setInt(1, col3);
    r.setLong(2, part);
    return r;
  }

  private static Function1<PartitionedFile, Iterator<InternalRow>> constantReader(InternalRow row) {
    return new AbstractFunction1<PartitionedFile, Iterator<InternalRow>>() {
      @Override
      public Iterator<InternalRow> apply(PartitionedFile file) {
        return JavaConverters.asScalaIterator(java.util.Collections.singletonList(row).iterator());
      }
    };
  }
}
