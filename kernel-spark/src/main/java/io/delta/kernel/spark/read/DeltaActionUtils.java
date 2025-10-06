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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.data.StructRow;
import java.util.Optional;

/** Utility methods for extracting Delta actions from ColumnarBatch. */
public class DeltaActionUtils {

  private DeltaActionUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Get AddFile action from a batch at the specified row, if present and has dataChange=true.
   */
  public static Optional<AddFile> getDataChangeAdd(ColumnarBatch batch, int rowId) {
    int addIdx = batch.getSchema().indexOf("add");
    if (addIdx < 0) {
      return Optional.empty();
    }

    ColumnVector addVector = batch.getColumnVector(addIdx);
    if (addVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row addFileRow = StructRow.fromStructVector(addVector, rowId);
    if (addFileRow == null) {
      return Optional.empty();
    }

    AddFile addFile = new AddFile(addFileRow);
    return addFile.getDataChange() ? Optional.of(addFile) : Optional.empty();
  }

  /**
   * Get RemoveFile action from a batch at the specified row, if present and has dataChange=true.
   */
  public static Optional<RemoveFile> getDataChangeRemove(ColumnarBatch batch, int rowId) {
    int removeIdx = batch.getSchema().indexOf("remove");
    if (removeIdx < 0) {
      return Optional.empty();
    }

    ColumnVector removeVector = batch.getColumnVector(removeIdx);
    if (removeVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row removeFileRow = StructRow.fromStructVector(removeVector, rowId);
    if (removeFileRow == null) {
      return Optional.empty();
    }

    RemoveFile removeFile = new RemoveFile(removeFileRow);
    return removeFile.getDataChange() ? Optional.of(removeFile) : Optional.empty();
  }

  /**
   * Get Protocol action from a batch at the specified row, if present.
   */
  public static Optional<Protocol> getProtocol(ColumnarBatch batch, int rowId) {
    int protocolIdx = batch.getSchema().indexOf("protocol");
    if (protocolIdx < 0) {
      return Optional.empty();
    }

    ColumnVector protocolVector = batch.getColumnVector(protocolIdx);
    if (protocolVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    return Optional.ofNullable(Protocol.fromColumnVector(protocolVector, rowId));
  }

  /**
   * Get Metadata action from a batch at the specified row, if present.
   */
  public static Optional<Metadata> getMetadata(ColumnarBatch batch, int rowId) {
    int metadataIdx = batch.getSchema().indexOf("metaData");
    if (metadataIdx < 0) {
      return Optional.empty();
    }

    ColumnVector metadataVector = batch.getColumnVector(metadataIdx);
    if (metadataVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    return Optional.ofNullable(Metadata.fromColumnVector(metadataVector, rowId));
  }
}
