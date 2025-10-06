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

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;

/**
 * Java Kernel version of IndexedFile that uses Kernel's AddFile and RemoveFile.
 *
 * <p>This class represents an indexed file in the Delta log for streaming purposes, similar to the
 * Scala IndexedFile but using Kernel action classes instead of Spark actions.
 *
 * <p>Special sentinel values with negative index are used for offset tracking: - BASE_INDEX: Marks
 * the beginning of a version - END_INDEX: Marks the end of a version - METADATA_CHANGE_INDEX: Marks
 * a metadata or protocol change
 */
public class KernelIndexedFile {
  private final long version;
  private final long index;
  private final AddFile add;
  private final RemoveFile remove;
  private final boolean shouldSkip;

  /**
   * Constructs a KernelIndexedFile.
   *
   * @param version The version of the Delta log containing this file
   * @param index The index of this file in the Delta log
   * @param add The Kernel AddFile, or null for sentinel values or RemoveFile
   * @param remove The Kernel RemoveFile, or null for AddFile or sentinel values
   * @param shouldSkip A flag to indicate whether this file should be skipped
   */
  public KernelIndexedFile(
      long version, long index, AddFile addFile, RemoveFile removeFile, boolean shouldSkip) {
    this.version = version;
    this.index = index;
    this.add = addFile;
    this.remove = removeFile;
    this.shouldSkip = shouldSkip;
  }

  public long getVersion() {
    return version;
  }

  public long getIndex() {
    return index;
  }

  public AddFile getAddFile() {
    return add;
  }

  public RemoveFile getRemoveFile() {
    return remove;
  }

  public boolean shouldSkip() {
    return shouldSkip;
  }

  /** Returns true if this IndexedFile has a file action (AddFile or RemoveFile). */
  public boolean hasFileAction() {
    return add != null || remove != null;
  }

  /**
   * Returns the file size if available. For AddFile, returns the size directly. For RemoveFile,
   * returns the size if present.
   *
   * @return file size, or 0 if not available
   */
  public long getFileSize() {
    if (add != null) {
      return add.getSize();
    }
    if (remove != null) {
      return remove.getSize().orElse(0L);
    }
    return 0L;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("KernelIndexedFile{");
    sb.append("version=").append(version);
    sb.append(", index=").append(index);
    sb.append(", add=").append(add);
    sb.append(", remove=").append(remove);
    sb.append(", shouldSkip=").append(shouldSkip);
    sb.append('}');
    return sb.toString();
  }
}
