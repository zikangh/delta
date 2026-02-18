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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.spark.internal.v2.read.CDCFileInfo;
import io.delta.spark.internal.v2.read.CDCReadFunc;
import io.delta.spark.internal.v2.read.CDCReaderFactory;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.read.IndexedFile;
import io.delta.spark.internal.v2.read.SparkMicroBatchStream;
import io.delta.spark.internal.v2.read.SparkReaderFactory;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PartitioningUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;

/** Utility class for partition-related operations shared across Delta Kernel Spark components. */
public class PartitionUtils {

  /**
   * Calculate the maximum split bytes for file partitioning, considering total bytes and file
   * count. This is used for optimal file splitting in both batch and streaming read.
   */
  public static long calculateMaxSplitBytes(
      SparkSession sparkSession, long totalBytes, int fileCount, SQLConf sqlConf) {
    long defaultMaxSplitBytes = sqlConf.filesMaxPartitionBytes();
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    Option<Object> minPartitionNumOption = sqlConf.filesMinPartitionNum();

    int minPartitionNum =
        minPartitionNumOption.isDefined()
            ? ((Number) minPartitionNumOption.get()).intValue()
            : sqlConf
                .getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM())
                .getOrElse(() -> sparkSession.sparkContext().defaultParallelism());
    if (minPartitionNum <= 0) {
      minPartitionNum = 1;
    }

    long calculatedTotalBytes = totalBytes + (long) fileCount * openCostInBytes;
    long bytesPerCore = calculatedTotalBytes / minPartitionNum;

    return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
  }

  /**
   * Build the partition {@link InternalRow} from kernel partition values by casting them to the
   * desired Spark types using the session time zone for temporal types.
   *
   * <p>Note: Partition values in AddFile use physical column names as keys when column mapping is
   * enabled. This method uses DeltaColumnMapping.getPhysicalName to map from logical schema fields
   * to physical partition value keys.
   */
  public static InternalRow getPartitionRow(
      MapValue partitionValues, StructType partitionSchema, ZoneId zoneId) {
    final int numPartCols = partitionSchema.fields().length;
    assert partitionValues.getSize() == numPartCols
        : String.format(
            java.util.Locale.ROOT,
            "Partition values size from add file %d != partition columns size %d",
            partitionValues.getSize(),
            numPartCols);

    final Object[] values = new Object[numPartCols];

    // Build physical name -> index map once
    // Partition values use physical names as keys when column mapping is enabled
    final Map<String, Integer> physicalNameToIndex = new HashMap<>(numPartCols);
    for (int i = 0; i < numPartCols; i++) {
      StructField field = partitionSchema.fields()[i];
      String physicalName = DeltaColumnMapping.getPhysicalName(field);
      physicalNameToIndex.put(physicalName, i);
      values[i] = null;
    }

    // Fill values in a single pass over partitionValues
    for (int idx = 0; idx < partitionValues.getSize(); idx++) {
      final String key = partitionValues.getKeys().getString(idx);
      final String strVal = partitionValues.getValues().getString(idx);
      final Integer pos = physicalNameToIndex.get(key);
      if (pos != null) {
        final StructField field = partitionSchema.fields()[pos];
        values[pos] =
            (strVal == null)
                ? null
                : PartitioningUtils.castPartValueToDesiredType(field.dataType(), strVal, zoneId);
      }
    }
    return InternalRow.fromSeq(
        CollectionConverters.asScala(Arrays.asList(values).iterator()).toSeq());
  }

  /**
   * Build a PartitionedFile from an AddFile with the given partition schema and table path.
   *
   * @param addFile The AddFile to convert
   * @param partitionSchema The partition schema for parsing partition values
   * @param tablePath The table path
   * @param zoneId The timezone for temporal partition values
   * @return A PartitionedFile ready for Spark execution
   */
  public static PartitionedFile buildPartitionedFile(
      AddFile addFile, StructType partitionSchema, String tablePath, ZoneId zoneId) {
    InternalRow partitionRow =
        getPartitionRow(addFile.getPartitionValues(), partitionSchema, zoneId);

    // Preferred node locations are not used.
    String[] preferredLocations = new String[0];
    // Constant metadata columns are not used.
    scala.collection.immutable.Map<String, Object> otherConstantMetadataColumnValues =
        scala.collection.immutable.Map$.MODULE$.empty();

    return new PartitionedFile(
        partitionRow,
        SparkPath.fromUrlString(new Path(tablePath, addFile.getPath()).toString()),
        /* start= */ 0L,
        /* length= */ addFile.getSize(),
        preferredLocations,
        addFile.getModificationTime(),
        /* fileSize= */ addFile.getSize(),
        otherConstantMetadataColumnValues);
  }

  /**
   * Build a PartitionedFile for CDC with metadata columns injected as constants.
   *
   * <p>For explicit CDC files: read from _change_data/ directory. For inferred CDC: read from data
   * files with injected _change_type.
   *
   * @param indexedFile The IndexedFile containing CDC information
   * @param partitionSchema The partition schema
   * @param tablePath The table path
   * @param zoneId The timezone for temporal partition values
   * @return A PartitionedFile ready for CDC read
   */
  public static PartitionedFile buildCDCPartitionedFile(
      IndexedFile indexedFile, StructType partitionSchema, String tablePath, ZoneId zoneId) {

    // Build constant CDC metadata columns
    Map<String, Object> constantMetadataColumns = new HashMap<>();
    constantMetadataColumns.put(SparkMicroBatchStream.CDC_COMMIT_VERSION, indexedFile.getVersion());
    constantMetadataColumns.put(
        SparkMicroBatchStream.CDC_COMMIT_TIMESTAMP,
        new Timestamp(indexedFile.getCommitTimestamp()));

    if (indexedFile.isCDCFile()) {
      // Explicit CDC file - _change_type is in the data, no need to inject
      CDCFileInfo cdcFile = indexedFile.getCdcFile();
      return buildPartitionedFileFromCDCFile(
          cdcFile, partitionSchema, tablePath, zoneId, constantMetadataColumns);
    } else if (indexedFile.getAddFile() != null) {
      // Inferred CDC from AddFile - inject _change_type as constant
      constantMetadataColumns.put(
          SparkMicroBatchStream.CDC_TYPE_COLUMN, indexedFile.getChangeType());
      return buildPartitionedFileWithConstants(
          indexedFile.getAddFile(), partitionSchema, tablePath, zoneId, constantMetadataColumns);
    } else if (indexedFile.getRemoveFile() != null) {
      // Inferred CDC from RemoveFile - inject _change_type as constant
      constantMetadataColumns.put(
          SparkMicroBatchStream.CDC_TYPE_COLUMN, indexedFile.getChangeType());
      return buildPartitionedFileFromRemove(
          indexedFile.getRemoveFile(), partitionSchema, tablePath, zoneId, constantMetadataColumns);
    } else {
      throw new IllegalStateException(
          "IndexedFile for CDC must have cdcFile, addFile, or removeFile");
    }
  }

  /** Build PartitionedFile from an explicit CDC file (AddCDCFile). */
  private static PartitionedFile buildPartitionedFileFromCDCFile(
      CDCFileInfo cdcFile,
      StructType partitionSchema,
      String tablePath,
      ZoneId zoneId,
      Map<String, Object> constantMetadataColumns) {

    InternalRow partitionRow =
        getPartitionRow(cdcFile.getPartitionValues(), partitionSchema, zoneId);

    String[] preferredLocations = new String[0];

    // Convert constant metadata columns to Scala immutable map
    scala.collection.immutable.Map<String, Object> scalaConstants =
        convertToScalaImmutableMap(constantMetadataColumns);

    return new PartitionedFile(
        partitionRow,
        SparkPath.fromUrlString(new Path(tablePath, cdcFile.getPath()).toString()),
        /* start= */ 0L,
        /* length= */ cdcFile.getSize(),
        preferredLocations,
        /* modificationTime= */ 0L, // CDC files don't have modification time
        /* fileSize= */ cdcFile.getSize(),
        scalaConstants);
  }

  /** Build PartitionedFile from AddFile with constant metadata columns. */
  private static PartitionedFile buildPartitionedFileWithConstants(
      AddFile addFile,
      StructType partitionSchema,
      String tablePath,
      ZoneId zoneId,
      Map<String, Object> constantMetadataColumns) {

    InternalRow partitionRow =
        getPartitionRow(addFile.getPartitionValues(), partitionSchema, zoneId);

    String[] preferredLocations = new String[0];

    scala.collection.immutable.Map<String, Object> scalaConstants =
        convertToScalaImmutableMap(constantMetadataColumns);

    return new PartitionedFile(
        partitionRow,
        SparkPath.fromUrlString(new Path(tablePath, addFile.getPath()).toString()),
        /* start= */ 0L,
        /* length= */ addFile.getSize(),
        preferredLocations,
        addFile.getModificationTime(),
        /* fileSize= */ addFile.getSize(),
        scalaConstants);
  }

  /** Build PartitionedFile from RemoveFile with constant metadata columns. */
  private static PartitionedFile buildPartitionedFileFromRemove(
      RemoveFile removeFile,
      StructType partitionSchema,
      String tablePath,
      ZoneId zoneId,
      Map<String, Object> constantMetadataColumns) {

    // RemoveFile partition values are optional
    InternalRow partitionRow =
        removeFile
            .getPartitionValues()
            .map(pv -> getPartitionRow(pv, partitionSchema, zoneId))
            .orElseGet(
                () ->
                    InternalRow.fromSeq(
                        CollectionConverters.asScala(
                                Arrays.asList(new Object[partitionSchema.fields().length])
                                    .iterator())
                            .toSeq()));

    String[] preferredLocations = new String[0];

    scala.collection.immutable.Map<String, Object> scalaConstants =
        convertToScalaImmutableMap(constantMetadataColumns);

    long fileSize = removeFile.getSize().orElse(0L);

    return new PartitionedFile(
        partitionRow,
        SparkPath.fromUrlString(new Path(tablePath, removeFile.getPath()).toString()),
        /* start= */ 0L,
        /* length= */ fileSize,
        preferredLocations,
        removeFile.getDeletionTimestamp().orElse(0L),
        /* fileSize= */ fileSize,
        scalaConstants);
  }

  /** Convert Java Map to Scala immutable Map. */
  @SuppressWarnings("unchecked")
  private static scala.collection.immutable.Map<String, Object> convertToScalaImmutableMap(
      Map<String, Object> javaMap) {
    scala.collection.immutable.Map<String, Object> result =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, Object> entry : javaMap.entrySet()) {
      result = result.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  /**
   * Create a {@link CDCReaderFactory} with type-specific ReadFuncs for CDC reads.
   *
   * <p>Builds TWO base Parquet ReadFuncs with different schemas:
   *
   * <ul>
   *   <li>Explicit CDC (AddCDCFile): reads {@code dataOnlySchema + _change_type} because {@code
   *       _change_type} IS physically present in AddCDCFile Parquet files
   *   <li>Inferred CDC (AddFile/RemoveFile): reads {@code dataOnlySchema} only — no CDC columns
   * </ul>
   *
   * <p>Each CDC ReadFunc wraps its base ReadFunc and appends CDC columns at the end of each
   * row/batch. This eliminates the dependency on Parquet schema evolution to populate null values
   * for missing CDC columns.
   *
   * @param snapshot The Delta table snapshot containing protocol, metadata, and table path
   * @param readDataSchema Must be the data-only schema (no CDC columns — caller must strip them)
   */
  public static PartitionReaderFactory createCDCReaderFactory(
      Snapshot snapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SQLConf sqlConf) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    Protocol protocol = snapshotImpl.getProtocol();
    Metadata metadata = snapshotImpl.getMetadata();
    String tablePath = snapshotImpl.getDataPath().toUri().toString();

    // readDataSchema is dataOnlySchema (no CDC columns)
    StructType dataOnlySchema = readDataSchema;

    // Explicit CDC schema: data + _change_type (physically present in AddCDCFile Parquet)
    StructType explicitSchema =
        dataOnlySchema.add(
            SparkMicroBatchStream.CDC_TYPE_COLUMN, org.apache.spark.sql.types.DataTypes.StringType);
    StructType dataSchemaWithCT =
        dataSchema.add(
            SparkMicroBatchStream.CDC_TYPE_COLUMN, org.apache.spark.sql.types.DataTypes.StringType);

    // Use explicit schema (the wider one) for vectorization check — if [data_cols, _change_type]
    // supports vectorization, so does [data_cols] (a subset). Both base ReadFuncs must agree.
    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, explicitSchema);
    scala.collection.immutable.Map<String, String> optionsWithVectorizedReading =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));

    DeltaParquetFileFormatV2 deltaFormat =
        new DeltaParquetFileFormatV2(
            protocol,
            metadata,
            /* nullableRowTrackingConstantFields */ false,
            /* nullableRowTrackingGeneratedFields */ false,
            /* optimizationsEnabled */ true,
            Option.apply(tablePath),
            /* isCDCRead */ true,
            /* useMetadataRowIndexOpt */ Option.empty());

    // IMPORTANT: Each buildReaderWithPartitionValues call mutates the passed hadoopConf
    // (via ParquetFileFormat.setupHadoopConf setting SPARK_ROW_REQUESTED_SCHEMA). In local
    // mode, Spark's broadcast holds a reference to the same Configuration object, so the
    // second call would overwrite the first call's schema. Use separate copies to avoid this.

    // Explicit CDC base: reads data + _change_type from Parquet.
    // Pass dataSchemaWithCT as the dataSchema param to avoid Parquet schema validation issues
    // when requiredSchema (explicitSchema) has _change_type but dataSchema doesn't.
    Function1<PartitionedFile, Iterator<InternalRow>> explicitBaseReadFunc =
        deltaFormat.buildReaderWithPartitionValues(
            SparkSession.active(),
            dataSchemaWithCT,
            partitionSchema,
            explicitSchema,
            CollectionConverters.asScala(Arrays.asList(dataFilters)).toSeq(),
            optionsWithVectorizedReading,
            new Configuration(hadoopConf));

    // Inferred CDC base: reads data only (no CDC columns at all)
    Function1<PartitionedFile, Iterator<InternalRow>> inferredBaseReadFunc =
        deltaFormat.buildReaderWithPartitionValues(
            SparkSession.active(),
            dataSchema,
            partitionSchema,
            dataOnlySchema,
            CollectionConverters.asScala(Arrays.asList(dataFilters)).toSeq(),
            optionsWithVectorizedReading,
            new Configuration(hadoopConf));

    // Compute field indices and counts for CDC ReadFuncs
    int changeTypeIndexInExplicit =
        explicitSchema.fieldIndex(SparkMicroBatchStream.CDC_TYPE_COLUMN);
    int numExplicitBaseFields = explicitSchema.fields().length + partitionSchema.fields().length;
    int numInferredBaseFields = dataOnlySchema.fields().length + partitionSchema.fields().length;

    // Create type-specific ReadFuncs
    CDCReadFunc.ExplicitCDCReadFunc explicitReadFunc =
        new CDCReadFunc.ExplicitCDCReadFunc(
            explicitBaseReadFunc, changeTypeIndexInExplicit, numExplicitBaseFields);

    CDCReadFunc.InferredCDCReadFunc inferredInsertReadFunc =
        new CDCReadFunc.InferredCDCReadFunc(
            inferredBaseReadFunc, numInferredBaseFields, SparkMicroBatchStream.CDC_TYPE_INSERT);

    CDCReadFunc.InferredCDCReadFunc inferredDeleteReadFunc =
        new CDCReadFunc.InferredCDCReadFunc(
            inferredBaseReadFunc, numInferredBaseFields, SparkMicroBatchStream.CDC_TYPE_DELETE);

    return new CDCReaderFactory(
        explicitReadFunc, inferredInsertReadFunc, inferredDeleteReadFunc, enableVectorizedReader);
  }

  /**
   * Create a PartitionReaderFactory for reading Parquet files with Delta-specific features.
   *
   * <p>Uses DeltaParquetFileFormatV2 which supports column mapping, deletion vectors, and other
   * Delta features through the ProtocolMetadataAdapterV2.
   *
   * @param snapshot The Delta table snapshot containing protocol, metadata, and table path
   */
  public static PartitionReaderFactory createDeltaParquetReaderFactory(
      Snapshot snapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SQLConf sqlConf) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    Protocol protocol = snapshotImpl.getProtocol();
    Metadata metadata = snapshotImpl.getMetadata();
    String tablePath = snapshotImpl.getDataPath().toUri().toString();

    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);
    scala.collection.immutable.Map<String, String> optionsWithVectorizedReading =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));

    // Use DeltaParquetFileFormatV2 to support column mapping and other Delta features
    DeltaParquetFileFormatV2 deltaFormat =
        new DeltaParquetFileFormatV2(
            protocol,
            metadata,
            /* nullableRowTrackingConstantFields */ false,
            /* nullableRowTrackingGeneratedFields */ false,
            /* optimizationsEnabled */ true,
            Option.apply(tablePath),
            /* isCDCRead */ false,
            /* useMetadataRowIndexOpt */ Option.empty());

    Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
        deltaFormat.buildReaderWithPartitionValues(
            SparkSession.active(),
            dataSchema,
            partitionSchema,
            readDataSchema,
            CollectionConverters.asScala(Arrays.asList(dataFilters)).toSeq(),
            optionsWithVectorizedReading,
            hadoopConf);

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }
}
