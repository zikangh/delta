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

package io.delta.internal

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.utils.ScalaUtils
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.DeltaV2Mode
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.Relocated.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Rule for applying the V2 streaming path by rewriting V1 StreamingRelation
 * with Delta DataSource to StreamingRelationV2 with SparkTable.
 *
 * This rule handles the case where Spark's FindDataSourceTable rule has converted
 * a StreamingRelationV2 (with DeltaTableV2) back to a StreamingRelation because
 * DeltaTableV2 doesn't advertise STREAMING_READ capability. We convert it back to
 * StreamingRelationV2 with SparkTable (from sparkV2) which does support streaming.
 *
 * Additionally, for CDC (Change Data Feed) reads, this rule augments the output schema
 * of StreamingRelationV2 with CDC metadata columns (_change_type, _commit_version,
 * _commit_timestamp). This is necessary because Spark's MicroBatchExecution uses
 * StreamingRelationV2.output as the schema for the streaming plan, and CDC columns
 * are virtual (not in the table's stored schema).
 *
 * See [[DeltaV2Mode]] for configuration behavior.
 *
 * @param session The Spark session for configuration access
 */
class ApplyV2Streaming(
    @transient private val session: SparkSession)
  extends Rule[LogicalPlan] {

  private def isDeltaStreamingRelation(s: StreamingRelation): Boolean = {
    // Check if this is a Delta streaming relation by examining:
    // 1. The source name (e.g., "delta" from .format("delta"))
    // 2. The catalog table's provider (e.g., "DELTA" from Unity Catalog)
    s.dataSource.catalogTable match {
      case Some(catalogTable) =>
        DeltaSourceUtils.isDeltaDataSourceName(s.sourceName) ||
        catalogTable.provider.exists(DeltaSourceUtils.isDeltaDataSourceName)
      case None =>
        // Path-based sources (.format("delta").load(path)) have no catalogTable.
        // Check sourceName directly to identify Delta sources.
        DeltaSourceUtils.isDeltaDataSourceName(s.sourceName)
    }
  }

  private def shouldApplyV2Streaming(s: StreamingRelation): Boolean = {
    if (!isDeltaStreamingRelation(s)) {
      return false
    }

    val deltaV2Mode = new DeltaV2Mode(session.sessionState.conf)
    deltaV2Mode.isStreamingReadsEnabled(s.dataSource.catalogTable.toJava)
  }

  /** Check if options indicate a CDC read. */
  private def isCDCRead(options: Map[String, String]): Boolean = {
    options.get(DeltaOptions.CDC_READ_OPTION)
      .orElse(options.get(DeltaOptions.CDC_READ_OPTION_LEGACY))
      .exists(_.equalsIgnoreCase("true"))
  }

  /** Check if options indicate a CDC read (from CaseInsensitiveStringMap). */
  private def isCDCRead(options: CaseInsensitiveStringMap): Boolean = {
    val cdcOpt = Option(options.get(DeltaOptions.CDC_READ_OPTION))
      .orElse(Option(options.get(DeltaOptions.CDC_READ_OPTION_LEGACY)))
    cdcOpt.exists(_.equalsIgnoreCase("true"))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    // Case 1: V1 StreamingRelation (from .format("delta").load()) -> convert to V2
    case s: StreamingRelation if shouldApplyV2Streaming(s) =>
      val options = s.dataSource.options

      // Build SparkTable + Identifier from either catalog table or path.
      val (ident, table, catalog) = s.dataSource.catalogTable match {
        case Some(ct) =>
          val id = Identifier.of(ct.identifier.database.toArray, ct.identifier.table)
          val tbl = new SparkTable(
            id,
            ct,
            ScalaUtils.toJavaMap(options))
          val cat = ct.identifier.catalog.map(
            session.sessionState.catalogManager.catalog)
          (id, tbl, cat)

        case None =>
          // Path-based source: extract path from DataSource options.
          val path = options("path")
          val id = Identifier.of(Array.empty, s"delta.`$path`")
          val tbl = new SparkTable(id, path, ScalaUtils.toJavaMap(options))
          (id, tbl, None)
      }

      // For CDC reads, augment schema with CDC metadata columns.
      val outputSchema = if (isCDCRead(options)) {
        CDCReader.cdcReadSchema(table.schema)
      } else {
        table.schema
      }

      StreamingRelationV2(
        source = None,
        sourceName = DeltaSourceUtils.NAME,
        table = table,
        extraOptions = new CaseInsensitiveStringMap(options.asJava),
        output = toAttributes(outputSchema),
        catalog = catalog,
        identifier = Some(ident),
        v1Relation = None)

    // Case 2: V2 StreamingRelationV2 with SparkTable already resolved (from .table() path).
    // Spark creates this directly when the table has MICRO_BATCH_READ capability.
    // For CDC reads, we need to augment the output with CDC metadata columns since Spark's
    // MicroBatchExecution uses StreamingRelationV2.output as-is (no column pruning for V2
    // streaming), and CDC columns are virtual (not in the table's stored schema).
    case s @ StreamingRelationV2(
        _, _, _: SparkTable, options, output,
        _, _, _)
        if isCDCRead(options) && !hasCDCColumns(output) =>
      val cdcCols = toAttributes(CDCReader.cdcReadSchema(
        new org.apache.spark.sql.types.StructType()))
      s.copy(output = output ++ cdcCols)
  }

  /** Check if output already has CDC columns (idempotency guard). */
  private def hasCDCColumns(
      output: Seq[
        org.apache.spark.sql.catalyst.expressions.Attribute
      ]): Boolean = {
    output.exists(_.name == CDCReader.CDC_TYPE_COLUMN_NAME)
  }
}
