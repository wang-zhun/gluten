/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.extension

import org.apache.gluten.GlutenConfig.PLAN_ONLY
import org.apache.gluten.GlutenSparkExtensionsInjector

import org.apache.spark.sql.{Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{CacheTable, CacheTableAsSelect, LogicalPlan, SetCatalogAndNamespace}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.execution.{CommandExecutionMode, GlutenExplainUtils, SQLExecution}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf.ADAPTIVE_EXECUTION_ENABLED
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, MapType, StringType, StructType}

object PlanOnlyExtensions extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser(PlanOnlyParser)
  }
}

case class PlanOnlyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan = {
    if (spark.sessionState.conf.getConf(PLAN_ONLY)) {
      ExplainGlutenPlanCommand(delegate.parsePlan(sqlText))
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(sqlText)
}

case class ExplainGlutenPlanCommand(logicalPlan: LogicalPlan) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {

    def executePlan(plan: LogicalPlan): Seq[Row] = {
      sparkSession.sessionState
        .executePlan(plan, CommandExecutionMode.ALL)
        .executedPlan
        .executeCollect()
      Seq.empty
    }

    logicalPlan match {
      case _: SetCommand | _: ResetCommand | _: SetNamespaceCommand | _: SetCatalogAndNamespace |
          _: AddFilesCommand | _: AddJarsCommand | _: AddArchivesCommand =>
        return executePlan(logicalPlan)
      case cacheTable: CacheTable =>
        val lazyCacheTable = CacheTable(
          table = cacheTable.table,
          multipartIdentifier = cacheTable.multipartIdentifier,
          isLazy = true,
          options = cacheTable.options,
          isAnalyzed = cacheTable.isAnalyzed)
        return executePlan(lazyCacheTable)
      case cacheTable: CacheTableAsSelect =>
        val lazyCacheTable = CacheTableAsSelect(
          tempViewName = cacheTable.tempViewName,
          plan = cacheTable.plan,
          originalText = cacheTable.originalText,
          isLazy = true,
          options = cacheTable.options,
          isAnalyzed = cacheTable.isAnalyzed,
          referredTempFunctions = cacheTable.referredTempFunctions
        )
        return executePlan(lazyCacheTable)
      case s: CreateViewCommand if s.viewType == LocalTempView || s.viewType == GlobalTempView =>
        return executePlan(logicalPlan)
      case s: CreateFunctionCommand if s.isTemp =>
        return executePlan(logicalPlan)
      case _ =>
    }
    val beforeValue = sparkSession.sessionState.conf.adaptiveExecutionEnabled
    sparkSession.sessionState.conf.setConf(ADAPTIVE_EXECUTION_ENABLED, false)
    val execution = sparkSession.sessionState.executePlan(logicalPlan, CommandExecutionMode.SKIP)

    val sc = sparkSession.sparkContext
    val executionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val concat = new PlanStringConcat()
    val (numGlutenNodes, fallbackNodeToReason) =
      GlutenExplainUtils.processPlan(execution.executedPlan, concat.append)
    sparkSession.sessionState.conf.setConf(ADAPTIVE_EXECUTION_ENABLED, beforeValue)
    Seq(
      Row(
        executionId.toLong,
        numGlutenNodes,
        fallbackNodeToReason.size,
        concat.toString(),
        fallbackNodeToReason))
  }

  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("execution_id", LongType, nullable = true)(),
      AttributeReference("num_gluten_nodes", IntegerType, nullable = true)(),
      AttributeReference("num_fallback_nodes", IntegerType, nullable = true)(),
      AttributeReference("physical_plan_description", StringType, nullable = true)(),
      AttributeReference(
        "fallback_node_to_reason",
        MapType(StringType, StringType),
        nullable = true)()
    )
  }
}
