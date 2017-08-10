/*
 * Copyright 2016 Philipp Salvisberg <philipp.salvisberg@trivadis.com>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.oddgen.bitemp.sqldev.model.generator

import java.util.ArrayList
import java.util.Collections
import java.util.LinkedHashMap
import java.util.List
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.resources.BitempResources

class GeneratorModelTools {
	def apiTypeToString(ApiType apiType) {
		switch (apiType) {
			case ApiType.NON_TEMPORAL: "non-temporal"
			case ApiType.UNI_TEMPORAL_TRANSACTION_TIME: "uni-temporal transaction-time"
			case ApiType.UNI_TEMPORAL_VALID_TIME: "uni-temporal valid-time"
			case ApiType.BI_TEMPORAL: "bi-temporal"
			default: "undefined"
		}
	}

	def booleanParamToString(String value) {
		if (value == "1") {
			return "Yes"
		} else if (value == "0") {
			return "No"
		} else {
			return value
		}
	}

	def maxLength(List<String> input) {
		val copy = input.toList
		Collections.sort(copy)[a, b|a.length - b.length]
		return copy.last.length
	}

	def relevantParams(GeneratorModel model) {
		val relevantParams = model.params.keySet.filter [
			(model.paramStates.get(it) === null || model.paramStates.get(it) == true) &&
				it != BitempRemodeler.GEN_TRANSACTION_TIME && it != BitempRemodeler.GEN_VALID_TIME
		]
		return relevantParams
	}

	def exists(Table table) {
		return table !== null && table.columns !== null && !table.columns.empty
	}

	def getHistTable(Table table) {
		val historyTable = table.primaryKeyConstraint?.referencingTables?.findFirst[it.historyTable]
		return historyTable
	}

	def getNewHistTable(GeneratorModel model) {
		val histTable = model.inputTable.histTable
		if (histTable !==
			null) {
			return histTable
		} else {
			val newHistTable = new Table()
			newHistTable.tableName = '''«model.baseTableName»«model.params.get(BitempRemodeler.HISTORY_TABLE_SUFFIX).toUpperCase»'''
			newHistTable.historyTable = true
			newHistTable.columns = new LinkedHashMap<String, Column>
			newHistTable.columns.put(BitempRemodeler.HISTORY_ID_COL_NAME, model.createHistIdColumn)
			newHistTable.columns.put(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME),
				model.createValidTimeColumn(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME)))
			newHistTable.columns.put(model.params.get(BitempRemodeler.VALID_TO_COL_NAME),
				model.createValidTimeColumn(model.params.get(BitempRemodeler.VALID_TO_COL_NAME)))
			newHistTable.columns.put(BitempRemodeler.IS_DELETED_COL_NAME, model.createIsDeletedColumn)
			newHistTable.columns.putAll(model.inputTable.columns)
			return newHistTable
		}
	}

	def getBaseTableName(GeneratorModel model) {
		if (model.inputTable.tableName.endsWith(model.params.get(BitempRemodeler.LATEST_TABLE_SUFFIX).toUpperCase)) {
			return model.inputTable.tableName.substring(0,
				model.inputTable.tableName.length - model.params.get(BitempRemodeler.LATEST_TABLE_SUFFIX).length)
		} else {
			return '''«model.inputTable.tableName»'''
		}
	}

	def getNewTableName(Table table, GeneratorModel model) {
		if (table.historyTable) {
			if (table.tableName.endsWith(model.params.get(BitempRemodeler.HISTORY_TABLE_SUFFIX).toUpperCase)) {
				return table.
					tableName
			} else {
				return '''«model.inputTable.tableName»«model.params.get(BitempRemodeler.HISTORY_TABLE_SUFFIX).toUpperCase»'''
			}
		} else {
			if (model.params.get(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "1") {
				if (table.tableName.endsWith(model.params.get(BitempRemodeler.LATEST_TABLE_SUFFIX).toUpperCase)) {
					return table.
						tableName
				} else {
					return '''«model.inputTable.tableName»«model.params.get(BitempRemodeler.LATEST_TABLE_SUFFIX).toUpperCase»'''
				}
			} else {
				if (table.tableName.endsWith(model.params.get(BitempRemodeler.LATEST_TABLE_SUFFIX).toUpperCase)) {
					return table.tableName.substring(0,
						table.tableName.length - model.params.get(BitempRemodeler.LATEST_TABLE_SUFFIX).length)
				} else {
					return '''«model.inputTable.tableName»'''
				}
			}
		}
	}
	
	def getStagingTableName(GeneratorModel model) {
		return '''«model.baseTableName»«BitempRemodeler.STAGING_TABLE_SUFFIX»'''
	}

	def getLoggingTableName(GeneratorModel model) {
		return '''«model.baseTableName»«BitempRemodeler.LOGGING_TABLE_SUFFIX»'''
	}

	def getFullDataType(Column column) {
		val result = '''
			«IF column.dataType == "NUMBER"»
				«column.dataType»(«IF column.dataPrecision !== null»«column.dataPrecision»«ELSE»38«ENDIF»«IF column.dataScale !== null», «column.dataScale»«ENDIF»)
			«ELSEIF column.dataType == "FLOAT"»
				«column.dataType»«IF column.dataPrecision !== null»(«column.dataPrecision»)«ENDIF»
			«ELSEIF #["CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"].contains(column.dataType)»
				«column.dataType»(«column.charLength»«IF column.charUsed == "C"» CHAR«ENDIF»)
			«ELSE»
				«column.dataType»
			«ENDIF»
		'''
		return result.toString.trim
	}

	def getNotNull(Column column) {
		val result = '''
			«IF column.nullable == "N"»NOT «ENDIF»NULL
		'''
		return result.toString.trim
	}

	def getDefaultClause(
		Column column) {
		val result = '''
			«IF column.dataDefault !== null && column.identityColumn == "NO"»DEFAULT«IF column.defaultOnNull == "YES"» ON NULL«ENDIF» «column.dataDefault»«ENDIF»
		'''
		return result.toString.trim
	}

	def getValidTimeDataType(GeneratorModel model) {
		return switch (model.params.get(BitempRemodeler.GRANULARITY)) {
			case BitempResources.getString("PREF_GRANULARITY_CENTISECOND"): "TIMESTAMP(2)"
			case BitempResources.getString("PREF_GRANULARITY_MILLISECOND"): "TIMESTAMP(3)"
			case BitempResources.getString("PREF_GRANULARITY_MICROSECOND"): "TIMESTAMP(6)"
			case BitempResources.getString("PREF_GRANULARITY_NANOSECOND"): "TIMESTAMP(9)"
			default: "DATE"
		}
	}

	def getValidTimeDataScale(GeneratorModel model) {
		return switch (model.params.get(BitempRemodeler.GRANULARITY)) {
			case BitempResources.getString("PREF_GRANULARITY_CENTISECOND"): 2
			case BitempResources.getString("PREF_GRANULARITY_MILLISECOND"): 3
			case BitempResources.getString("PREF_GRANULARITY_MICROSECOND"): 6
			case BitempResources.getString("PREF_GRANULARITY_NANOSECOND"): 9
			default: null
		}
	}

	def granularityRequiresTruncation(GeneratorModel model) {
		return switch (model.params.get(BitempRemodeler.GRANULARITY)) {
			case BitempResources.getString("PREF_GRANULARITY_YEAR"): true
			case BitempResources.getString("PREF_GRANULARITY_MONTH"): true
			case BitempResources.getString("PREF_GRANULARITY_WEEK"): true
			case BitempResources.getString("PREF_GRANULARITY_DAY"): true
			case BitempResources.getString("PREF_GRANULARITY_HOUR"): true
			case BitempResources.getString("PREF_GRANULARITY_MINUTE"): true
			default: false // truncated by precision of target column
		}
	}

	def getGranuarityTruncationFormat(GeneratorModel model) {
		return switch (model.params.get(BitempRemodeler.GRANULARITY)) {
			case BitempResources.getString("PREF_GRANULARITY_YEAR"): "YEAR"
			case BitempResources.getString("PREF_GRANULARITY_MONTH"): "MONTH"
			case BitempResources.getString("PREF_GRANULARITY_WEEK"): "IW"
			case BitempResources.getString("PREF_GRANULARITY_DAY"): "DDD"
			case BitempResources.getString("PREF_GRANULARITY_HOUR"): "HH24"
			case BitempResources.getString("PREF_GRANULARITY_MINUTE"): "MI"
		}
	}

	def isTemporalValidityColumn(Column column, GeneratorModel model) {
		for (period : model.inputTable.temporalValidityPeriods) {
			if (column.columnName == period.periodstart || column.columnName == period.periodend ||
				column.columnName == period.periodname) {
				return true
			}
		}
		return false
	}

	def createHistIdColumn(GeneratorModel model) {
		val col = new Column
		col.columnName = BitempRemodeler.HISTORY_ID_COL_NAME.toUpperCase
		col.dataType = "INTEGER"
		col.dataPrecision = null
		col.dataScale = 0
		col.charLength = 0
		col.charUsed = null
		col.nullable = "N"
		col.dataDefault = null
		col.defaultOnNull = "NO"
		col.hiddenColumn = "NO"
		col.virtualColumn = "NO"
		col.identityColumn = "YES"
		col.generationType = null // do not need correct value
		col.sequenceName = null // do not need correct value
		return col
	}

	def createIsDeletedColumn(GeneratorModel model) {
		val col = new Column
		col.columnName = BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase
		col.dataType = "NUMBER"
		col.dataPrecision = 1
		col.dataScale = 0
		col.charLength = 0
		col.charUsed = null
		col.nullable = "Y"
		col.dataDefault = null
		col.defaultOnNull = "NO"
		col.hiddenColumn = "NO"
		col.virtualColumn = "NO"
		col.identityColumn = "NO"
		col.generationType = null
		col.sequenceName = null
		return col
	}

	def createValidTimeColumn(GeneratorModel model, String columnName) {
		val col = new Column
		col.columnName = columnName.toUpperCase
		col.dataType = model.getValidTimeDataType
		col.dataPrecision = null
		col.dataScale = model.getValidTimeDataScale
		col.charLength = 0
		col.charUsed = null
		col.nullable = "Y"
		col.dataDefault = null
		col.defaultOnNull = "NO"
		col.hiddenColumn = "NO"
		col.virtualColumn = "NO"
		col.identityColumn = "NO"
		col.generationType = null
		col.sequenceName = null
		return col
	}

	def isTemporalValidity(GeneratorModel model) {
		return model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL
	}

	def getFullHistoryViewName(
		GeneratorModel model) {
		val name = '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.FULL_HISTORY_VIEW_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getHistoryViewName(
		GeneratorModel model) {
		val name = '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.HISTORY_VIEW_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getHistoryViewInsteadOfTriggerName(GeneratorModel model) {
		val name = '''«model.historyViewName»«model.params.get(BitempRemodeler.IOT_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getHistoryTableName(GeneratorModel model) {
		val name = '''«model.getNewHistTable.tableName.toLowerCase»'''
		return name.toString
	}

	def getLatestViewName(GeneratorModel model) {
		if (model.params.get(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "1") {
			return model.getBaseTableName.
				toLowerCase
		} else {
			return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.LATEST_VIEW_SUFFIX).toLowerCase»'''
		}
	}

	def getAlternativeLatestViewName(GeneratorModel model) {
		if (model.params.get(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "0") {
			return model.getBaseTableName.
				toLowerCase
		} else {
			return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.LATEST_VIEW_SUFFIX).toLowerCase»'''
		}
	}

	def getLatestViewInsteadOfTriggerName(GeneratorModel model) {
		val name = '''«model.latestViewName»«model.params.get(BitempRemodeler.IOT_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getLatestTableName(GeneratorModel model) {
		val name = '''«model.inputTable.getNewTableName(model).toLowerCase»'''
		return name.toString
	}

	def getApiPackageName(
		GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.API_PACKAGE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getHookPackageName(
		GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.HOOK_PACKAGE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getObjectTypeName(
		GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getCollectionTypeName(
		GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.COLLECTION_TYPE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getLatestPkColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		for (col : model.inputTable.primaryKeyConstraint.columnNames) {
			cols.add(col.toLowerCase)
		}
		return cols
	}

	def getHistoryUkColumnNames(GeneratorModel model) {
		val cols = model.latestPkColumnNames
		cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase)
		return cols
	}

}
