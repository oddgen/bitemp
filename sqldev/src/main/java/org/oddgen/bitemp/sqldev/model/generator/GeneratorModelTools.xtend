package org.oddgen.bitemp.sqldev.model.generator

import java.util.Collections
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
			(model.paramStates.get(it) == null || model.paramStates.get(it) == true) &&
				it != BitempRemodeler.GEN_TRANSACTION_TIME && it != BitempRemodeler.GEN_VALID_TIME
		]
		return relevantParams
	}

	def exists(Table table) {
		return table != null && table.columns != null && !table.columns.empty
	}

	def getHistTable(Table table) {
		val historyTable = table.primaryKeyConstraint?.referencingTables?.findFirst[it.historyTable]
		return historyTable
	}

	def getNewHistTable(GeneratorModel model) {
		val histTable = model.inputTable.histTable
		if (histTable !=
			null) {
			return histTable
		} else {
			val newHistTable = new Table()
			newHistTable.tableName = '''«model.baseTableName»«model.params.get(BitempRemodeler.HISTORY_TABLE_SUFFIX).toUpperCase»'''
			newHistTable.historyTable = true
			newHistTable.columns = model.inputTable.columns
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

	def getFullDataType(Column column) {
		val result = '''
			«IF column.dataType == "NUMBER"»
				«column.dataType»(«IF column.dataPrecision != null»«column.dataPrecision»«ELSE»*«ENDIF»«IF column.dataScale != null», «column.dataScale»«ENDIF»)
			«ELSEIF column.dataType == "FLOAT"»
				«column.dataType»«IF column.dataPrecision != null»(«column.dataPrecision»)«ENDIF»
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
			«IF column.dataDefault != null»DEFAULT«IF column.defaultOnNull == "YES"» ON NULL«ENDIF» «column.dataDefault»«ENDIF»
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

	def isTemporalValidityColumn(Column column, GeneratorModel model) {
		for (period : model.inputTable.temporalValidityPeriods) {
			if (column.columnName == period.periodstart || column.columnName == period.periodend ||
				column.columnName == period.periodname) {
				return true
			}
		}
		return false
	}
}
