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
package org.oddgen.bitemp.sqldev.templates

import com.jcabi.aspects.Loggable
import java.util.ArrayList
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateFullHistoryView {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getFullHistoryViewName(
		GeneratorModel model) {
		return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.FULL_HISTORY_VIEW_SUFFIX).toLowerCase»'''
	}

	def getColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		if (model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add("VERSIONS_STARTSCN")
			cols.add("VERSIONS_ENDSCN")
			cols.add("VERSIONS_STARTTIME")
			cols.add("VERSIONS_ENDTIME")
			cols.add("VERSIONS_XID")
			cols.add("VERSIONS_OPERATION")
		}
		if (model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME))
			cols.add(model.params.get(BitempRemodeler.VALID_TO_COL_NAME))
			cols.add(BitempRemodeler.IS_DELETED_COL_NAME)
		}
		for (col : model.inputTable.columns.values.filter [it.virtualColumn == "NO" && it.hiddenColumn != "YES" && !cols.contains(it.columnName)]) {
			cols.add(col.columnName)
		}
		return cols
	}
	
	def getPkColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		cols.addAll(model.inputTable.primaryKeyConstraint.columnNames)
		if (model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add("VERSIONS_STARTSCN")
		}
		if (model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME))
		}
		return cols
	}

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			«IF model.targetModel != ApiType.NON_TEMPORAL»
				--
				-- Create history view
				--
				CREATE OR REPLACE FORCE VIEW «model.fullHistoryViewName» (
					«FOR col : model.columnNames»
						«col.toLowerCase»,
					«ENDFOR»
					PRIMARY KEY («FOR col : model.pkColumnNames SEPARATOR ", "»«col.toLowerCase»«ENDFOR») RELY DISABLE NOVALIDATE
				) AS
				SELECT «FOR col : model.columnNames SEPARATOR ',' + System.lineSeparator + '       '»«
				       	»«col.toLowerCase»«
				       »«ENDFOR»
				«IF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
					«'  '»FROM «model.inputTable.tableName.toLowerCase» VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE;
				«ELSEIF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
					«'  '»FROM «model.getNewHistTable.tableName.toLowerCase» VERSIONS PERIOD FOR «
					BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE;
				«ELSE»
					«'  '»FROM «model.getNewHistTable.tableName.toLowerCase» VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE VERSIONS PERIOD FOR «
					BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE;
				«ENDIF»
			«ENDIF»
		«ENDIF»		
	'''
}
