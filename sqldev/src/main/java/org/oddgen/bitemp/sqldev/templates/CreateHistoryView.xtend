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
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateHistoryView {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getHistoryViewName(
		GeneratorModel model) {
		return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.HISTORY_VIEW_SUFFIX).toLowerCase»'''
	}

	def getColumns(GeneratorModel model) {
		val columns = model.getNewHistTable.columns.values.filter [
			it.virtualColumn == "NO" && it.hiddenColumn != "YES" &&
				it.columnName != BitempRemodeler.HISTORY_ID_COL_NAME.toUpperCase &&
				it.columnName != BitempRemodeler.VALID_TIME_PERIOD_NAME &&
				it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase
		]
		return columns
	}

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			--
			-- Create history view
			--
			CREATE OR REPLACE FORCE VIEW «model.historyViewName» (
				«FOR col : model.columns»
					«col.columnName.toLowerCase»,
				«ENDFOR»
				PRIMARY KEY («FOR col : model.inputTable.primaryKeyConstraint.columnNames»«col.toLowerCase», «ENDFOR»«model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase») RELY DISABLE NOVALIDATE
			) AS
			SELECT «FOR col : model.columns SEPARATOR ',' + System.lineSeparator + '       '»«
			       	»«col.columnName.toLowerCase»«
			       »«ENDFOR»
			  FROM «model.getNewHistTable.tableName.toLowerCase»
			 WHERE «BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» IS NULL«
			» OR «BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» = 0;
		«ENDIF»
	'''
}
