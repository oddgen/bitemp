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
class CreateHistoryTable {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(
		GeneratorModel model) '''
		«IF model.inputTable.exists»
			«val histTable = getHistTable(model.inputTable)»
			«IF histTable == null»
				«val latestTableName = getNewTableName(model.inputTable, model)»
				«val historyTableName = getNewTableName(model.newHistTable, model)»
				--
				-- Create history table
				--
				CREATE TABLE «historyTableName.toLowerCase» (
					«BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase» INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
					«model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase» «model.validTimeDataType» NULL,
					«model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase» «model.validTimeDataType» NULL,
					«BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» NUMBER(1,0) NULL,
					CHECK («BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» IN (0,1)),
					PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» («model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase», «model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase»),
					«FOR col : model.inputTable.columns.values.filter[!it.isTemporalValidityColumn(model) && it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase] SEPARATOR ","»
						«col.columnName.toLowerCase» «col.fullDataType»«IF col.hiddenColumn == "YES"» INVISIBLE«ENDIF»«
						»«IF col.virtualColumn == "YES"» GENERATED ALWAYS AS («col.dataDefault») VIRTUAL«
						»«ELSE»«IF !col.defaultClause.empty» «col.defaultClause»«ENDIF» «col.notNull»«
						»«ENDIF»
					«ENDFOR»
				);
				«val latestPkCols = model.inputTable.primaryKeyConstraint.columnNames»
				ALTER TABLE «historyTableName.toLowerCase» ADD FOREIGN KEY («FOR col : latestPkCols SEPARATOR ", "»«col.toLowerCase»«ENDFOR») REFERENCES «latestTableName.toLowerCase»;
				CREATE INDEX «historyTableName.toLowerCase»_i0$ ON «historyTableName» («FOR col : latestPkCols SEPARATOR ", "»«col.toLowerCase»«ENDFOR»);
				«var int index = 1»
				«FOR fk : model.inputTable.foreignKeyConstraints»
					CREATE INDEX «historyTableName.toLowerCase»_i«index++»$ ON «historyTableName.toLowerCase» («FOR col : fk.columnNames SEPARATOR ", "»«col.toLowerCase»«ENDFOR»);
				«ENDFOR»
			«ENDIF»
		«ENDIF»
	'''
}
