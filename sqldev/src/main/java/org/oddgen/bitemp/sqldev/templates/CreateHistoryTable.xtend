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

import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools

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
				CREATE TABLE «historyTableName» (
					«BitempRemodeler.HISTORY_ID_COL_NAME» INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
					«model.params.get(BitempRemodeler.VALID_FROM_COL_NAME)» «model.validTimeDataType» NULL,
					«model.params.get(BitempRemodeler.VALID_TO_COL_NAME)» «model.validTimeDataType» NULL,
					«model.params.get(BitempRemodeler.IS_DELETED_COL_NAME)» NUMBER(1,0) NULL,
					CHECK («model.params.get(BitempRemodeler.IS_DELETED_COL_NAME)» IN (0,1)),
					PERIOD FOR vt («model.params.get(BitempRemodeler.VALID_FROM_COL_NAME)», «model.params.get(BitempRemodeler.VALID_TO_COL_NAME)»),
					«FOR col : model.inputTable.columns.values.filter[!it.isTemporalValidityColumn(model) && it.columnName != BitempRemodeler.IS_DELETED_COL_NAME] SEPARATOR ","»
						«col.columnName» «col.fullDataType»«IF col.hiddenColumn == "YES"» INVISIBLE«ENDIF»«
						»«IF col.virtualColumn == "YES"» GENERATED ALWAYS AS («col.dataDefault») VIRTUAL«
						»«ELSE»«IF !col.defaultClause.empty» «col.defaultClause»«ENDIF» «col.notNull»«
						»«ENDIF»
					«ENDFOR»
				);
				«val latestPkCols = model.inputTable.primaryKeyConstraint.columnNames»
				ALTER TABLE «historyTableName» ADD FOREIGN KEY («FOR col : latestPkCols SEPARATOR ", "»«col»«ENDFOR») REFERENCES «latestTableName»;
				CREATE INDEX «historyTableName»_I0$ ON «historyTableName» («FOR col : latestPkCols SEPARATOR ", "»«col»«ENDFOR»);
				«var int index = 1»
				«FOR fk : model.inputTable.foreignKeyConstraints»
					CREATE INDEX «historyTableName»_I«index++»$ ON «historyTableName» («FOR col : fk.columnNames SEPARATOR ", "»«col»«ENDFOR»);
				«ENDFOR»
			«ENDIF»
		«ENDIF»
	'''
}
