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
class CreateHistoryViewInsteadOfTrigger {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		if (model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME))
			cols.add(model.params.get(BitempRemodeler.VALID_TO_COL_NAME))
		}
		for (col : model.inputTable.columns.values.filter [
			it.virtualColumn == "NO" && it.hiddenColumn != "YES" && !cols.contains(it.columnName) &&
			it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase()
		]) {
			cols.add(col.columnName)
		}
		return cols
	}

	def getPkColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		cols.addAll(model.inputTable.primaryKeyConstraint.columnNames)
		if (model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME))
		}
		return cols
	}

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			«IF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL»
				--
				-- Create instead of trigger on latest view
				--
				CREATE OR REPLACE TRIGGER «model.historyViewInsteadOfTriggerName»
				   INSTEAD OF INSERT OR UPDATE OR DELETE 
				   ON «model.historyViewName»
				DECLARE
					l_old_row «model.objectTypeName»;
					l_new_row «model.objectTypeName»;
				BEGIN
					--
					-- Populate old row
					--
					IF UPDATING OR DELETING THEN
					   l_old_row := NEW «model.objectTypeName»();
					   «FOR col : model.columnNames»
					   	l_old_row.«col.toLowerCase» := :OLD.«col.toLowerCase»;
					   «ENDFOR»
					END IF;
					--
					-- Populate new row
					--
					IF INSERTING OR UPDATING THEN
					   l_new_row := NEW «model.objectTypeName»();
					   «FOR col : model.columnNames»
					   	l_new_row.«col.toLowerCase» := :NEW.«col.toLowerCase»;
					   «ENDFOR»
					END IF;
					--
					-- Call API
					--
					IF INSERTING THEN
						«model.apiPackageName».ins(in_new_row => l_new_row);
					ELSIF UPDATING THEN
						«model.apiPackageName».upd(in_new_row => l_new_row, in_old_row => l_old_row);
					ELSIF DELETING THEN
						«model.apiPackageName».del(in_old_row => l_old_row);
					END IF;
				END «model.historyViewInsteadOfTriggerName»;
				/
			«ENDIF»
		«ENDIF»
	'''
}
