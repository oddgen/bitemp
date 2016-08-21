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
class CreateLatestViewInsteadOfTrigger {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getLatestViewName(GeneratorModel model) {
		if (model.params.get(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "1") {
			return model.getBaseTableName.toLowerCase
		} else {
			return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.LATEST_VIEW_SUFFIX).toLowerCase»'''
		}
	}

	def getColumns(GeneratorModel model) {
		val columns = model.inputTable.columns.values.filter [
			!it.isTemporalValidityColumn(model) && it.virtualColumn == "NO" &&
				it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase
		]
		return columns
	}

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			--
			-- Create instead of trigger on latest view
			--
			CREATE OR REPLACE TRIGGER «model.latestViewName.toLowerCase»«model.params.get(BitempRemodeler.IOT_SUFFIX).toLowerCase»
			   INSTEAD OF INSERT OR UPDATE OR DELETE 
			   ON «model.latestViewName»
			DECLARE
				l_old_row «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»;
				l_new_row «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»;
			BEGIN
				--
				-- Populate old row
				--
				IF UPDATING OR DELETING THEN
				   l_old_row := NEW «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»();
				   «FOR col : model.columns»
				   	l_old_row.«col.columnName.toLowerCase» := :OLD.«col.columnName.toLowerCase»;
				   «ENDFOR»
				END IF;
				--
				-- Populate new row
				--
				IF INSERTING OR UPDATING THEN
				   l_new_row := NEW «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»();
				   «FOR col : model.columns»
				   	l_new_row.«col.columnName.toLowerCase» := :NEW.«col.columnName.toLowerCase»;
				   «ENDFOR»
				END IF;
				--
				-- Call API
				--
				IF INSERTING THEN
					«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.API_PACKAGE_SUFFIX).toLowerCase».ins(
						in_new_row => l_new_row
					);
				ELSIF UPDATING THEN
					«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.API_PACKAGE_SUFFIX).toLowerCase».upd(
						in_new_row => l_new_row,
						in_old_row => l_old_row
					);
				ELSIF DELETING THEN
					«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.API_PACKAGE_SUFFIX).toLowerCase».del(
						in_old_row => l_old_row
					);
				END IF;
			END «model.latestViewName.toLowerCase»«model.params.get(BitempRemodeler.IOT_SUFFIX).toLowerCase»;
			/
		«ENDIF»
	'''
}