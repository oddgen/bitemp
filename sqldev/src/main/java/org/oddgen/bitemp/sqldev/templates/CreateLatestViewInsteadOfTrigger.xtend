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
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateLatestViewInsteadOfTrigger {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getColumns(GeneratorModel model) {
		val columns = model.inputTable.columns.values.filter [
			!it.isTemporalValidityColumn(model) && it.virtualColumn == "NO" &&
				it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase
		]
		return columns
	}

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			«val validFrom = model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase»
			«val validTo = model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase»
			--
			-- Create instead of trigger on latest view
			--
			CREATE OR REPLACE TRIGGER «model.latestViewInsteadOfTriggerName»
			   INSTEAD OF INSERT OR UPDATE OR DELETE 
			   ON «model.latestViewName»
			DECLARE
			   l_old_row «model.objectTypeName»;
			   l_new_row «model.objectTypeName»;
			   l_«validFrom» «model.validTimeDataType» := SYSTIMESTAMP;
			BEGIN
			   «IF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL»
			   	--
			   	-- Override default temporal period start for update and delete operations
			   	--
			   	IF UPDATING OR DELETING THEN
			   	   SELECT «validFrom»
			   	     INTO l_«validFrom»
			   	     FROM «model.historyViewName»
			   	    WHERE «validTo» IS NULL
			   	      «FOR col : model.inputTable.primaryKeyConstraint.columnNames AFTER ";"»
			   	       	AND «col» = :OLD.«col»
			   	      «ENDFOR»
			   	END IF;
			   «ENDIF»
			   --
			   -- Populate old row
			   --
			   IF UPDATING OR DELETING THEN
			      l_old_row := NEW «model.objectTypeName»();
			      «FOR col : model.columns»
			      	l_old_row.«col.columnName.toLowerCase» := :OLD.«col.columnName.toLowerCase»;
			      «ENDFOR»
			      «IF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL»
			      	l_old_row.«validFrom» := l_«validFrom»;
			      «ENDIF»
			   END IF;
			   --
			   -- Populate new row
			   --
			   IF INSERTING OR UPDATING THEN
			      l_new_row := NEW «model.objectTypeName»();
			      «FOR col : model.columns»
			      	l_new_row.«col.columnName.toLowerCase» := :NEW.«col.columnName.toLowerCase»;
			      «ENDFOR»
			      «IF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL»
			      	l_new_row.«validFrom» := l_«validFrom»;
			      «ENDIF»
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
			END «model.latestViewInsteadOfTriggerName»;
			/
		«ENDIF»
	'''
}
