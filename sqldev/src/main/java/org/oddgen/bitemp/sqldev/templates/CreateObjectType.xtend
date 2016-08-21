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
class CreateObjectType {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			«val columns = model.inputTable.columns.values.filter[
				!it.isTemporalValidityColumn(model) && it.virtualColumn == "NO" && 
				it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase
			]»
			--
			-- Create object type
			--
			CREATE OR REPLACE TYPE «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase» FORCE AS OBJECT (
			   «IF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME ||  model.targetModel == ApiType.BI_TEMPORAL»
			   	«BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase» INTEGER,
			   	«model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase» «model.validTimeDataType»,
			   	«model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase» «model.validTimeDataType»,
			   	«BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» NUMBER(1,0),
			   «ENDIF»
			   «FOR col : columns»
			   	«col.columnName.toLowerCase» «col.fullDataType»,
			   «ENDFOR»
			   CONSTRUCTOR FUNCTION «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase» RETURN SELF AS RESULT
			);
			/
			--
			-- Create object type body
			--
			CREATE OR REPLACE TYPE BODY «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase» IS
				--
				-- Default constructor
				--
				CONSTRUCTOR FUNCTION «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»
				   RETURN SELF AS RESULT IS
				BEGIN
				   RETURN;
				END «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»;
			END;
			/
			--
			-- Create collection type
			--
			CREATE OR REPLACE TYPE «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.COLLECTION_TYPE_SUFFIX).toLowerCase» «
			»AS TABLE OF «model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»;
			/
		«ENDIF»
	'''
}
