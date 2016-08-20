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
class CreateLatestView {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getLatestViewName(GeneratorModel model) {
		if (model.params.get(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "1") {
			return model.getBaseTableName.toLowerCase
		} else {
			return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.LATEST_VIEW_SUFFIX)»'''
		}
	}

	def getColumns(GeneratorModel model) {
		val columns = model.inputTable.columns.values.filter [
			!it.isTemporalValidityColumn(model) && it.virtualColumn == "NO" &&
				it.columnName != model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toUpperCase
		]
		return columns
	}

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			--
			-- Create latest view
			--
			CREATE OR REPLACE FORCE VIEW «model.latestViewName» (
				«FOR col : model.columns»
					«col.columnName.toLowerCase»,
				«ENDFOR»
				PRIMARY KEY («FOR col : model.inputTable.primaryKeyConstraint.columnNames SEPARATOR ", "»«col.toLowerCase»«ENDFOR») RELY DISABLE NOVALIDATE
			) AS
			SELECT «FOR col : model.columns SEPARATOR ',' + System.lineSeparator + '       '»«
			       	»«col.columnName.toLowerCase»«
			       »«ENDFOR»
			  FROM «model.inputTable.getNewTableName(model)»
			«IF model.isTemporalValidity»
			WHERE «model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» IS NULL«
			  » OR «model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» = 0«ENDIF»;
		«ENDIF»
	'''
}
