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

import java.sql.Connection
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools

class CreateDataStructure {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(Connection conn, GeneratorModel model) '''
		«val removeFlashbackArchive = new RemoveFlashbackArchive»
		«val removeTemporalValidity = new RemoveTemporalValidity»
		«val removeTable = new RemoveTable»
		«val renameTable = new RenameTable»
		«val addFlashbackArchive = new AddFlashbackArchive»
		«val createHistoryTable = new CreateHistoryTable»
		«val addDeletedIndicatorColumn = new AddDeletedIndicatorColumn»
		«val removeDeletedIndicatorColumn = new RemoveDeletedIndicatorColumn»
		«IF model.targetModel == ApiType.NON_TEMPORAL»
			«removeFlashbackArchive.compile(model.inputTable)»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model.inputTable, model)»
			«removeFlashbackArchive.compile(model.inputTable.histTable)»
			«removeTable.compile(model.inputTable.histTable)»
			«removeDeletedIndicatorColumn.compile(model)»
		«ELSEIF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model.inputTable, model)»
			«removeDeletedIndicatorColumn.compile(model)»
			«addFlashbackArchive.compile(model.inputTable, model)»
			«IF model.originModel == ApiType.BI_TEMPORAL»
				-- TODO migrate data
			«ENDIF»
			«removeFlashbackArchive.compile(model.inputTable.histTable)»
			«removeTable.compile(model.inputTable.histTable)»
		«ELSEIF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
			«removeFlashbackArchive.compile(model.inputTable)»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model.inputTable, model)»
			«removeFlashbackArchive.compile(model.inputTable.histTable)»
			«addDeletedIndicatorColumn.compile(model)»
			«createHistoryTable.compile(model)»
			«IF model.originModel == ApiType.NON_TEMPORAL || model.originModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
				-- TODO populate hist table
			«ENDIF»
		«ELSEIF model.targetModel == ApiType.BI_TEMPORAL»
			«removeFlashbackArchive.compile(model.inputTable)»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model.inputTable, model)»
			«addDeletedIndicatorColumn.compile(model)»
			«createHistoryTable.compile(model)»
			«addFlashbackArchive.compile(model.newHistTable, model)»
			«IF model.originModel == ApiType.NON_TEMPORAL»
				-- TODO populate hist table
			«ELSEIF model.originModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
				-- TODO migrate data
			«ENDIF»
		«ENDIF»
	'''
}
