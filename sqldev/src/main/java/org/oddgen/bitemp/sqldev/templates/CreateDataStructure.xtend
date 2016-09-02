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
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateDataStructure {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(GeneratorModel model) '''
		«val removeFlashbackArchive = new RemoveFlashbackArchive»
		«val removeTemporalValidity = new RemoveTemporalValidity»
		«val removeTable = new RemoveTable»
		«val renameTable = new RenameTable»
		«val addFlashbackArchive = new AddFlashbackArchive»
		«val setFlashbackArchiveContextLevel = new SetFlashbackArchiveContextLevel»
		«val createHistoryTable = new CreateHistoryTable»
		«val addDeletedIndicatorColumn = new AddDeletedIndicatorColumn»
		«val removeDeletedIndicatorColumn = new RemoveDeletedIndicatorColumn»
		«val initializeHistory = new InitializeHistoryTable»
		«val populateFlashbackArchive = new PopulateFlashbackArchive»
		«IF model.targetModel == ApiType.NON_TEMPORAL»
			«removeFlashbackArchive.compile(model.inputTable)»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model)»
			«removeFlashbackArchive.compile(model.inputTable.histTable)»
			«removeTable.compile(model.inputTable.histTable)»
			«removeDeletedIndicatorColumn.compile(model)»
		«ELSEIF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model)»
			«setFlashbackArchiveContextLevel.compile(model)»
			«addFlashbackArchive.compile(model.inputTable, model)»
			«IF model.originModel == ApiType.BI_TEMPORAL»
				«populateFlashbackArchive.compile(model)»
			«ENDIF»
			«removeFlashbackArchive.compile(model.inputTable.histTable)»
			«removeTable.compile(model.inputTable.histTable)»
			«removeDeletedIndicatorColumn.compile(model)»
		«ELSEIF model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
			«removeFlashbackArchive.compile(model.inputTable)»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model)»
			«removeFlashbackArchive.compile(model.inputTable.histTable)»
			«addDeletedIndicatorColumn.compile(model)»
			«createHistoryTable.compile(model)»
			«IF model.originModel == ApiType.NON_TEMPORAL || model.originModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
				«initializeHistory.compile(model)»
			«ENDIF»
		«ELSEIF model.targetModel == ApiType.BI_TEMPORAL»
			«removeTemporalValidity.compile(model.inputTable)»
			«renameTable.compile(model)»
			«addDeletedIndicatorColumn.compile(model)»
			«createHistoryTable.compile(model)»
			«setFlashbackArchiveContextLevel.compile(model)»
			«addFlashbackArchive.compile(model.newHistTable, model)»
			«initializeHistory.compile(model)»
			«initializeHistory.compile(model)»
			«populateFlashbackArchive.compile(model)»
			«removeFlashbackArchive.compile(model.inputTable)»
		«ENDIF»
	'''
}
