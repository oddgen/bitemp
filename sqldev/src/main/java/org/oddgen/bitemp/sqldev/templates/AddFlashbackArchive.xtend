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

import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.bitemp.sqldev.model.generator.Table
import org.oddgen.bitemp.sqldev.generators.BitempTapiGenerator

class AddFlashbackArchive {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(Table table, GeneratorModel model) '''
		«IF table.exists»
			«IF table.flashbackArchiveTable == null»
				«val newTableName = getNewTableName(table, model)»
				--
				-- Add flashback archive
				--
				ALTER TABLE «newTableName» FLASHBACK ARCHIVE «model.params.get(BitempTapiGenerator.FLASHBACK_ARCHIVE_NAME)»;
			«ENDIF»
		«ENDIF»
	'''
}
