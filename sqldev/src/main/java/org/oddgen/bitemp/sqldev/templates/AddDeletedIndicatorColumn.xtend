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
class AddDeletedIndicatorColumn {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(GeneratorModel model) '''
		«val isDeletedColumName = BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase»
		«IF model.inputTable.columns.get(isDeletedColumName) == null»
			--
			-- Add indicator for deleted rows
			--
			ALTER TABLE «model.latestTableName» ADD (
			   «isDeletedColumName.toLowerCase» NUMBER(1,0) NULL,
			   CHECK («BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» IN (0,1))
			);
		«ENDIF»
	'''
}
