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
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools

class RootTemplate {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(Connection conn, GeneratorModel model) '''
		-- 
		-- bi-temporal TAPI generator configuration
		-- - Input table : «model.inputTable.tableName»
		-- - Origin model: «model.originModel.apiTypeToString»
		-- - Target model: «model.targetModel.apiTypeToString»
		-- - Parameters
		«val relevantParams = model.relevantParams»
		«val maxParamLen = maxLength(relevantParams.toList)»
		«FOR key : relevantParams»
			--     - «String.format("%1$-" + maxParamLen + "s", key)»: «model.params.get(key).booleanParamToString»
		«ENDFOR»
		--
	'''
}
