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
import java.util.Collections
import java.util.List
import org.oddgen.bitemp.sqldev.generators.BitempTapiGenerator
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel

class RootTemplate {
	def apiTypeToString(ApiType apiType) {
		switch (apiType) {
			case ApiType.NON_TEMPORAL: "non-temporal"
			case ApiType.UNI_TEMPORAL_TRANSACTION_TIME: "uni-temporal transaction-time"
			case ApiType.UNI_TEMPORAL_VALID_TIME: "uni-temporal valid-time"
			case ApiType.BI_TEMPORAL: "bi-temporal"
			default: "undefined"
		}
	}

	def booleanParamToString(String value) {
		if (value == "1") {
			return "Yes"
		} else if (value == "0") {
			return "No"
		} else {
			return value
		}
	}

	def maxLength(List<String> input) {
		val copy = input.toList
		Collections.sort(copy)[a, b|a.length - b.length]
		return copy.last.length
	}

	def compile(Connection conn, GeneratorModel model) {
		val relevantParams = model.params.keySet.filter [
			(model.paramStates.get(it) == null || model.paramStates.get(it) == true) &&
				it != BitempTapiGenerator.GEN_TRANSACTION_TIME && it != BitempTapiGenerator.GEN_VALID_TIME
		]
		val maxParamLen = maxLength(relevantParams.toList)
		val result = '''
			-- 
			-- bi-temporal TAPI generator configuration
			-- - Input table : «model.inputTable.tableName»
			-- - Origin model: «model.originModel.apiTypeToString»
			-- - Target model: «model.targetModel.apiTypeToString»
			-- - Parameters
			«FOR key : relevantParams»
				--     - «String.format("%1$-" + maxParamLen + "s", key)»: «model.params.get(key).booleanParamToString»
			«ENDFOR»
			--
		'''
		return result
	}
}
