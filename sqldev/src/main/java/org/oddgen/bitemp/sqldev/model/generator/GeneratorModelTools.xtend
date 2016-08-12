package org.oddgen.bitemp.sqldev.model.generator

import java.util.Collections
import java.util.List
import org.oddgen.bitemp.sqldev.generators.BitempTapiGenerator

class GeneratorModelTools {
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

	def relevantParams(GeneratorModel model) {
		val relevantParams = model.params.keySet.filter [
			(model.paramStates.get(it) == null || model.paramStates.get(it) == true) &&
				it != BitempTapiGenerator.GEN_TRANSACTION_TIME && it != BitempTapiGenerator.GEN_VALID_TIME
		]
		return relevantParams
	}
}
