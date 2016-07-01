package org.oddgen.bitemp.sqldev.generators

import org.oddgen.sqldev.generators.OddgenGenerator
import java.sql.Connection
import java.util.LinkedHashMap

class BitempTapiGenerator implements OddgenGenerator {
	
	override getName(Connection conn) {
		return "Bi-temporal TAPI Generator"
	}

	override getDescription(Connection conn) {
		return "Generates non-temporal, uni-temporal or bi-temporal table APIs."
	}

	override getObjectTypes(Connection conn) {
		return #[]
	}

	override getObjectNames(Connection conn, String objectType) {
		return #[]
	}

	override getParams(Connection conn, String objectType, String objectName) {
		return null
	}

	override getLov(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return null
	}

	override getParamStates(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return null
	}

	override generate(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return null
	}
}