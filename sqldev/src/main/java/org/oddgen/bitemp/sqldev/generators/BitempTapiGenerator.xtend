package org.oddgen.bitemp.sqldev.generators

import java.sql.Connection
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.List
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.generators.OddgenGenerator

class BitempTapiGenerator implements OddgenGenerator {
	
	override getName(Connection conn) {
		return BitempResources.get("GEN_NAME")
	}

	override getDescription(Connection conn) {
		return BitempResources.get("GEN_DESCRIPTION")
	}

	override getObjectTypes(Connection conn) {
		return #[]
	}

	override getObjectNames(Connection conn, String objectType) {
		return #[]
	}

	override getParams(Connection conn, String objectType, String objectName) {
		return new LinkedHashMap<String, String>()
	}

	override getLov(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return new HashMap<String, List<String>>()
	}

	override getParamStates(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return new HashMap<String, Boolean>()
	}

	override generate(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return "-- TODO"
	}
}