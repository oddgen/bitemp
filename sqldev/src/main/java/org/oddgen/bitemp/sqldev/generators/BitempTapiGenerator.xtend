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
package org.oddgen.bitemp.sqldev.generators

import java.sql.Connection
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.List
import org.oddgen.bitemp.sqldev.model.PreferenceModel
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.generators.OddgenGenerator
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource

class BitempTapiGenerator implements OddgenGenerator {

	public static String GEN_VALID_TIME = BitempResources.get("PREF_GEN_VALID_TIME_LABEL")
	public static String GEN_TRANSACTION_TIME = BitempResources.get("PREF_GEN_TRANSACTION_TIME_LABEL")
	public static String FLASHBACK_ARCHIVE_NAME = BitempResources.get("PREF_FLASHBACK_ARCHIVE_NAME_LABEL")
	public static String VALID_FROM_COL_NAME = BitempResources.get("PREF_VALID_FROM_COL_NAME_LABEL")
	public static String VALID_TO_COL_NAME = BitempResources.get("PREF_VALID_TO_COL_NAME_LABEL")
	public static String IS_DELETED_COL_NAME = BitempResources.get("PREF_IS_DELETED_COL_NAME_LABEL")
	public static String OBJECT_TYPE_SUFFIX = BitempResources.get("PREF_OBJECT_TYPE_SUFFIX_LABEL")
	public static String COLLECTION_TYPE_SUFFIX = BitempResources.get("PREF_COLLECTION_TYPE_SUFFIX_LABEL")
	public static String LATEST_TABLE_SUFFIX = BitempResources.get("PREF_LATEST_TABLE_SUFFIX_LABEL")
	public static String HISTORY_TABLE_SUFFIX = BitempResources.get("PREF_HISTORY_TABLE_SUFFIX_LABEL")
	public static String HISTORY_SEQUENCE_SUFFIX = BitempResources.get("PREF_HISTORY_SEQUENCE_SUFFIX_LABEL")
	public static String HISTORY_VIEW_SUFFIX = BitempResources.get("PREF_HISTORY_VIEW_SUFFIX_LABEL")
	public static String IOT_SUFFIX = BitempResources.get("PREF_IOT_SUFFIX_LABEL")
	public static String API_PACKAGE_SUFFIX = BitempResources.get("PREF_API_PACKAGE_SUFFIX_LABEL")
	public static String HOOK_PACKAGE_SUFFIX = BitempResources.get("PREF_HOOK_PACKAGE_SUFFIX_LABEL")

	override getName(Connection conn) {
		return BitempResources.get("GEN_TAPI_NAME")
	}

	override getDescription(Connection conn) {
		return BitempResources.get("GEN_TAPI_DESCRIPTION")
	}

	override getObjectTypes(Connection conn) {
		return #["TABLE"]
	}

	override getObjectNames(Connection conn, String objectType) {
		val sql = '''
			SELECT object_name
			  FROM user_objects
			 WHERE object_type = ?
			   AND generated = 'N'
			ORDER BY object_name
		'''
		val jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(conn, true))
		val objectNames = jdbcTemplate.queryForList(sql, String, objectType)
		return objectNames
	}

	override getParams(Connection conn, String objectType, String objectName) {
		val params = new LinkedHashMap<String, String>()
		val PreferenceModel pref = PreferenceModel.getInstance(null)
		params.put(GEN_TRANSACTION_TIME, if(pref.genTransactionTime) "1" else "0")
		params.put(FLASHBACK_ARCHIVE_NAME, pref.flashbackArchiveName)
		params.put(GEN_VALID_TIME, if(pref.genValidTime) "1" else "0")
		params.put(VALID_FROM_COL_NAME, pref.validFromColName)
		params.put(VALID_TO_COL_NAME, pref.validToColName)
		params.put(IS_DELETED_COL_NAME, pref.isDeletedColName)
		params.put(LATEST_TABLE_SUFFIX, pref.latestTableSuffix)
		params.put(HISTORY_TABLE_SUFFIX, pref.historyTableSuffix)
		params.put(HISTORY_SEQUENCE_SUFFIX, pref.historySequenceSuffix)
		params.put(HISTORY_VIEW_SUFFIX, pref.historyViewSuffix)
		params.put(OBJECT_TYPE_SUFFIX, pref.objectTypeSuffix)
		params.put(COLLECTION_TYPE_SUFFIX, pref.collectionTypeSuffix)
		params.put(IOT_SUFFIX, pref.iotSuffix)
		params.put(API_PACKAGE_SUFFIX, pref.apiPackageSuffix)
		params.put(org.oddgen.bitemp.sqldev.generators.BitempTapiGenerator.HOOK_PACKAGE_SUFFIX, pref.hookPackageSuffix)
		return params
	}

	override getLov(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		val lov = new HashMap<String, List<String>>()
		// true values have to be defined first for checkbox to work properly in v0.2.3
		lov.put(GEN_VALID_TIME, #["1", "0"])
		lov.put(GEN_TRANSACTION_TIME, #["1", "0"])
		return lov
	}

	override getParamStates(Connection conn, String objectType, String objectName,
		LinkedHashMap<String, String> params) {
		val paramStates = new HashMap<String, Boolean>()
		val isTransactionTime = params.get(GEN_TRANSACTION_TIME) == "1"
		paramStates.put(FLASHBACK_ARCHIVE_NAME, isTransactionTime)
		val isValidTime = params.get(GEN_VALID_TIME) == "1"
		paramStates.put(VALID_FROM_COL_NAME, isValidTime)
		paramStates.put(VALID_TO_COL_NAME, isValidTime)
		paramStates.put(IS_DELETED_COL_NAME, isValidTime)
		paramStates.put(LATEST_TABLE_SUFFIX, isValidTime)
		paramStates.put(HISTORY_TABLE_SUFFIX, isValidTime)
		paramStates.put(HISTORY_SEQUENCE_SUFFIX, isValidTime)
		paramStates.put(HISTORY_VIEW_SUFFIX, isValidTime) 
		return paramStates
	}

	override generate(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		return "-- TODO"
	}
}
