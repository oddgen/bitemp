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
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.List
import oracle.ide.config.Preferences
import org.oddgen.bitemp.sqldev.dal.SessionDao
import org.oddgen.bitemp.sqldev.dal.TableDao
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.preference.PreferenceModel
import org.oddgen.bitemp.sqldev.model.prerequisite.PrerequisiteModel
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.bitemp.sqldev.templates.MissingPrerequisiteSolution
import org.oddgen.sqldev.generators.OddgenGenerator

class BitempTapiGenerator implements OddgenGenerator {

	public static String GEN_API = BitempResources.get("PREF_GEN_API_LABEL")
	public static String CRUD_COMPATIBILITY_ORIGINAL_TABLE = BitempResources.get(
		"PREF_CRUD_COMPATIBILITY_ORIGINAL_TABLE_LABEL")
	public static String LATEST_TABLE_SUFFIX = BitempResources.get("PREF_LATEST_TABLE_SUFFIX_LABEL")
	public static String LATEST_VIEW_SUFFIX = BitempResources.get("PREF_LATEST_VIEW_SUFFIX_LABEL")
	public static String GEN_VALID_TIME = BitempResources.get("PREF_GEN_VALID_TIME_LABEL")
	public static String GRANULARITY = BitempResources.get("PREF_GRANULARITY_LABEL")
	public static String GEN_TRANSACTION_TIME = BitempResources.get("PREF_GEN_TRANSACTION_TIME_LABEL")
	public static String FLASHBACK_ARCHIVE_NAME = BitempResources.get("PREF_FLASHBACK_ARCHIVE_NAME_LABEL")
	public static String VALID_FROM_COL_NAME = BitempResources.get("PREF_VALID_FROM_COL_NAME_LABEL")
	public static String VALID_TO_COL_NAME = BitempResources.get("PREF_VALID_TO_COL_NAME_LABEL")
	public static String IS_DELETED_COL_NAME = BitempResources.get("PREF_IS_DELETED_COL_NAME_LABEL")
	public static String OBJECT_TYPE_SUFFIX = BitempResources.get("PREF_OBJECT_TYPE_SUFFIX_LABEL")
	public static String COLLECTION_TYPE_SUFFIX = BitempResources.get("PREF_COLLECTION_TYPE_SUFFIX_LABEL")
	public static String HISTORY_TABLE_SUFFIX = BitempResources.get("PREF_HISTORY_TABLE_SUFFIX_LABEL")
	public static String HISTORY_VIEW_SUFFIX = BitempResources.get("PREF_HISTORY_VIEW_SUFFIX_LABEL")
	public static String FULL_HISTORY_VIEW_SUFFIX = BitempResources.get("PREF_FULL_HISTORY_VIEW_SUFFIX_LABEL")
	public static String IOT_SUFFIX = BitempResources.get("PREF_IOT_SUFFIX_LABEL")
	public static String API_PACKAGE_SUFFIX = BitempResources.get("PREF_API_PACKAGE_SUFFIX_LABEL")
	public static String HOOK_PACKAGE_SUFFIX = BitempResources.get("PREF_HOOK_PACKAGE_SUFFIX_LABEL")
	public static String HISTORY_ID_COL_NAME = "HIST_ID$"

	private PrerequisiteModel prerequisiteModel = new PrerequisiteModel
	private GeneratorModel generatorModel = new GeneratorModel

	override getName(Connection conn) {
		return BitempResources.get("GEN_TAPI_NAME")
	}

	override getDescription(Connection conn) {
		return BitempResources.get("GEN_TAPI_DESCRIPTION")
	}

	override getObjectTypes(Connection conn) {
		val result = new ArrayList<String>
		conn.populatePrerequisiteModel
		if (prerequisiteModel.missingGeneratePrerequisites.size > 0) {
			result.add(BitempResources.get("MISSING_GENERATE_PREREQUISITES_LABEL"))
		} else {
			result.add("TABLE")
		}
		if (prerequisiteModel.missingInstallPrerequisites.size > 0) {
			result.add(BitempResources.get("MISSING_INSTALL_PREREQUISITES_LABEL"))
		}
		return result
	}

	override getObjectNames(Connection conn, String objectType) {
		if (objectType == "TABLE") {
			val sessionDao = new SessionDao(conn)
			return sessionDao.nonHistoryTables
		} else if (objectType == BitempResources.get("MISSING_INSTALL_PREREQUISITES_LABEL")) {
			return prerequisiteModel.missingInstallPrerequisites
		} else if (objectType == BitempResources.get("MISSING_GENERATE_PREREQUISITES_LABEL")) {
			return prerequisiteModel.missingGeneratePrerequisites
		}
	}

	override getParams(Connection conn, String objectType, String objectName) {
		val params = new LinkedHashMap<String, String>()
		if (objectType == "TABLE") {
			val preferences = Preferences.getPreferences();
			val PreferenceModel pref = PreferenceModel.getInstance(preferences)
			params.put(GEN_API, if(pref.genApi) "1" else "0")
			params.put(CRUD_COMPATIBILITY_ORIGINAL_TABLE, if(pref.crudCompatiblityOriginalTable) "1" else "0")
			params.put(LATEST_TABLE_SUFFIX, pref.latestTableSuffix)
			params.put(LATEST_VIEW_SUFFIX, pref.latestViewSuffix)
			params.put(GEN_TRANSACTION_TIME, if(pref.genTransactionTime) "1" else "0")
			params.put(FLASHBACK_ARCHIVE_NAME, pref.flashbackArchiveName)
			params.put(GEN_VALID_TIME, if(pref.genValidTime) "1" else "0")
			params.put(GRANULARITY, pref.granularity)
			params.put(VALID_FROM_COL_NAME, pref.validFromColName)
			params.put(VALID_TO_COL_NAME, pref.validToColName)
			params.put(IS_DELETED_COL_NAME, pref.isDeletedColName)
			params.put(HISTORY_TABLE_SUFFIX, pref.historyTableSuffix)
			params.put(HISTORY_VIEW_SUFFIX, pref.historyViewSuffix)
			params.put(FULL_HISTORY_VIEW_SUFFIX, pref.fullHistoryViewSuffix)
			params.put(OBJECT_TYPE_SUFFIX, pref.objectTypeSuffix)
			params.put(COLLECTION_TYPE_SUFFIX, pref.collectionTypeSuffix)
			params.put(IOT_SUFFIX, pref.iotSuffix)
			params.put(API_PACKAGE_SUFFIX, pref.apiPackageSuffix)
			params.put(BitempTapiGenerator.HOOK_PACKAGE_SUFFIX, pref.hookPackageSuffix)
		}
		return params
	}

	override getLov(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		val lov = new HashMap<String, List<String>>()
		if (objectType == "TABLE") {
			// true values have to be defined first for a check box to work properly in oddgen v0.2.3
			lov.put(GEN_API, #["1", "0"])
			lov.put(CRUD_COMPATIBILITY_ORIGINAL_TABLE, #["1", "0"])
			lov.put(GEN_TRANSACTION_TIME, #["1", "0"])
			val sessionDao = new SessionDao(conn)
			lov.put(FLASHBACK_ARCHIVE_NAME, sessionDao.accessibleFlashbackArchives)
			lov.put(GEN_VALID_TIME, #["1", "0"])
			lov.put(GRANULARITY,
				#[BitempResources.getString("PREF_GRANULARITY_YEAR"),
					BitempResources.getString("PREF_GRANULARITY_MONTH"),
					BitempResources.getString("PREF_GRANULARITY_WEEK"),
					BitempResources.getString("PREF_GRANULARITY_DAY"),
					BitempResources.getString("PREF_GRANULARITY_SECOND"),
					BitempResources.getString("PREF_GRANULARITY_CENTISECOND"),
					BitempResources.getString("PREF_GRANULARITY_MILLISECOND"),
					BitempResources.getString("PREF_GRANULARITY_MICROSECOND"),
					BitempResources.getString("PREF_GRANULARITY_NANOSECOND")])
		}
		return lov
	}

	override getParamStates(Connection conn, String objectType, String objectName,
		LinkedHashMap<String, String> params) {
		val paramStates = new HashMap<String, Boolean>()
		if (objectType == "TABLE") {
			val isGenApi = params.get(GEN_API) == "1"
			paramStates.put(CRUD_COMPATIBILITY_ORIGINAL_TABLE, isGenApi)
			val isCrudCompatiblityOriginalTable = params.get(CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "1"
			paramStates.put(LATEST_TABLE_SUFFIX, isCrudCompatiblityOriginalTable && isGenApi)
			paramStates.put(LATEST_VIEW_SUFFIX, !isCrudCompatiblityOriginalTable && isGenApi)
			val isTransactionTime = params.get(GEN_TRANSACTION_TIME) == "1"
			paramStates.put(FLASHBACK_ARCHIVE_NAME, isTransactionTime)
			val isValidTime = params.get(GEN_VALID_TIME) == "1"
			paramStates.put(GRANULARITY, isValidTime && isGenApi)
			paramStates.put(VALID_FROM_COL_NAME, isValidTime)
			paramStates.put(VALID_TO_COL_NAME, isValidTime)
			paramStates.put(IS_DELETED_COL_NAME, isValidTime)
			paramStates.put(HISTORY_TABLE_SUFFIX, isValidTime)
			paramStates.put(HISTORY_VIEW_SUFFIX, (isValidTime || isTransactionTime) && isGenApi)
			paramStates.put(FULL_HISTORY_VIEW_SUFFIX, (isValidTime || isTransactionTime) && isGenApi)
			paramStates.put(OBJECT_TYPE_SUFFIX, isGenApi)
			paramStates.put(COLLECTION_TYPE_SUFFIX, isGenApi)
			paramStates.put(IOT_SUFFIX, isGenApi)
			paramStates.put(API_PACKAGE_SUFFIX, isGenApi)
			paramStates.put(HOOK_PACKAGE_SUFFIX, isGenApi)
		}
		return paramStates
	}

	override generate(Connection conn, String objectType, String objectName, LinkedHashMap<String, String> params) {
		if (objectType == "TABLE") {
			populateGeneratorModel(conn, objectName, params)
			return "-- TODO"
		} else {
			val template = new MissingPrerequisiteSolution
			return template.compile(conn, objectName)
		}
	}

	def private populatePrerequisiteModel(Connection conn) {
		val sessionDao = new SessionDao(conn)
		prerequisiteModel.missingGeneratePrerequisites = sessionDao.missingGeneratorPrerequisites
		prerequisiteModel.missingInstallPrerequisites = sessionDao.missingInstallPrerequisites
	}

	def private populateGeneratorModel(Connection conn, String tableName, LinkedHashMap<String, String> params) {
		generatorModel.params = params
		val tableDao = new TableDao(conn)
		generatorModel.inputTable = tableDao.getTable(tableName)
	}

	def getModel() {
		return generatorModel
	}

}
