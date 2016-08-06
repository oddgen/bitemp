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
package org.oddgen.bitemp.sqldev.model.preference

import java.util.HashSet
import oracle.javatools.data.HashStructure
import oracle.javatools.data.HashStructureAdapter
import oracle.javatools.data.PropertyStorage
import org.eclipse.xtext.xbase.lib.util.ToStringBuilder
import org.oddgen.bitemp.sqldev.resources.BitempResources

class PreferenceModel extends HashStructureAdapter {
	static final String DATA_KEY = "oddgen.bitemp"

	private new(HashStructure hash) {
		super(hash)
	}

	def static getInstance(PropertyStorage prefs) {
		return new PreferenceModel(findOrCreate(prefs, DATA_KEY))
	}

	static final String KEY_CRUD_COMPATIBILITY_ORIGINAL_TABLE = "crudCompatiblityOriginalTable"
	static final String KEY_LATEST_TABLE_SUFFIX = "latestTableSuffix"
	static final String KEY_LATEST_VIEW_SUFFIX = "latestViewSuffix"
	static final String KEY_GEN_TRANSACTION_TIME = "genTransactionTime"
	static final String KEY_FLASHBACK_ARCHIVE_NAME = "flashbackArchiveName"
	static final String KEY_GEN_VALID_TIME = "genValidTime"
	static final String KEY_GRANULARITY = "granularity"
	static final String KEY_VALID_FROM_COL_NAME = "validFromColName"
	static final String KEY_VALID_TO_COL_NAME = "validToColName"
	static final String KEY_IS_DELETED_COL_NAME = "isDeletedColName"
	static final String KEY_HISTORY_TABLE_SUFFIX = "historyTableSuffix"
	static final String KEY_HISTORY_SEQUENCE_SUFFIX = "historySequenceSuffix"
	static final String KEY_HISTORY_VIEW_SUFFIX = "historyViewSuffix"
	static final String KEY_FULL_HISTORY_VIEW_SUFFIX = "fullHistoryViewSuffix"
	static final String KEY_OBJECT_TYPE_SUFFIX = "objectTypeSuffix"
	static final String KEY_COLLECTION_TYPE_SUFFIX = "collectionTypeSuffix"
	static final String KEY_IOT_SUFFIX = "iotSuffix"
	static final String KEY_API_PACKAGE_SUFFIX = "apiPackageSuffix"
	static final String KEY_HOOK_PACKAGE_SUFFIX = "hookPackageSuffix"

	def isCrudCompatiblityOriginalTable() {
		return getHashStructure.getBoolean(PreferenceModel.KEY_CRUD_COMPATIBILITY_ORIGINAL_TABLE, true)
	}

	def setCrudCompatiblityOriginalTable(boolean crudCompatiblityOriginalTable) {
		getHashStructure.putBoolean(PreferenceModel.KEY_CRUD_COMPATIBILITY_ORIGINAL_TABLE,
			crudCompatiblityOriginalTable)
	}

	def getLatestTableSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_LATEST_TABLE_SUFFIX, "_lt")
	}

	def setLatestTableSuffix(String latestTableSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_LATEST_TABLE_SUFFIX, latestTableSuffix)
	}

	def getLatestViewSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_LATEST_VIEW_SUFFIX, "_lv")
	}

	def setLatestViewSuffix(String latestObjectViewSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_LATEST_VIEW_SUFFIX, latestObjectViewSuffix)
	}

	def isGenTransactionTime() {
		return getHashStructure.getBoolean(PreferenceModel.KEY_GEN_TRANSACTION_TIME, false)
	}

	def setGenTransactionTime(boolean transactionTimeDefault) {
		getHashStructure.putBoolean(PreferenceModel.KEY_GEN_TRANSACTION_TIME, transactionTimeDefault)
	}

	def getFlashbackArchiveName() {
		return getHashStructure.getString(PreferenceModel.KEY_FLASHBACK_ARCHIVE_NAME, "")
	}

	def setFlashbackArchiveName(String flashbackArchiveName) {
		getHashStructure.putString(PreferenceModel.KEY_VALID_TO_COL_NAME, flashbackArchiveName)

	}

	def isGenValidTime() {
		return getHashStructure.getBoolean(PreferenceModel.KEY_GEN_VALID_TIME, true)
	}

	def setGenValidTime(boolean validTimeDefault) {
		getHashStructure.putBoolean(PreferenceModel.KEY_GEN_VALID_TIME, validTimeDefault)
	}

	def getGranularity() {
		val granularity = getHashStructure.getString(PreferenceModel.KEY_GRANULARITY,
			BitempResources.getString("PREF_GRANULARITY_DAY"))
		val validGranularities = new HashSet<String>()
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_YEAR"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_MONTH"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_WEEK"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_DAY"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_SECOND"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_CENTISECOND"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_MILLISECOND"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_MICROSECOND"))
		validGranularities.add(BitempResources.getString("PREF_GRANULARITY_NANOSECOND"))
		if (validGranularities.contains(granularity)) {
			return granularity
		} else {
			// set to default, e.g after changing SQL Developer GUI language
			return BitempResources.getString("PREF_GRANULARITY_DAY")
		}
	}
	
	def setGranularity(String granularity) {
		getHashStructure.putString(PreferenceModel.KEY_GRANULARITY, granularity)		
	}

	def getValidFromColName() {
		return getHashStructure.getString(PreferenceModel.KEY_VALID_FROM_COL_NAME, "valid_from")
	}

	def setValidFromColName(String validFromColName) {
		getHashStructure.putString(PreferenceModel.KEY_VALID_FROM_COL_NAME, validFromColName)

	}

	def getValidToColName() {
		return getHashStructure.getString(PreferenceModel.KEY_VALID_TO_COL_NAME, "valid_to")
	}

	def setValidToColName(String validToColName) {
		getHashStructure.putString(PreferenceModel.KEY_VALID_TO_COL_NAME, validToColName)

	}

	def getIsDeletedColName() {
		return getHashStructure.getString(PreferenceModel.KEY_IS_DELETED_COL_NAME, "is_deleted")
	}

	def setIsDeletedColName(String isDeletedColName) {
		getHashStructure.putString(PreferenceModel.KEY_IS_DELETED_COL_NAME, isDeletedColName)

	}

	def getHistoryTableSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_HISTORY_TABLE_SUFFIX, "_ht")
	}

	def setHistoryTableSuffix(String historyTableSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_HISTORY_TABLE_SUFFIX, historyTableSuffix)

	}

	def getHistorySequenceSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_HISTORY_SEQUENCE_SUFFIX, "_seq")
	}

	def setHistorySequenceSuffix(String historySequenceSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_HISTORY_SEQUENCE_SUFFIX, historySequenceSuffix)

	}

	def getHistoryViewSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_HISTORY_VIEW_SUFFIX, "_hv")
	}

	def setHistoryViewSuffix(String historyViewSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_HISTORY_VIEW_SUFFIX, historyViewSuffix)

	}

	def getFullHistoryViewSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_FULL_HISTORY_VIEW_SUFFIX, "_fhv")
	}

	def setFullHistoryViewSuffix(String historyViewSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_FULL_HISTORY_VIEW_SUFFIX, historyViewSuffix)

	}

	def getObjectTypeSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_OBJECT_TYPE_SUFFIX, "_ot")
	}

	def setObjectTypeSuffix(String objectTypeSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_OBJECT_TYPE_SUFFIX, objectTypeSuffix)

	}

	def getCollectionTypeSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_COLLECTION_TYPE_SUFFIX, "_ct")
	}

	def setCollectionTypeSuffix(String objectTypeSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_COLLECTION_TYPE_SUFFIX, objectTypeSuffix)

	}

	def getIotSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_IOT_SUFFIX, "_trg")
	}

	def setIotSuffix(String iotSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_IOT_SUFFIX, iotSuffix)

	}

	def getApiPackageSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_API_PACKAGE_SUFFIX, "_api")
	}

	def setApiPackageSuffix(String apiPackageSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_API_PACKAGE_SUFFIX, apiPackageSuffix)

	}

	def getHookPackageSuffix() {
		return getHashStructure.getString(PreferenceModel.KEY_HOOK_PACKAGE_SUFFIX, "_hook")
	}

	def setHookPackageSuffix(String hookPackageSuffix) {
		getHashStructure.putString(PreferenceModel.KEY_HOOK_PACKAGE_SUFFIX, hookPackageSuffix)

	}

	override toString() {
		new ToStringBuilder(this).addAllFields.toString
	}
}
