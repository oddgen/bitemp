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
package org.oddgen.bitemp.sqldev

import com.jcabi.aspects.Loggable
import javax.swing.DefaultComboBoxModel
import javax.swing.JCheckBox
import javax.swing.JComboBox
import javax.swing.JTextField
import oracle.ide.panels.DefaultTraversablePanel
import oracle.ide.panels.TraversableContext
import oracle.ide.panels.TraversalException
import oracle.javatools.ui.layout.FieldLayoutBuilder
import org.oddgen.bitemp.sqldev.model.preference.PreferenceModel
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class PreferencePanel extends DefaultTraversablePanel {
	val JCheckBox genApiCheckBox = new JCheckBox
	val JCheckBox crudCompitiblityForOriginalTableCheckBox = new JCheckBox
	val JTextField latestTableSuffix = new JTextField
	val JTextField latestViewSuffix = new JTextField
	val JCheckBox validTimeDefaultCheckBox = new JCheckBox
	var JComboBox<String> granularityComboBox
	val JCheckBox transactionTimeDefaultCheckBox = new JCheckBox
	val JTextField flashbackArchiveName = new JTextField
	var JComboBox<String> contextLevelComboBox
	val JTextField validFromColName = new JTextField
	val JTextField validToColName = new JTextField
	val JTextField objectTypeSuffix = new JTextField
	val JTextField collectionTypeSuffix = new JTextField
	val JTextField historyTableSuffix = new JTextField
	val JTextField historyViewSuffix = new JTextField
	val JTextField fullHistoryViewSuffix = new JTextField
	val JTextField iotSuffix = new JTextField
	val JTextField apiPackageSuffix = new JTextField
	val JTextField hookPackageSuffix = new JTextField

	new() {
		layoutControls()
	}

	def private layoutControls() {
		val FieldLayoutBuilder builder = new FieldLayoutBuilder(this)
		builder.alignLabelsLeft = true
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_GEN_API_LABEL")).component(genApiCheckBox))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_CRUD_COMPATIBILITY_ORIGINAL_TABLE_LABEL")).
				component(crudCompitiblityForOriginalTableCheckBox))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_GEN_TRANSACTION_TIME_LABEL")).component(
				transactionTimeDefaultCheckBox))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_FLASHBACK_ARCHIVE_NAME_LABEL")).component(
				flashbackArchiveName))
		val contextLevelModel = new DefaultComboBoxModel<String>
		contextLevelModel.addElement(BitempResources.getString("PREF_CONTEXT_LEVEL_ALL"))
		contextLevelModel.addElement(BitempResources.getString("PREF_CONTEXT_LEVEL_TYPICAL"))
		contextLevelModel.addElement(BitempResources.getString("PREF_CONTEXT_LEVEL_NONE"))
		contextLevelModel.addElement(BitempResources.getString("PREF_CONTEXT_LEVEL_KEEP"))
		contextLevelComboBox = new JComboBox<String>(contextLevelModel)
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_FLASHBACK_ARCHIVE_CONTEXT_LEVEL_LABEL")).component(
				contextLevelComboBox))		
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_GEN_VALID_TIME_LABEL")).component(
				validTimeDefaultCheckBox))
		val granularityModel = new DefaultComboBoxModel<String>
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_YEAR"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_MONTH"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_WEEK"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_DAY"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_SECOND"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_CENTISECOND"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_MILLISECOND"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_MICROSECOND"))
		granularityModel.addElement(BitempResources.getString("PREF_GRANULARITY_NANOSECOND"))
		granularityComboBox = new JComboBox<String>(granularityModel)
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_GRANULARITY_LABEL")).component(
				granularityComboBox))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_VALID_FROM_COL_NAME_LABEL")).component(
				validFromColName))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_VALID_TO_COL_NAME_LABEL")).component(
				validToColName))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_LATEST_TABLE_SUFFIX_LABEL")).component(
				latestTableSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_LATEST_VIEW_SUFFIX_LABEL")).component(
				latestViewSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_HISTORY_TABLE_SUFFIX_LABEL")).component(
				historyTableSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_HISTORY_VIEW_SUFFIX_LABEL")).component(
				historyViewSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_FULL_HISTORY_VIEW_SUFFIX_LABEL")).component(
				fullHistoryViewSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_OBJECT_TYPE_SUFFIX_LABEL")).component(
				objectTypeSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_COLLECTION_TYPE_SUFFIX_LABEL")).component(
				collectionTypeSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_IOT_SUFFIX_LABEL")).component(iotSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_API_PACKAGE_SUFFIX_LABEL")).component(
				apiPackageSuffix))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_HOOK_PACKAGE_SUFFIX_LABEL")).component(
				hookPackageSuffix))
		builder.addVerticalSpring
	}

	override onEntry(TraversableContext traversableContext) {
		var PreferenceModel info = traversableContext.userInformation
		genApiCheckBox.selected = info.genApi
		crudCompitiblityForOriginalTableCheckBox.selected = info.crudCompatiblityOriginalTable
		latestTableSuffix.text = info.latestTableSuffix
		latestViewSuffix.text = info.latestViewSuffix
		transactionTimeDefaultCheckBox.selected = info.genTransactionTime
		flashbackArchiveName.text = info.flashbackArchiveName
		contextLevelComboBox.selectedItem = info.flashbackArchiveContextLevel
		validTimeDefaultCheckBox.selected = info.isGenValidTime
		granularityComboBox.selectedItem = info.granularity
		validFromColName.text = info.validFromColName
		validToColName.text = info.validToColName
		latestTableSuffix.text = info.latestTableSuffix
		historyTableSuffix.text = info.historyTableSuffix
		historyViewSuffix.text = info.historyViewSuffix
		fullHistoryViewSuffix.text = info.fullHistoryViewSuffix
		objectTypeSuffix.text = info.objectTypeSuffix
		collectionTypeSuffix.text = info.collectionTypeSuffix
		iotSuffix.text = info.iotSuffix
		apiPackageSuffix.text = info.apiPackageSuffix
		hookPackageSuffix.text = info.hookPackageSuffix
		super.onEntry(traversableContext)
	}

	override onExit(TraversableContext traversableContext) throws TraversalException {
		var PreferenceModel info = traversableContext.userInformation
		info.genApi = genApiCheckBox.selected
		info.crudCompatiblityOriginalTable = crudCompitiblityForOriginalTableCheckBox.selected
		info.latestTableSuffix = latestTableSuffix.text
		info.latestViewSuffix = latestViewSuffix.text
		info.genTransactionTime = transactionTimeDefaultCheckBox.selected
		info.flashbackArchiveName = flashbackArchiveName.text
		info.flashbackArchiveContextLevel = contextLevelComboBox.selectedItem as String
		info.genValidTime = validTimeDefaultCheckBox.selected
		info.granularity = granularityComboBox.selectedItem as String
		info.validFromColName = validFromColName.text
		info.validToColName = validToColName.text
		info.historyTableSuffix = historyTableSuffix.text
		info.historyViewSuffix = historyViewSuffix.text
		info.fullHistoryViewSuffix = fullHistoryViewSuffix.text
		info.objectTypeSuffix = objectTypeSuffix.text
		info.collectionTypeSuffix = collectionTypeSuffix.text
		info.iotSuffix = iotSuffix.text
		info.apiPackageSuffix = apiPackageSuffix.text
		info.hookPackageSuffix = hookPackageSuffix.text
		super.onExit(traversableContext)
	}

	def private static PreferenceModel getUserInformation(TraversableContext tc) {
		return PreferenceModel.getInstance(tc.propertyStorage)
	}
}
