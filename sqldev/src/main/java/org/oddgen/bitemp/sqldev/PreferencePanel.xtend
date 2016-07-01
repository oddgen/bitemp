/*
 * Copyright 2015-2016 Philipp Salvisberg <philipp.salvisberg@trivadis.com>
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
import javax.swing.JCheckBox
import oracle.ide.panels.DefaultTraversablePanel
import oracle.ide.panels.TraversableContext
import oracle.ide.panels.TraversalException
import oracle.javatools.ui.layout.FieldLayoutBuilder
import org.oddgen.bitemp.sqldev.model.PreferenceModel
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class PreferencePanel extends DefaultTraversablePanel {
	final JCheckBox transactionTimeDefaultCheckBox = new JCheckBox()
	final JCheckBox validTimeDefaultCheckBox = new JCheckBox()

	new() {
		layoutControls()
	}

	def private layoutControls() {
		val FieldLayoutBuilder builder = new FieldLayoutBuilder(this)
		builder.alignLabelsLeft = true
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_TRANSACTION_TIME_DEFAULT_LABEL")).component(
				transactionTimeDefaultCheckBox).withHint(
				BitempResources.getString("PREF_TRANSACTION_TIME_DEFAULT_HINT")
			))
		builder.add(
			builder.field.label.withText(BitempResources.getString("PREF_VALID_TIME_DEFAULT_LABEL")).component(
				validTimeDefaultCheckBox).withHint(
				BitempResources.getString("PREF_VALID_TIME_DEFAULT_HINT")
			))
		builder.addVerticalSpring
	}

	override onEntry(TraversableContext traversableContext) {
		var PreferenceModel info = traversableContext.userInformation
		transactionTimeDefaultCheckBox.selected = info.transactionTimeDefault
		validTimeDefaultCheckBox.selected = info.isValidTimeDefault
		super.onEntry(traversableContext)
	}

	override onExit(TraversableContext traversableContext) throws TraversalException {
		var PreferenceModel info = traversableContext.userInformation
		info.transactionTimeDefault = transactionTimeDefaultCheckBox.selected
		info.validTimeDefault = validTimeDefaultCheckBox.selected
		super.onExit(traversableContext)
	}

	def private static PreferenceModel getUserInformation(TraversableContext tc) {
		return PreferenceModel.getInstance(tc.propertyStorage)
	}
}
