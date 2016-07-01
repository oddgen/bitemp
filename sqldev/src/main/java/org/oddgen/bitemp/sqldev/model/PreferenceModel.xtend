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
package org.oddgen.bitemp.sqldev.model

import oracle.javatools.data.HashStructure
import oracle.javatools.data.HashStructureAdapter
import oracle.javatools.data.PropertyStorage
import org.eclipse.xtext.xbase.lib.util.ToStringBuilder

class PreferenceModel extends HashStructureAdapter {
	static final String DATA_KEY = "oddgen.bitemp"

	private new(HashStructure hash) {
		super(hash)
	}

	def static getInstance(PropertyStorage prefs) {
		return new PreferenceModel(findOrCreate(prefs, DATA_KEY))
	}

	/** 
	 * default value for transaction time
	 */
	static final String KEY_TRANSACTION_TIME_DEFAULT = "transactionTimeDefault"

	/**
	 * default value for valid time
	 */
	static final String KEY_VALID_TIME_DEFAULT = "validTimeDefault"

	def isTransactionTimeDefault() {
		return getHashStructure.getBoolean(PreferenceModel.KEY_TRANSACTION_TIME_DEFAULT, false)
	}

	def setTransactionTimeDefault(boolean transactionTimeDefault) {
		getHashStructure.putBoolean(PreferenceModel.KEY_TRANSACTION_TIME_DEFAULT, transactionTimeDefault)
	}

	def isValidTimeDefault() {
		return getHashStructure.getBoolean(PreferenceModel.KEY_VALID_TIME_DEFAULT, true)
	}

	def setValidTimeDefault(boolean validTimeDefault) {
		getHashStructure.putBoolean(PreferenceModel.KEY_VALID_TIME_DEFAULT, validTimeDefault)
	}

	override toString() {
		new ToStringBuilder(this).addAllFields.toString
	}
}
