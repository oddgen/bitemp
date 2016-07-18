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
package org.oddgen.bitemp.sqldev.preferences.tests

import org.junit.Assert
import org.junit.Test
import org.oddgen.bitemp.sqldev.model.PreferenceModel

class PreferenceModelTest {
	@Test
	def testDefaultValues() {
		val PreferenceModel model = PreferenceModel.getInstance(null)
		Assert.assertFalse(model.genTransactionTime)
		Assert.assertEquals("", model.flashbackArchiveName)
		Assert.assertTrue(model.genValidTime)
		Assert.assertEquals("valid_from", model.validFromColName)
		Assert.assertEquals("valid_to", model.validToColName)
		Assert.assertEquals("is_deleted", model.isDeletedColName)
		Assert.assertEquals("_l", model.latestTableSuffix)
		Assert.assertEquals("_h", model.historyTableSuffix)
		Assert.assertEquals("_seq", model.historySequenceSuffix)
		Assert.assertEquals("_v", model.historyViewSuffix)
		Assert.assertEquals("_ot", model.objectTypeSuffix)
		Assert.assertEquals("_ct", model.collectionTypeSuffix)
		Assert.assertEquals("_trg", model.iotSuffix)
		Assert.assertEquals("_api", model.apiPackageSuffix)
		Assert.assertEquals("_hook", model.hookPackageSuffix)
	}
}
