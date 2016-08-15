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
package org.oddgen.bitemp.sqldev.generators.tests

import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest
import org.oddgen.sqldev.generators.OddgenGenerator

class BitempRemodelerTest extends AbstractJdbcTest {
	static var OddgenGenerator gen

	@Test
	def getName() {
		Assert.assertEquals(BitempResources.get("GEN_BITEMP_NAME"), gen.getName(dataSource.connection))
	}

	@Test
	def getDescription() {
		Assert.assertEquals(BitempResources.get("GEN_BITEMP_DESCRIPTION"), gen.getDescription(dataSource.connection))
	}

	@Test
	def getObjectTypes() {
		val objectTypes = gen.getObjectTypes(dataSource.connection)
		Assert.assertEquals(#["TABLE"], objectTypes)
	}

	@Test
	def getObjectNamesTest() {
		val objectNames = gen.getObjectNames(dataSource.connection, "TABLE")
		Assert.assertTrue(objectNames.length > 0)
	}

	@Test
	def getParamsTest() {
		val params = gen.getParams(dataSource.connection, "TABLE", null)
		Assert.assertEquals(19, params.size)
	}

	@Test
	def getLov() {
		val lov = gen.getLov(dataSource.connection, "TABLE", null, null)
		Assert.assertEquals(6, lov.size)
	}

	@Test
	def getParamStates() {
		val params = gen.getParams(dataSource.connection, "TABLE", null)
		var paramStates = gen.getParamStates(dataSource.connection, "TABLE", null, params)
		Assert.assertEquals(16, paramStates.size)
		Assert.assertEquals(true, paramStates.get(BitempRemodeler.FLASHBACK_ARCHIVE_NAME))
	}

	@Test
	def generateEmpDefaultTest() {
		val params = gen.getParams(dataSource.connection, "TABLE", "EMP")
		val result = gen.generate(dataSource.connection, "TABLE", "EMP", params)
		val expectedStart = '''
			-- 
			-- Bitemp Remodeler configuration
			-- - Input table : EMP
			-- - Origin model: non-temporal
			-- - Target model: bi-temporal
			-- - Parameters
			--     - Generate table API?                   : Yes
			--     - CRUD compatibility for original table?: No
			--     - Suffix for view with latest content   : _lv
			--     - Flashback data archive name           : FBA1
			--     - Granularity                           : Day
			--     - Column name for valid from            : valid_from
			--     - Column name for valid to              : valid_to
			--     - Column name for is deleted indicator  : is_deleted
			--     - Suffix for history table              : _ht
			--     - Suffix for history view               : _hv
			--     - Suffix for full history view          : _fhv
			--     - Suffix for object type                : _ot
			--     - Suffix for collection type            : _ct
			--     - Suffix for instead-of-trigger         : _trg
			--     - Suffix for API PL/SQL package         : _api
			--     - Suffix for hook PL/SQL package        : _hook
			--
		'''
		Assert.assertEquals(expectedStart, result.substring(
			0,
			expectedStart.length
		))
	}
	
	@Test
	def genEmpBitemp() {
		val params = gen.getParams(dataSource.connection, "TABLE", "EMP")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val result = gen.generate(dataSource.connection, "TABLE", "EMP", params)
		Assert.assertTrue(result != null)
	}

	@BeforeClass
	static def void setup() {
		gen = new BitempRemodeler
	}
}
