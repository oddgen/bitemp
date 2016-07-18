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
import org.oddgen.bitemp.sqldev.generators.BitempTapiGenerator
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.generators.OddgenGenerator

class BitempTapiGeneratorTest extends AbstractJdbcTest {
	static var OddgenGenerator gen

	@Test
	def getName() {
		Assert.assertEquals(BitempResources.get("GEN_TAPI_NAME"), gen.getName(dataSource.connection))
	}

	@Test
	def getDescription() {
		Assert.assertEquals(BitempResources.get("GEN_TAPI_DESCRIPTION"), gen.getDescription(dataSource.connection))
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
		val params = gen.getParams(dataSource.connection, null, null)
		Assert.assertEquals(15, params.size)
	}

	@Test
	def getLov() {
		val lov = gen.getLov(dataSource.connection, null, null, null)
		Assert.assertEquals(2, lov.size)
	}

	@Test
	def getParamStates() {
		val params = gen.getParams(dataSource.connection, null, null)
		var paramStates = gen.getParamStates(dataSource.connection, null, null, params)
		Assert.assertEquals(8, paramStates.size)
		Assert.assertEquals(false,paramStates.get(BitempTapiGenerator.FLASHBACK_ARCHIVE_NAME))
	}

	@Test
	def generateEmpDefaultTest() {
		val params = gen.getParams(dataSource.connection, "TABLE", "EMP")
		val result = gen.generate(dataSource.connection, "TABLE", "EMP", params)
		val expected = "-- TODO"
		Assert.assertEquals(expected, result)
	}
	
	@BeforeClass
	static def void setup() {
		gen = new BitempTapiGenerator
	}
}
