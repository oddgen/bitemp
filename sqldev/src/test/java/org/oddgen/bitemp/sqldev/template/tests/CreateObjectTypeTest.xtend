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
package org.oddgen.bitemp.sqldev.template.tests

import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.CreateObjectType
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class CreateObjectTypeTest extends AbstractJdbcTest {

	def getColCount(String tableName) {
		val colCount = jdbcTemplate.queryForObject('''
			SELECT attributes 
			  FROM user_types
			 WHERE type_name = ?
		''', Integer, #[tableName])
		return colCount
	}

	@Test
	def deptBitemporal() {
		val template = new CreateObjectType
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "DEPT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val model = gen.getModel(dataSource.connection, "DEPT", params)
		val script = template.compile(model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		Assert.assertEquals(7, getColCount("DEPT_OT"))
		Assert.assertEquals(0, getColCount("DEPT_CT"))
		Assert.assertEquals("VALID", getObjectStatus("TYPE BODY", "DEPT_OT"))
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TYPE dept_ct")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TYPE dept_ot")
		} catch (Exception e) {
		}

	}
}
