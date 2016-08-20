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
import org.oddgen.bitemp.sqldev.templates.CreateHookPackageSpecification
import org.oddgen.bitemp.sqldev.templates.CreateObjectType
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class CreateHookPackageSpecificationTest extends AbstractJdbcTest {

	def getStatus(String objectName) {
		val status = jdbcTemplate.queryForObject('''
			SELECT status 
			  FROM user_objects
			 WHERE object_type = 'PACKAGE'
			   AND object_name = ? 
		''', String, #[objectName])
		return status
	}

	@Test
	def deptBitemporal() {
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "DEPT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val model = gen.getModel(dataSource.connection, "DEPT", params)
		for (stmt : (new CreateObjectType).compile(model).toString.statements) {
			jdbcTemplate.execute(stmt)
		}
		val script = (new CreateHookPackageSpecification).compile(model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}	
		Assert.assertEquals("VALID", getStatus("DEPT_HOOK"))
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP PACKAGE dept_hook")
		} catch (Exception e) {
		}
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
