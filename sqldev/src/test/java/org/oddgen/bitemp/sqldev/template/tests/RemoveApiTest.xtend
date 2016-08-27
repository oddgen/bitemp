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

import org.junit.Assert
import org.junit.Test
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.RemoveApi
import org.oddgen.bitemp.sqldev.templates.RootTemplate
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class RemoveApiTest extends AbstractJdbcTest {

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
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "0")
		params.put(BitempRemodeler.GEN_VALID_TIME, "0")
		val model = gen.getModel(dataSource.connection, "DEPT", params)
		for (stmt : (new RootTemplate).compile(model).toString.statements) {
			jdbcTemplate.execute(stmt)
		}
		val script = (new RemoveApi).compile(model).toString
		val statements = script.statements
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		Assert.assertEquals(5, statements.size)
	}
}
