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
import org.junit.Test
import org.oddgen.bitemp.sqldev.dal.TableDao
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.RenameTable
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class RenameTableTest extends AbstractJdbcTest {

	@Test
	def noRename() {
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "DEPT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		val model = gen.getModel(dataSource.connection, "DEPT", params)
		val template = new RenameTable
		Assert.assertEquals("", template.compile(model.inputTable, model).toString)
	}

	@Test
	def rename() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
			   c1 INTEGER
			)
		''')
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE,"1")
		val model = gen.getModel(dataSource.connection, "T1", params)
		val template = new RenameTable
		val script = template.compile(model.inputTable, model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val dao = new TableDao(dataSource.connection)
		val tableAfter = dao.getTable("T1_LT")
		Assert.assertEquals(1, tableAfter.columns.size)
	}
	
	@AfterClass
	def static void tearDown() {
		try {
		jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}
		try {
		jdbcTemplate.execute("DROP TABLE t1_lt PURGE")
		} catch (Exception e) {
		}
	}
}
