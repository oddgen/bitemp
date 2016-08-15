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
import org.oddgen.bitemp.sqldev.dal.TableDao
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.RemoveDeletedIndicatorColumn
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class RemoveDeletedIndicatorColumnTest extends AbstractJdbcTest {

	@Test
	def delRowsAndRemoveCol() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
			   c1 INTEGER PRIMARY KEY,
			   IS_DELETED INTEGER
			)
		''')
		jdbcTemplate.execute('''
			BEGIN
			   INSERT INTO t1 VALUES (1, NULL);
			   INSERT INTO t1 VALUES (2, 1);
			   INSERT INTO t1 VALUES (3, 0);
			END;
		''')
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("T1")
		Assert.assertEquals(2, table.columns.size)
		val template = new RemoveDeletedIndicatorColumn
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "0")
		params.put(BitempRemodeler.GEN_VALID_TIME, "0")
		val model = gen.getModel(dataSource.connection, "T1", params)
		val script = template.compile(model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val rows = jdbcTemplate.queryForObject('''SELECT COUNT(*) FROM t1''', Integer)
		Assert.assertEquals(2, rows)
		val tableAfter = dao.getTable("T1")
		Assert.assertEquals(1, tableAfter.columns.size)
		val modelAfter = gen.getModel(dataSource.connection, "T1", params)
		val scriptAfter = template.compile(modelAfter).toString.trim
		Assert.assertEquals("", scriptAfter)
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}

	}
}
