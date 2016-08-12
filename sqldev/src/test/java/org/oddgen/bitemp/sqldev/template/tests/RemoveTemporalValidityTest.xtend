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
import org.oddgen.bitemp.sqldev.templates.RemoveTemporalValidity
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class RemoveTemporalValidityTest extends AbstractJdbcTest {

	@Test
	def noTemporalValidity() {
		val template = new RemoveTemporalValidity
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("DEPT")
		Assert.assertEquals(0, table.temporalValidityPeriods.size)
		Assert.assertEquals("", template.compile(table).toString)
	}

	@Test
	def withTemporalValidity() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
			   c1 INTEGER,
			   PERIOD FOR vt
			)
		''')
		jdbcTemplate.execute('''
			ALTER TABLE t1 add (PERIOD FOR dt)
		''')
		val template = new RemoveTemporalValidity
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("T1")
		Assert.assertEquals(2, table.temporalValidityPeriods.size)
		val script = template.compile(table).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val tableAfter = dao.getTable("T1")
		Assert.assertEquals(0, tableAfter.temporalValidityPeriods.size)
	}
	
	@AfterClass
	def static void tearDown() {
		try {
		jdbcTemplate.execute("ALTER TABLE t1 DROP (PERIOD FOR vt)")
		} catch (Exception e) {
		}
		try {
		jdbcTemplate.execute("ALTER TABLE t1 DROP (PERIOD FOR dt)")
		} catch (Exception e) {
		}
		try {
		jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}
	}
}
