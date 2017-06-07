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
import org.oddgen.bitemp.sqldev.templates.RemoveFlashbackArchive
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class RemoveFlashbackArchiveTest extends AbstractJdbcTest {

	@Test
	def noFba() {
		val template = new RemoveFlashbackArchive
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("DEPT")
		Assert.assertEquals("", template.compile(table).toString)
	}

	@Test
	def withFba() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (c1 INTEGER) FLASHBACK ARCHIVE fba1
		''')
		
		val template = new RemoveFlashbackArchive
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("T1")
		Assert.assertTrue(table.flashbackArchiveTable != null)
		val script = template.compile(table).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val tableAfter = dao.getTable("T1")
		Assert.assertTrue(tableAfter.flashbackArchiveTable == null || tableAfter.flashbackArchiveTable.status == 'DISABLED')
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}
	
	@AfterClass
	def static void tearDown() {
		try {
		jdbcTemplate.execute("ALTER TABLE t1 NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
		jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}
		
	}
}
