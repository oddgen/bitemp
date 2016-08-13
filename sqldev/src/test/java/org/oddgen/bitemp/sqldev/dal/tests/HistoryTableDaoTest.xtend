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
package org.oddgen.bitemp.sqldev.dal.tests

import org.junit.AfterClass
import org.junit.Assert
import org.junit.Test
import org.oddgen.bitemp.sqldev.dal.TableDao
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class HistoryTableDaoTest extends AbstractJdbcTest {

	@Test
	def void notFound() {
		val dao = new TableDao(dataSource.connection)
		val historyTable = dao.isHistoryTable("DEPT")
		Assert.assertEquals(false, historyTable)
	}

	@Test
	def void table1() {
		try {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
			   «BitempRemodeler.HISTORY_ID_COL_NAME» INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, 
			   c1 VARCHAR2(20)
			)
		''')
		} catch (Exception e) {
		}
		val dao = new TableDao(dataSource.connection)
		val historyTable = dao.isHistoryTable("T1")
		Assert.assertEquals(true, historyTable)
	}
	
	@Test
	def void table2() {
		try {
		jdbcTemplate.execute('''
			CREATE TABLE t2 (
			   hist_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, 
			   c1 VARCHAR2(20)
			)
		''')
		} catch (Exception e) {
		}
		val dao = new TableDao(dataSource.connection)
		val historyTable = dao.isHistoryTable("T2")
		Assert.assertEquals(false, historyTable)
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t2 PURGE")
		} catch (Exception e) {
		}
	}
}
