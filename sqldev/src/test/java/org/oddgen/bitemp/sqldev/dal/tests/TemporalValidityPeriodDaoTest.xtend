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
import org.oddgen.bitemp.sqldev.model.TemporalValidityPeriod
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class TemporalValidityPeriodDaoTest extends AbstractJdbcTest {

	@Test
	def void notFound() {
		val dao = new TableDao(dataSource.connection)
		val periods = dao.getTemporalValidityPeriods("BONUS")
		Assert.assertEquals(0, periods.size)
	}

	@Test
	def void table1() {
		jdbcTemplate.execute('''
			CREATE TABLE tv_table1 (
			   c1 integer, 
			   PERIOD FOR vt
			)
		''')

		val dao = new TableDao(dataSource.connection)
		val periods = dao.getTemporalValidityPeriods("TV_TABLE1")
		Assert.assertEquals(1, periods.size)
		val TemporalValidityPeriod period = periods.get(0)
		Assert.assertEquals("VT", period.periodname)
		Assert.assertEquals("VT_START", period.periodstart)
		Assert.assertEquals("VT_END", period.periodend)
		Assert.assertEquals(1, period.flags)
	}

	@Test
	def void table2() {
		jdbcTemplate.execute('''
			CREATE TABLE tv_table2 (
			   c1         INTEGER, 
			   valid_from DATE,
			   valid_to   DATE,
			   PERIOD FOR vt (valid_from, valid_to)
			)
		''')
		val dao = new TableDao(dataSource.connection)
		val periods = dao.getTemporalValidityPeriods("TV_TABLE2")
		Assert.assertEquals(1, periods.size)
		val period = periods.get(0)
		Assert.assertEquals("VT", period.periodname)
		Assert.assertEquals("VALID_FROM", period.periodstart)
		Assert.assertEquals("VALID_TO", period.periodend)
		Assert.assertEquals(0, period.flags)
	}

	@Test
	def void table3() {
		jdbcTemplate.execute('''
			CREATE TABLE tv_table3 (
			   c1         integer, 
			   valid_from TIMESTAMP,
			   valid_to   TIMESTAMP,
			   PERIOD FOR vt (valid_from, valid_to)
			)
		''')
		// avoid ORA-54015: Duplicate column expression was specified 
		Thread.sleep(200) 
		jdbcTemplate.execute('''
			alter TABLE tv_table3 ADD (
			   decision_from TIMESTAMP,
			   decision_to   TIMESTAMP,
			   PERIOD FOR dt (decision_from, decision_to)
			)
		''')
		val dao = new TableDao(dataSource.connection)
		val periods = dao.getTemporalValidityPeriods("TV_TABLE3")
		Assert.assertEquals(2, periods.size)
		val vt = periods.filter[it.periodname == "VT"].get(0)
		val dt = periods.filter[it.periodname == "DT"].get(0)
		Assert.assertEquals("VT", vt.periodname)
		Assert.assertEquals("VALID_FROM", vt.periodstart)
		Assert.assertEquals("VALID_TO", vt.periodend)
		Assert.assertEquals(0, vt.flags)
		Assert.assertEquals("DT", dt.periodname)
		Assert.assertEquals("DECISION_FROM", dt.periodstart)
		Assert.assertEquals("DECISION_TO", dt.periodend)
		Assert.assertEquals(0, dt.flags)
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE tv_table1 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE tv_table2 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE tv_table3 PURGE")
		} catch (Exception e) {
		}
	}

}
