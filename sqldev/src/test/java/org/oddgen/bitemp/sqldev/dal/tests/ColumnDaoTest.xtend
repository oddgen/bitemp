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

import org.junit.Assert
import org.junit.Test
import org.oddgen.bitemp.sqldev.dal.TableDao
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class ColumnDaoTest extends AbstractJdbcTest {

	@Test
	def void dept() {
		val dao = new TableDao(dataSource.connection)
		val cols = dao.getColumns("DEPT")
		Assert.assertEquals(3, cols.size)
		val deptno = cols.get("DEPTNO")
		Assert.assertEquals("NUMBER", deptno.dataType)
		Assert.assertEquals(2, deptno.dataPrecision)
		Assert.assertEquals(0, deptno.dataScale)
		Assert.assertEquals(null, deptno.dataDefault)
	}

	@Test
	def void defaultValue() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (c1 VARCHAR2(20) DEFAULT 'this is a default') 
		''')
		val dao = new TableDao(dataSource.connection)
		val cols = dao.getColumns("T1")
		jdbcTemplate.execute('''
			DROP TABLE t1 PURGE 
		''')
		val c1 = cols.get("C1")
		Assert.assertEquals("'this is a default'", c1.dataDefault)
	}

	@Test
	def void hiddenAndVirtualColumns() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
			   c1 INTEGER,
			   PERIOD FOR vt
			)
		''')
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("T1")
		jdbcTemplate.execute('''
			DROP TABLE t1 PURGE 
		''')
		Assert.assertEquals(4, table.columns.size)
		Assert.assertEquals("NO", table.columns.get("C1").virtualColumn)
		Assert.assertEquals("NO", table.columns.get("C1").hiddenColumn)
		Assert.assertEquals("YES", table.columns.get("VT").virtualColumn)
		Assert.assertEquals("YES", table.columns.get("VT_START").hiddenColumn)
	}
}
