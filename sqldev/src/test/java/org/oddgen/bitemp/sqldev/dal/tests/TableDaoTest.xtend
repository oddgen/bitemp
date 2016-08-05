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
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class TableDaoTest extends AbstractJdbcTest {

	@Test
	def void notFound() {
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("NON_EXISTENT")
		Assert.assertEquals("NON_EXISTENT", table.tableName)
		Assert.assertEquals(false, table.historyTable)
		Assert.assertEquals(null, table.flashbackArchiveTable)
		Assert.assertEquals(#[], table.temporalValidityPeriods)
		Assert.assertEquals(null, table.primaryKeyConstraint)
		Assert.assertEquals(#[], table.foreignKeyConstraints)
	}

	@Test
	def void emp() {
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("EMP")
		Assert.assertEquals("EMP", table.tableName)
		Assert.assertEquals(false, table.historyTable)
		Assert.assertEquals(null, table.flashbackArchiveTable)
		Assert.assertEquals(#[], table.temporalValidityPeriods)
		Assert.assertEquals("PK_EMP", table.primaryKeyConstraint.constraintName)
		Assert.assertEquals(#["EMPNO"], table.primaryKeyConstraint.columnNames)
		Assert.assertEquals(0, table.primaryKeyConstraint.referencingTables.size)
		Assert.assertEquals(1, table.foreignKeyConstraints.size)
	}
	
	@Test
	def void dept() {
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("DEPT")
		Assert.assertEquals("DEPT", table.tableName)
		Assert.assertEquals(false, table.historyTable)
		Assert.assertEquals(null, table.flashbackArchiveTable)
		Assert.assertEquals(#[], table.temporalValidityPeriods)
		Assert.assertEquals("PK_DEPT", table.primaryKeyConstraint.constraintName)
		Assert.assertEquals(#["DEPTNO"], table.primaryKeyConstraint.columnNames)
		Assert.assertEquals(1, table.primaryKeyConstraint.referencingTables.size)
		Assert.assertEquals(0, table.foreignKeyConstraints.size)
	}	

	@Test
	def void compoundKey() {
		try {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
			   c1 INTEGER NOT NULL, 
			   c2 VARCHAR2(20) NOT NULL,
			   c3 DATE,
			   c4 TIMESTAMP,
			   CONSTRAINT t1_pk PRIMARY KEY (c1, c2)
			)
		''')
		} catch (Exception e) {
		}
		try {
		jdbcTemplate.execute('''
			CREATE TABLE t2 (
			   c1 INTEGER NOT NULL, 
			   c2 VARCHAR2(20) NOT NULL,
			   c3 INTEGER NOT NULL,
			   c4 VARCHAR2(50),
			   CONSTRAINT t2_pk PRIMARY KEY (c3),
			   CONSTRAINT t2_t1_fk FOREIGN KEY (c1, c2) REFERENCES t1 (c1, c2)
			)
		''')
		} catch (Exception e) {
		}		
		val dao = new TableDao(dataSource.connection)
		val t1 = dao.getTable("T1")
		Assert.assertEquals("T1", t1.tableName)
		Assert.assertEquals(false, t1.historyTable)
		Assert.assertEquals(null, t1.flashbackArchiveTable)
		Assert.assertEquals(#[], t1.temporalValidityPeriods)
		Assert.assertEquals("T1_PK", t1.primaryKeyConstraint.constraintName)
		Assert.assertEquals(#["C1", "C2"], t1.primaryKeyConstraint.columnNames)
		Assert.assertEquals(1, t1.primaryKeyConstraint.referencingTables.size)
		Assert.assertEquals(0, t1.foreignKeyConstraints.size)
		val t2 = dao.getTable("T2")
		Assert.assertEquals("T2", t2.tableName)
		Assert.assertEquals(false, t2.historyTable)
		Assert.assertEquals(null, t2.flashbackArchiveTable)
		Assert.assertEquals(#[], t2.temporalValidityPeriods)
		Assert.assertEquals("T2_PK", t2.primaryKeyConstraint.constraintName)
		Assert.assertEquals(#["C3"], t2.primaryKeyConstraint.columnNames)
		Assert.assertEquals(0, t2.primaryKeyConstraint.referencingTables.size)
		Assert.assertEquals(1, t2.foreignKeyConstraints.size)
	}	

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE t2 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}
	}

}
