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

class PrimaryKeyConstraintDaoTest extends AbstractJdbcTest {

	@Test
	def void notFound() {
		val dao = new TableDao(dataSource.connection)
		val pk = dao.getPrimaryKeyConstraint("BONUS", true)
		Assert.assertEquals(null, pk)
	}

	@Test
	def void found() {
		val dao = new TableDao(dataSource.connection)
		val pk = dao.getPrimaryKeyConstraint("EMP", true)
		Assert.assertEquals("PK_EMP", pk.constraintName)
		Assert.assertEquals(#["EMPNO"], pk.columnNames)
		Assert.assertEquals(0, pk.referencingTables.size)
	}

	@Test
	def void foundWithReferencingTables() {
		val dao = new TableDao(dataSource.connection)
		val pk = dao.getPrimaryKeyConstraint("DEPT", true)
		Assert.assertEquals("PK_DEPT", pk.constraintName)
		Assert.assertEquals(#["DEPTNO"], pk.columnNames)
		Assert.assertEquals(1, pk.referencingTables.size)
		val tab = pk.referencingTables.get(0)
		Assert.assertEquals("EMP", tab.tableName)
		Assert.assertEquals(false, tab.historyTable)
		Assert.assertEquals(null, tab.flashbackArchiveTable)
		Assert.assertEquals(#[], tab.temporalValidityPeriods)
		Assert.assertEquals("PK_EMP", tab.primaryKeyConstraint.constraintName)
		Assert.assertEquals(#["EMPNO"], tab.primaryKeyConstraint.columnNames)
		Assert.assertEquals(1, tab.foreignKeyConstraints.size)
		val fk = tab.foreignKeyConstraints.get(0)
		Assert.assertEquals("FK_DEPTNO", fk.constraintName)
		Assert.assertEquals(#["DEPTNO"], fk.columnNames)
		Assert.assertEquals("DEPT", fk.referencedTableName)
		Assert.assertEquals("PK_DEPT", fk.referencedConstraintName)
	}

}
