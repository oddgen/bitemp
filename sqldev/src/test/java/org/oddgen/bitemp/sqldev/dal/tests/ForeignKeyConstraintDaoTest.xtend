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

class ForeignKeyConstraintDaoTest extends AbstractJdbcTest {

	@Test
	def void notFound() {
		val dao = new TableDao(dataSource.connection)
		val fks = dao.getForeignKeyConstraints("DEPT", true)
		Assert.assertEquals(#[], fks)
	}

	@Test
	def void found() {
		val dao = new TableDao(dataSource.connection)
		val fks = dao.getForeignKeyConstraints("EMP", true)
		Assert.assertEquals(1, fks.size)
		val fk = fks.get(0)
		Assert.assertEquals("FK_DEPTNO", fk.constraintName)
		Assert.assertEquals(#["DEPTNO"], fk.columnNames)
		Assert.assertEquals("PK_DEPT", fk.referencedConstraintName)
		Assert.assertEquals("DEPT", fk.referencedTableName)
	}

}
