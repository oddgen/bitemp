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
package org.oddgen.bitemp.sqldev.generators.tests

import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class NonTemporalTest extends AbstractJdbcTest {

	def getCount(String tableName, String whereClause) {
		val count = jdbcTemplate.queryForObject('''
			SELECT COUNT(*)
			  FROM «tableName»
			  «whereClause»
		''', Integer)
		return count
	}

	@Test
	def genDeptBased() {
		jdbcTemplate.execute('''
			CREATE TABLE d1 AS SELECT * FROM dept
		''')
		jdbcTemplate.execute('''
			ALTER TABLE d1 ADD CONSTRAINT d1_pk PRIMARY KEY (deptno, dname)
		''')

		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "D1")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "0")
		params.put(BitempRemodeler.GEN_VALID_TIME, "0")
		val script = gen.generate(dataSource.connection, "TABLE", "D1", params)
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val invalids = jdbcTemplate.queryForObject('''
			SELECT COUNT(*)
			  FROM user_objects
			 WHERE status != 'VALID'
		''', Integer)
		Assert.assertEquals(0, invalids)
		jdbcTemplate.execute('''
			INSERT 
			  INTO d1_lv 
			VALUES (50, 'TEST', 'ZUERICH')
		''')
		Assert.assertEquals(5, getCount("D1", ""))
		jdbcTemplate.execute('''
			UPDATE d1_lv 
			   SET loc = 'Zürich'
			 WHERE deptno = 50
		''')
		Assert.assertEquals(1, getCount("D1", "WHERE loc = 'Zürich'"))
		jdbcTemplate.execute('''
			DELETE 
			  FROM d1_lv 
			 WHERE deptno = 50
		''')
		Assert.assertEquals(4, getCount("D1", ""))
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE d1")
		} catch (Exception e) {
		}
	}

}
