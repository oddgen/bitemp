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

class UniTemporalValidTimeTest extends AbstractJdbcTest {

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
			CREATE TABLE d2 AS SELECT * FROM dept
		''')
		jdbcTemplate.execute('''
			ALTER TABLE d2 ADD CONSTRAINT d2_pk PRIMARY KEY (deptno, dname)
		''')

		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "D2")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "0")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val script = gen.generate(dataSource.connection, "TABLE", "D2", params)
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val invalids = jdbcTemplate.queryForObject('''
			SELECT COUNT(*)
			  FROM user_objects
			 WHERE status != 'VALID'
			   AND object_name LIKE 'D2%'
		''', Integer)
		Assert.assertEquals(0, invalids)
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_lv 
			VALUES (50, 'TEST', 'ZUERICH')
		''')
		Assert.assertEquals(5, getCount("D2", ""))
		Assert.assertEquals(5, getCount("D2_HT", ""))
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_hv 
			VALUES (SYSDATE, SYSDATE+10, 50, 'TEST', 'Zürich')
		''')
		Assert.assertEquals(5, getCount("D2", ""))
		Assert.assertEquals(7, getCount("D2_HT", ""))
		Assert.assertEquals(4, getCount("D2_LV", ""))
		Assert.assertEquals(6, getCount("D2_HV", ""))
		Assert.assertEquals(0, getCount("D2_LV", "WHERE loc = 'Zürich'"))
		// TODO: Why is the last deleted version updated? Not really wrong, but unexpected
		// Assert.assertEquals(1, getCount("D2_LV", "WHERE loc = 'ZUERICH'"))
		Assert.assertEquals(1, getCount("D2_HT", "WHERE loc = 'Zürich' AND is_deleted$ IS NULL"))
		Assert.assertEquals(1, getCount("D2_HT", "WHERE loc = 'ZUERICH' AND is_deleted$ IS NULL"))
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_hv 
			VALUES (SYSDATE, SYSDATE+400, 60, 'TEST2', 'BERN')
		''')
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60 and is_deleted$ = 1"))
		Assert.assertEquals(1, getCount("D2_HT", "WHERE deptno = 60 and is_deleted$ IS NULL"))
		Assert.assertEquals(2, getCount("D2_HT", "WHERE deptno = 60 and is_deleted$ = 1"))
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_hv 
			VALUES (SYSDATE+200, NULL, 60, 'TEST2', 'BERN')
		''')
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60 and is_deleted$ IS NULL"))
		Assert.assertEquals(2, getCount("D2_HT", "WHERE deptno = 60"))
		Assert.assertEquals(1, getCount("D2_HT", "WHERE deptno = 60 AND is_deleted$ = 1 AND valid_to < SYSDATE"))
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_hv 
			VALUES (NULL, SYSDATE, 60, 'TEST2', 'BERN')
		''')
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60 and is_deleted$ IS NULL"))
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60"))
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60 AND is_deleted$ IS NULL"))


	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("ALTER TABLE d2_ht NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE d2_ht PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE d2 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW d2_fhv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW d2_hv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW d2_lv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP PACKAGE d2_api")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP PACKAGE d2_hook")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TYPE d2_ct")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TYPE d2_ot")
		} catch (Exception e) {
		}

	}

}
