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

import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.oddgen.bitemp.sqldev.dal.TableDao
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
		Assert.assertEquals(6, getCount("D2_HT", ""))
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_hv 
			VALUES (SYSDATE, SYSDATE+10, 50, 'TEST', 'Zürich')
		''')
		Assert.assertEquals(5, getCount("D2", ""))
		Assert.assertEquals(7, getCount("D2_HT", ""))
		Assert.assertEquals(5, getCount("D2_LV", ""))
		Assert.assertEquals(6, getCount("D2_HV", ""))
		Assert.assertEquals(0, getCount("D2_LV", "WHERE loc = 'Zürich'"))
		Assert.assertEquals(1, getCount("D2_LV", "WHERE loc = 'ZUERICH'"))
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
		Assert.assertEquals(1, getCount("D2_HT", "WHERE deptno = 60 AND is_deleted$ = 1 AND vt_end < SYSDATE"))
		jdbcTemplate.execute('''
			INSERT 
			  INTO d2_hv 
			VALUES (NULL, SYSDATE, 60, 'TEST2', 'BERN')
		''')
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60 and is_deleted$ IS NULL"))
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60"))
		Assert.assertEquals(1, getCount("D2", "WHERE deptno = 60 AND is_deleted$ IS NULL"))
	}

	@Test
	def alwaysGeneratedIdentityColumn() {
		jdbcTemplate.execute('''
			CREATE TABLE D3 (
			   c1 INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
			   c2 VARCHAR2(20)
			)
		''')
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "D3")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "0")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val script = gen.generate(dataSource.connection, "TABLE", "D3", params)
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val invalids = jdbcTemplate.queryForObject('''
			SELECT COUNT(*)
			  FROM user_objects
			 WHERE status != 'VALID'
			   AND object_name LIKE 'D3%'
		''', Integer)
		Assert.assertEquals(0, invalids)
		jdbcTemplate.execute('''
			INSERT 
			  INTO d3_hv 
			VALUES (NULL, NULL, 200 /* ignored */, 'Text')
		''')
		Assert.assertEquals(1, getCount("D3", ""))
		Assert.assertEquals(1, getCount("D3_HT", ""))
		jdbcTemplate.execute('''
			UPDATE d3_hv
			   SET c2 = 'Text updated'
			 WHERE c2 = 'Text'
		''')
		Assert.assertEquals(1, getCount("D3", "WHERE c2 = 'Text updated'"))
		Assert.assertEquals(1, getCount("D3_HT", "WHERE c2 = 'Text updated'"))
		jdbcTemplate.execute('''
			DELETE
			  FROM d3_hv
		''')
		Assert.assertEquals(0, getCount("D3_LV", ""))
		Assert.assertEquals(0, getCount("D3_HV", ""))
	}

	@Test
	def genBulkLoad() {
		jdbcTemplate.execute('''
			CREATE TABLE d2 (
			   deptno NUMBER(10,0) NOT NULL PRIMARY KEY,
			   dname  VARCHAR2(14) NOT NULL
			)
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
			BEGIN
			   d2_api.create_load_tables();
			END;
		''')
		val dao = new TableDao(dataSource.connection)
		val staCols = dao.getTable("D2_STA$").columns.keySet.toList
		Assert.assertEquals(#["VT_START", "VT_END", "IS_DELETED$", "DEPTNO", "DNAME"], staCols)
		val logCols = dao.getTable("D2_LOG$").columns.keySet.toList
		Assert.assertEquals(
			#["ORA_ERR_NUMBER$", "ORA_ERR_MESG$", "ORA_ERR_ROWID$", "ORA_ERR_OPTYP$", "ORA_ERR_TAG$", "HIST_ID$", "VT_START",
				"VT_END", "IS_DELETED$", "DEPTNO", "DNAME"], logCols)
		jdbcTemplate.execute('''
			DECLARE
			   PROCEDURE ins (
			      in_vt_start    DATE,
			      in_vt_end      DATE,
			      in_is_deleted$ INTEGER,
			      in_deptno      VARCHAR2,
			      in_dname       VARCHAR2
			   ) IS
			   BEGIN
			      INSERT INTO d2_sta$ (
			                     vt_start,
			                     vt_end,
			                     is_deleted$,
			                     deptno,
			                     dname
			                  )
			           VALUES (
			                    in_vt_start,
			                    in_vt_end,
			                    in_is_deleted$,
			                    in_deptno,
			                    in_dname
			                  );
			   END ins;
			BEGIN
			   ins(null, null, null, 10, 'ACCOUNTING');
			   ins(null, DATE '2016-01-01', null, 20, 'RESEARCH');
			   ins(DATE '2016-01-01', null, null, 20, 'Research');
			   ins(DATE '2016-01-01', DATE '3000-01-01', null, 30, 'SALES');
			   ins(DATE '2016-01-01', DATE '2018-01-01', null, 40, 'OPERATIONS');
			   ins(DATE '2010-01-01', DATE '2012-01-01', null, 40, 'OPS BETA');
			END; 
		''')
		Assert.assertEquals(6, getCount("D2_STA$", ""))
		jdbcTemplate.execute('''
			BEGIN
				d2_api.init_load;
			END;
		''')
		Assert.assertEquals(4, getCount("D2", ""))
		Assert.assertEquals(9, getCount("D2_HT", ""))

	}

	@Before
	def void setup() {
		tearDown();
	}

	@After
	def void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE d2_sta$ PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE d2_log$ PURGE")
		} catch (Exception e) {
		}
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
		try {
			jdbcTemplate.execute("ALTER TABLE d3_ht NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE d3_ht PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE d3 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW d3_fhv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW d3_hv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW d3_lv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP PACKAGE d3_api")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP PACKAGE d3_hook")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TYPE d3_ct")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TYPE d3_ot")
		} catch (Exception e) {
		}

	}

}
