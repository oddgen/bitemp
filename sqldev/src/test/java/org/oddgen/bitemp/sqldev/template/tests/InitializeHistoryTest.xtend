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
import org.junit.Test
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.InitializeHistory
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class InitializeHistoryTest extends AbstractJdbcTest {

	@Test
	def validTime() {
		jdbcTemplate.execute('''
			CREATE TABLE t1_lt (
				c1 INTEGER PRIMARY KEY,
				c2 VARCHAR2(20)
			)
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t1_lt VALUES (1, 'one');
				INSERT INTO t1_lt VALUES (2, 'two');
				INSERT INTO t1_lt VALUES (3, 'three');
				INSERT INTO t1_lt VALUES (4, 'four');
				INSERT INTO t1_lt VALUES (5, 'five');
				COMMIT;
			END;
		''')
		val template = new InitializeHistory
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1_LT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "1")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "0")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val model = gen.getModel(dataSource.connection, "T1_LT", params)
		jdbcTemplate.execute('''
			CREATE TABLE t1_ht (
			   hist_id$ INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
			   valid_from DATE NULL,
			   valid_to DATE NULL,
			   is_deleted NUMBER(1,0) NULL,
			   CHECK (is_deleted IN (0,1)),
			   PERIOD FOR vt (valid_from, valid_to),
			   c1 INTEGER,
			   c2 VARCHAR2(20)
			)
		''')
		jdbcTemplate.execute('''
			ALTER TABLE t1_ht ADD FOREIGN KEY (c1) REFERENCES t1_lt
		''')
		jdbcTemplate.execute('''
			CREATE INDEX t1_ht_i0$ ON t1_ht (c1)
		''')
		val script = template.compile(model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val rows = jdbcTemplate.queryForObject('''SELECT COUNT(*) FROM t1_ht''', Integer)
		Assert.assertEquals(5, rows)
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("DROP TABLE t1_ht PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1_lt PURGE")
		} catch (Exception e) {
		}
	}
}
