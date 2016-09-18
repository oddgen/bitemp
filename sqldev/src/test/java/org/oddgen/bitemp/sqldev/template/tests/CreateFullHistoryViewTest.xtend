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
import org.junit.BeforeClass
import org.junit.Test
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.CreateFullHistoryView
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class CreateFullHistoryViewTest extends AbstractJdbcTest {

	@Test
	def newBiTemporal() {
		val template = new CreateFullHistoryView
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "DEPT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val model = gen.getModel(dataSource.connection, "DEPT", params)
		val script = template.compile(model).toString
		for (stmt : script.statements) {
			try {
			jdbcTemplate.execute(stmt)
			} catch (Exception e) {
			}
		}
		Assert.assertEquals("INVALID", getObjectStatus("VIEW", "DEPT_FHV"))
		val colCount = jdbcTemplate.queryForObject('''
			SELECT count(*)
			  FROM user_tab_columns
			 WHERE table_name = ?
		''', Integer, #["DEPT_FHV"])
		Assert.assertEquals(12, colCount)
	}

	@Test
	def existingBiTemporal() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (
				c1 INTEGER PRIMARY KEY,
				c2 VARCHAR2(20),
				is_deleted$ NUMBER(1,0) NULL,
				CHECK (is_deleted$ = 1)
			)
		''')
		jdbcTemplate.execute('''
			CREATE TABLE t1_ht (
			   hist_id$ INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
			   vt_start DATE NULL,
			   vt_end DATE NULL,
			   is_deleted$ NUMBER(1,0) NULL,
			   CHECK (is_deleted$ = 1),
			   PERIOD FOR vt$ (vt_start, vt_end),
			   c1 INTEGER,
			   c2 VARCHAR2(20),
			   UNIQUE (c1, vt_start),
			   FOREIGN KEY (c1) REFERENCES t1
			)
		''')
		jdbcTemplate.execute('''
			CREATE INDEX t1_ht_i0$ ON t1_ht (c1)
		''')
		jdbcTemplate.execute('''
			ALTER TABLE t1_ht FLASHBACK ARCHIVE fba1
		''')
		val template = new CreateFullHistoryView
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val model = gen.getModel(dataSource.connection, "T1", params)
		val script = template.compile(model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		Assert.assertEquals("VALID", getObjectStatus("VIEW", "T1_FHV"))
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("ALTER TABLE t1_ht NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1_ht PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW t1_fhv")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP VIEW dept_fhv")
		} catch (Exception e) {
		}
	}
}
