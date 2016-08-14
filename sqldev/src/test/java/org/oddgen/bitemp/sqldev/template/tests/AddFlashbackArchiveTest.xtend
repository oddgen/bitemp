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
import org.oddgen.bitemp.sqldev.dal.TableDao
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.templates.AddFlashbackArchive
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class AddFlashbackArchiveTest extends AbstractJdbcTest {

	@Test
	def unitemporal() {
		jdbcTemplate.execute('''
			CREATE TABLE t1 (c1 INTEGER)
		''')

		val template = new AddFlashbackArchive
		val dao = new TableDao(dataSource.connection)
		val table = dao.getTable("T1")
		Assert.assertTrue(table.flashbackArchiveTable == null)
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "0")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "0")
		val model = gen.getModel(dataSource.connection, "T1", params)
		val script = template.compile(table, model).toString
		for (stmt : script.statements) {
			jdbcTemplate.execute(stmt)
		}
		val tableAfter = dao.getTable("T1")
		Assert.assertTrue(tableAfter.flashbackArchiveTable != null)
	}
	
	@Test
	def fromBitempToUnitemporal() {
		jdbcTemplate.execute('''
			CREATE TABLE t1_text (
			   c0 VARCHAR2(20) PRIMARY KEY
			)
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t1_text (c0) VALUES ('one');
				INSERT INTO t1_text (c0) VALUES ('two');
				INSERT INTO t1_text (c0) VALUES ('three');
				INSERT INTO t1_text (c0) VALUES ('three changed');
				INSERT INTO t1_text (c0) VALUES ('four');
				INSERT INTO t1_text (c0) VALUES ('five');
				INSERT INTO t1_text (c0) VALUES ('six');
				INSERT INTO t1_text (c0) VALUES ('seven');
				COMMIT;
			END;
		''')
		jdbcTemplate.execute('''
			CREATE TABLE t1_lt (
				c1         INTEGER PRIMARY KEY,
				c2         VARCHAR2(20), FOREIGN KEY (c2) REFERENCES t1_text,
				is_deleted NUMBER(1,0)
			)
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (1, 'one', 1);
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (2, 'two', 1);
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (3, 'three changed', null);
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (4, 'four', null);
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (5, 'five', null);
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (6, 'six', null);
				INSERT INTO t1_lt (c1, c2, is_deleted) VALUES (7, 'seven', null);
				COMMIT;
			END;
		''')

		jdbcTemplate.execute('''
			CREATE TABLE t1_ht (
			   hist_id$ INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
			   valid_from DATE NULL,
			   valid_to DATE NULL,
			   is_deleted NUMBER(1,0) NULL,
			   CHECK (is_deleted IN (0,1)),
			   PERIOD FOR vt (valid_from, valid_to),
			   c1 INTEGER, FOREIGN KEY (c1) REFERENCES t1_lt,
			   c2 VARCHAR2(20)
			) FLASHBACK ARCHIVE fba1
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t1_ht (c1, c2) VALUES (1, 'one');
				INSERT INTO t1_ht (c1, c2) VALUES (2, 'two');
				INSERT INTO t1_ht (c1, c2) VALUES (3, 'three');
				INSERT INTO t1_ht (c1, c2) VALUES (4, 'four');
				INSERT INTO t1_ht (c1, c2) VALUES (5, 'five');
				COMMIT;
			END;
		''')
		jdbcTemplate.execute('''
			BEGIN
				UPDATE t1_ht
				   SET is_deleted = 1
				 WHERE c1 IN (1, 2);
				COMMIT;
				UPDATE t1_ht 
				   SET c2 = 'three changed'
				  WHERE c1 = 3;
				COMMIT;
				INSERT INTO t1_ht (c1, c2) VALUES (6, 'six');
				INSERT INTO t1_ht (c1, c2) VALUES (7, 'seven');
				COMMIT;
			END;
		''')
		val template = new AddFlashbackArchive
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1_LT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "1")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "0")
		val model = gen.getModel(dataSource.connection, "T1_LT", params)
		val script = template.compile(model.inputTable, model).toString
		// TODO: proper test using script parser, interactive testing via debugger... (breakpoint on Assert)
		Assert.assertTrue(script != null)		
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
			jdbcTemplate.execute("ALTER TABLE t1_lt NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1_ht PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1_lt PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1_text PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("ALTER TABLE t1 NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t1 PURGE")
		} catch (Exception e) {
		}

	}
}
