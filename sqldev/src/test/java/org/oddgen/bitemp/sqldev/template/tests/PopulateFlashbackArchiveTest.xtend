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
import org.oddgen.bitemp.sqldev.templates.PopulateFlashbackArchive
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class PopulateFlashbackArchiveTest extends AbstractJdbcTest {

	@Test
	def toBitemp() {
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
				COMMIT; -- SCN does not count, FBA not enabled
			END;
		''')
		jdbcTemplate.execute('''
			ALTER TABLE t1_lt FLASHBACK ARCHIVE fba1
		''')
		jdbcTemplate.execute('''
			BEGIN
				DELETE FROM t1_lt 
				 WHERE c1 IN (1, 2);
				COMMIT; -- 1st SCN
				UPDATE t1_lt 
				   SET C2 = 'three changed'
				  WHERE c1 = 3;
				COMMIT; -- 2nd SCN
				INSERT INTO t1_lt VALUES (6, 'six');
				INSERT INTO t1_lt VALUES (7, 'seven');
				COMMIT; -- 3rd SCN
			END;
		''')
		val template = new PopulateFlashbackArchive
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T1_LT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "1")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "1")
		val model = gen.getModel(dataSource.connection, "T1_LT", params)
		jdbcTemplate.execute('''
			CREATE TABLE t1_ht (
			   hist_id$ INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
			   vt_start DATE NULL,
			   vt_end DATE NULL,
			   is_deleted$ NUMBER(1,0) NULL,
			   CHECK (is_deleted$ IN (0,1)),
			   PERIOD FOR vt$ (vt_start, vt_end),
			   c1 INTEGER,
			   c2 VARCHAR2(20),
			   UNIQUE (c1, vt_start),
			   FOREIGN KEY (c1) REFERENCES t1_lt
			)
		''')
		jdbcTemplate.execute('''
			CREATE INDEX t1_ht_i0$ ON t1_ht (c1)
		''')
		jdbcTemplate.execute('''
			ALTER TABLE t1_ht FLASHBACK ARCHIVE fba1
		''')
		jdbcTemplate.execute('''
			INSERT INTO t1_ht (c1, c2)
			SELECT c1, c2
			  FROM t1_lt
		''')
		val script = template.compile(model).toString
		val stmts = getStatements(script)
		for (stmt : stmts) {
			jdbcTemplate.execute(stmt)
		}
		val scns = jdbcTemplate.queryForList('''
			SELECT versions_startscn
			 FROM t1_lt VERSIONS BETWEEN TIMESTAMP SYSTIMESTAMP - INTERVAL '1' DAY AND SYSTIMESTAMP
			WHERE versions_startscn IS NOT NULL
			UNION 
			SELECT versions_endscn
			 FROM t1_lt VERSIONS BETWEEN TIMESTAMP SYSTIMESTAMP - INTERVAL '1' DAY AND SYSTIMESTAMP
			WHERE versions_endscn IS NOT NULL
			UNION 
			SELECT 0 
			  FROM DUAL
			UNION
			SELECT dbms_flashback.get_system_change_number
			  FROM DUAL
		''', Integer)
		Assert.assertEquals(5, scns.size)
		for (scn : scns) {
			val count = jdbcTemplate.queryForObject('''
				SELECT COUNT(*) 
				  FROM (
				           SELECT c1, c2
				             FROM t1_ht AS OF SCN ?
				            MINUS
				           SELECT c1, c2
				             FROM t1_lt AS OF SCN ?
				       )
			''', Integer, #[scn, scn])
			Assert.assertEquals(0, count)
		}
		for (scn : scns) {
			val count = jdbcTemplate.queryForObject('''
				SELECT COUNT(*) 
				  FROM (
				           SELECT c1, c2
				             FROM t1_lt AS OF SCN ?
				            MINUS
				           SELECT c1, c2
				             FROM t1_ht AS OF SCN ?
				       )
			''', Integer, #[scn, scn])
			Assert.assertEquals(0, count)
		}
	}

	@Test
	def toUnitempTT() {
		jdbcTemplate.execute('''
			CREATE TABLE t2_text (
			   c0 VARCHAR2(20) PRIMARY KEY
			)
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t2_text (c0) VALUES ('one');
				INSERT INTO t2_text (c0) VALUES ('two');
				INSERT INTO t2_text (c0) VALUES ('three');
				INSERT INTO t2_text (c0) VALUES ('three changed');
				INSERT INTO t2_text (c0) VALUES ('four');
				INSERT INTO t2_text (c0) VALUES ('five');
				INSERT INTO t2_text (c0) VALUES ('six');
				INSERT INTO t2_text (c0) VALUES ('seven');
				COMMIT;
			END;
		''')
		jdbcTemplate.execute('''
			CREATE TABLE t2_lt (
				c1         INTEGER PRIMARY KEY,
				c2         VARCHAR2(20), FOREIGN KEY (c2) REFERENCES t2_text,
				is_deleted$ NUMBER(1,0)
			)
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (1, 'one', 1);
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (2, 'two', 1);
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (3, 'three changed', null);
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (4, 'four', null);
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (5, 'five', null);
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (6, 'six', null);
				INSERT INTO t2_lt (c1, c2, is_deleted$) VALUES (7, 'seven', null);
				COMMIT;
			END;
		''')

		jdbcTemplate.execute('''
			CREATE TABLE t2_ht (
			   hist_id$ INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1000) NOT NULL PRIMARY KEY,
			   vt_start DATE NULL,
			   vt_end DATE NULL,
			   is_deleted$ NUMBER(1,0) NULL,
			   CHECK (is_deleted$ IN (0,1)),
			   PERIOD FOR vt$ (vt_start, vt_end),
			   c1 INTEGER, FOREIGN KEY (c1) REFERENCES t2_lt,
			   c2 VARCHAR2(20),
			   UNIQUE (c1, vt_start)
			) FLASHBACK ARCHIVE fba1
		''')
		jdbcTemplate.execute('''
			BEGIN
				INSERT INTO t2_ht (c1, c2) VALUES (1, 'one');
				INSERT INTO t2_ht (c1, c2) VALUES (2, 'two');
				INSERT INTO t2_ht (c1, c2) VALUES (3, 'three');
				INSERT INTO t2_ht (c1, c2) VALUES (4, 'four');
				INSERT INTO t2_ht (c1, c2) VALUES (5, 'five');
				COMMIT; -- 1st SCN
			END;
		''')
		jdbcTemplate.execute('''
			BEGIN
				UPDATE t2_ht
				   SET is_deleted$ = 1
				 WHERE c1 IN (1, 2);
				COMMIT; -- 2nd SCN
				UPDATE t2_ht 
				   SET c2 = 'three changed'
				  WHERE c1 = 3;
				COMMIT; -- 3rd SCN
				INSERT INTO t2_ht (c1, c2) VALUES (6, 'six');
				INSERT INTO t2_ht (c1, c2) VALUES (7, 'seven');
				COMMIT; -- 4th SCN
			END;
		''')
		val template = new PopulateFlashbackArchive
		val gen = new BitempRemodeler
		val params = gen.getParams(dataSource.connection, "TABLE", "T2_LT")
		params.put(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE, "1")
		params.put(BitempRemodeler.GEN_TRANSACTION_TIME, "1")
		params.put(BitempRemodeler.GEN_VALID_TIME, "0")
		val model = gen.getModel(dataSource.connection, "T2_LT", params)
		jdbcTemplate.execute('''
			ALTER TABLE t2_lt FLASHBACK ARCHIVE fba1
		''')
		// workaround to avoid ORA-1466
		jdbcTemplate.execute('''
			BEGIN
			   dbms_flashback_archive.disassociate_fba(
			      owner_name => USER,
			      table_name => 'T2_HT'
			   );
			   dbms_flashback_archive.reassociate_fba(
			      owner_name => USER,
			      table_name => 'T2_HT'
			   );
			END;
		''')
		val script = template.compile(model).toString
		val stmts = getStatements(script)
		for (stmt : stmts) {
			jdbcTemplate.execute(stmt)
		} 
		// try to avoid wrong query result (3 rows instead of 7)
		Thread.sleep(500)
		val scns = jdbcTemplate.queryForList('''
			SELECT versions_startscn
			 FROM t2_ht VERSIONS BETWEEN TIMESTAMP SYSTIMESTAMP - INTERVAL '1' DAY AND SYSTIMESTAMP
			WHERE versions_startscn IS NOT NULL
			UNION 
			SELECT versions_endscn
			 FROM t2_ht VERSIONS BETWEEN TIMESTAMP SYSTIMESTAMP - INTERVAL '1' DAY AND SYSTIMESTAMP
			WHERE versions_endscn IS NOT NULL
			UNION 
			SELECT 0 
			  FROM DUAL
			UNION
			SELECT dbms_flashback.get_system_change_number
			  FROM DUAL
		''', Integer)
		Assert.assertEquals(7, scns.size)
		for (scn : scns) {
			val count = jdbcTemplate.queryForObject('''
				SELECT COUNT(*) 
				  FROM (
				           SELECT c1, c2, is_deleted$
				             FROM t2_ht AS OF SCN ?
				            WHERE is_deleted$ = 0 OR is_deleted$ IS NULL
				            MINUS
				           SELECT c1, c2, is_deleted$
				             FROM t2_lt AS OF SCN ?
				       )
			''', Integer, #[scn, scn])
			Assert.assertEquals(0, count)
		}
		for (scn : scns) {
			val count = jdbcTemplate.queryForObject('''
				SELECT COUNT(*) 
				  FROM (
				           SELECT c1, c2, is_deleted$
				             FROM t2_lt AS OF SCN ?
				            MINUS
				           SELECT c1, c2, is_deleted$
				             FROM t2_ht AS OF SCN ?
				       )
			''', Integer, #[scn, scn])
			Assert.assertEquals(0, count)
		}
	}

	@BeforeClass
	def static void setup() {
		tearDown();
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute('''
				BEGIN
				   dbms_flashback_archive.reassociate_fba(
				      owner_name => USER, 
				      table_name => 'T1_HT'
				   );
				END;
			''')
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute('''
				BEGIN
				   dbms_flashback_archive.reassociate_fba(
				      owner_name => USER, 
				      table_name => 'T1_LT'
				   );
				END;
			''')
		} catch (Exception e) {
		}
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
			jdbcTemplate.execute('''
				BEGIN
				   dbms_flashback_archive.reassociate_fba(
				      owner_name => USER, 
				      table_name => 'T2_HT'
				   );
				END;
			''')
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute('''
				BEGIN
				   dbms_flashback_archive.reassociate_fba(
				      owner_name => USER, 
				      table_name => 'T2_LT'
				   );
				END;
			''')
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("ALTER TABLE t2_ht NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("ALTER TABLE t2_lt NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t2_ht PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t2_lt PURGE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE t2_text PURGE")
		} catch (Exception e) {
		}
	}
}
