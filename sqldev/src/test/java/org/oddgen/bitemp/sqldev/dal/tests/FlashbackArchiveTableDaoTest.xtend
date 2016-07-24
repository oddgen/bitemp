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
import org.oddgen.bitemp.sqldev.dal.FlashbackArchiveTableDao
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class FlashbackArchiveTableDaoTest extends AbstractJdbcTest {

	@Test
	def void getArchiveTableNotFound() {
		val dao = new FlashbackArchiveTableDao(dataSource.connection)
		val table = dao.getArchiveTable("BONUS")
		Assert.assertTrue(table == null)
	}

	@Test
	def void getArchiveTableFound() {
		jdbcTemplate.execute('''
			DECLARE
			   e_fba_exists EXCEPTION;
			   PRAGMA EXCEPTION_INIT(e_fba_exists, -55605);
			BEGIN
			   EXECUTE IMMEDIATE 'CREATE FLASHBACK ARCHIVE fba1 TABLESPACE users RETENTION 1 YEAR';
			EXCEPTION
			   WHEN e_fba_exists THEN
			     NULL;
			END;
		''')
		jdbcTemplate.execute('''
			CREATE TABLE fba_table (c1 integer) FLASHBACK ARCHIVE fba1
		''')
		val dao = new FlashbackArchiveTableDao(dataSource.connection)
		val table = dao.getArchiveTable("FBA_TABLE")
		Assert.assertTrue(table != null)
		Assert.assertEquals("FBA1", table.flashbackArchiveName)
		val objectId = jdbcTemplate.queryForObject('''
			SELECT object_id 
			  FROM user_objects 
			 WHERE object_type = 'TABLE' AND object_name = 'FBA_TABLE'
		''', Integer)
		Assert.assertEquals("SYS_FBA_HIST_" + objectId, table.archiveTableName)
		Assert.assertEquals("ENABLED", table.status)
	}

	@AfterClass
	def static void tearDown() {
		try {
			jdbcTemplate.execute("ALTER TABLE fba_table NO FLASHBACK ARCHIVE")
		} catch (Exception e) {
		}
		try {
			jdbcTemplate.execute("DROP TABLE fba_table PURGE")
		} catch (Exception e) {
		}

	}

}
