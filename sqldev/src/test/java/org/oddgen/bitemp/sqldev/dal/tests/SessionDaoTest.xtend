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
import org.oddgen.bitemp.sqldev.dal.SessionDao
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class SessionDaoTest extends AbstractJdbcTest {
	
	@Test
	def void inputTableCandidates() {
		val dao = new SessionDao(dataSource.connection)
		val tables = dao.inputTableCandidates
		Assert.assertEquals(#["DEPT", "EMP"], tables)
	}

	@Test
	def void selectCatalogRole() {
		val dao = new SessionDao(dataSource.connection)
		val hasSelectCatalogRole = dao.hasRole("SELECT_CATALOG_ROLE")
		Assert.assertEquals(true, hasSelectCatalogRole)
	}

	@Test
	def void dba() {
		val dao = new SessionDao(dataSource.connection)
		val hasDba = dao.hasRole("DBA")
		Assert.assertEquals(false, hasDba)
	}
	
	@Test
	def void flashbackArchiveAdminister() {
		val dao = new SessionDao(dataSource.connection)
		val hasFbaAdminister = dao.hasPrivilege("FLASHBACK ARCHIVE ADMINISTER")
		Assert.assertEquals(true, hasFbaAdminister)
	}
	
	@Test
	def void dbmsFlashbackArchive() {
		val dao = new SessionDao(dataSource.connection)
		val hasDbmsFba = dao.hasExecuteRights("DBMS_FLASHBACK_ARCHIVE")
		Assert.assertEquals(true, hasDbmsFba)
	}

	@Test
	def void allFlashbackArchives() {
		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.allFlashbackArchives
		Assert.assertEquals(1, fbas.size)
		Assert.assertEquals("FBA1", fbas.get(0))
	}

	@Test
	def void accessibleFlashbackArchives() {
		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.accessibleFlashbackArchives
		Assert.assertEquals(#["FBA1"], fbas)
	}

	@Test
	def void accessibleFbaWithDefault() {
		sysJdbcTemplate.execute('''
			CREATE FLASHBACK ARCHIVE DEFAULT fba2 TABLESPACE users RETENTION 1 YEAR
		''')

		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.accessibleFlashbackArchives
		sysJdbcTemplate.execute('''
			DROP FLASHBACK ARCHIVE fba2
		''')		
		Assert.assertEquals(#["", "FBA1", "FBA2"], fbas)
	}

	@Test
	def void accessibleUserDefaultImplicit() {
		sysJdbcTemplate.execute('''
			CREATE FLASHBACK ARCHIVE DEFAULT fba2 TABLESPACE users RETENTION 1 YEAR
		''')
		sysJdbcTemplate.execute('''
			REVOKE FLASHBACK ARCHIVE ADMINISTER FROM scott
		''')
		sysJdbcTemplate.execute('''
			GRANT FLASHBACK ARCHIVE ON fba1 TO scott
		''')
		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.accessibleFlashbackArchives
		sysJdbcTemplate.execute('''
			REVOKE FLASHBACK ARCHIVE ON fba1 FROM scott
		''')		
		sysJdbcTemplate.execute('''
			GRANT FLASHBACK ARCHIVE ADMINISTER TO scott
		''')
		sysJdbcTemplate.execute('''
			DROP FLASHBACK ARCHIVE fba2
		''')		
		Assert.assertEquals(#["","FBA1"], fbas)
	}


	@Test
	def void accessibleUserDefaultExplicit() {
		sysJdbcTemplate.execute('''
			CREATE FLASHBACK ARCHIVE DEFAULT fba2 TABLESPACE users RETENTION 1 YEAR
		''')
		sysJdbcTemplate.execute('''
			REVOKE FLASHBACK ARCHIVE ADMINISTER FROM scott
		''')
		sysJdbcTemplate.execute('''
			GRANT FLASHBACK ARCHIVE ON fba2 TO scott
		''')
		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.accessibleFlashbackArchives
		sysJdbcTemplate.execute('''
			REVOKE FLASHBACK ARCHIVE ON fba2 FROM scott
		''')		
		sysJdbcTemplate.execute('''
			GRANT FLASHBACK ARCHIVE ADMINISTER TO scott
		''')
		sysJdbcTemplate.execute('''
			DROP FLASHBACK ARCHIVE fba2
		''')		
		Assert.assertEquals(#["","FBA2"], fbas)
	}

	@Test
	def void accessibleUserDefaultOnly() {
		sysJdbcTemplate.execute('''
			CREATE FLASHBACK ARCHIVE DEFAULT fba2 TABLESPACE users RETENTION 1 YEAR
		''')
		sysJdbcTemplate.execute('''
			REVOKE FLASHBACK ARCHIVE ADMINISTER FROM scott
		''')
		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.accessibleFlashbackArchives
		sysJdbcTemplate.execute('''
			GRANT FLASHBACK ARCHIVE ADMINISTER TO scott
		''')
		sysJdbcTemplate.execute('''
			DROP FLASHBACK ARCHIVE fba2
		''')		
		Assert.assertEquals(#[""], fbas)
	}
	
	@Test
	def void dataFilePath() {
		val dao = new SessionDao(dataSource.connection)
		val path = dao.dataFilePath
		Assert.assertEquals("/u01/app/oracle/oradata/odb/", path)
	}

	@Test
	def void getMissingGeneratorPrerequisites() {
		val dao = new SessionDao(dataSource.connection)
		val prereqs = dao.missingGeneratorPrerequisites
		Assert.assertEquals(0, prereqs.size)
	}
	
	@Test
	def void getMissingInstallPrerequisites() {
		val dao = new SessionDao(dataSource.connection)
		val prereqs = dao.missingInstallPrerequisites
		Assert.assertEquals(0, prereqs.size)
	}
	
	@Test
	def void existsObject() {
		val dao = new SessionDao(dataSource.connection)
		Assert.assertEquals(true, dao.existsObject("TABLE", "EMP"))
		Assert.assertEquals(false, dao.existsObject("TABLE", "EMP_MISSING"))
	}
}
