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

import org.junit.Assert
import org.junit.Test
import org.oddgen.bitemp.sqldev.dal.SessionDao
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.bitemp.sqldev.templates.MissingPrerequisiteSolution
import org.oddgen.bitemp.sqldev.tests.AbstractJdbcTest

class MissingPrerequisiteSolutionTest extends AbstractJdbcTest {

	@Test
	def oracle12cRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_ORACLE_12_REQUIRED")»"
			-- upgrade your database to Oracle 12c, see https://docs.oracle.com/database/121/UPGRD/toc.htm
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_ORACLE_12_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def selectCatalogRoleRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_SELECT_CATALOG_ROLE_REQUIRED")»" run the following statement as SYS:
			GRANT SELECT_CATALOG_ROLE TO SCOTT;
			ALTER USER SCOTT DEFAULT ROLE ALL;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_SELECT_CATALOG_ROLE_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def noFlashbackArchive() {
		val dao = new SessionDao(dataSource.connection)
		val fbas = dao.allFlashbackArchives
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_NO_FLASHBACK_ARCHIVE")»" run the following statements as SYS:
			GRANT FLASHBACK ARCHIVE ON «fbas?.get(0)» TO SCOTT;
			-- quotas for flashback archive tablespaces required to ensure the background process does not fail with ORA-01950 when creating archive tables
			-- alternatively to the UNLIMITED TABLESPACE privilege you may set quotes on tablespaces defined in DBA_FLASHBACK_ARCHIVE_TS
			GRANT UNLIMITED TABLESPACE TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_NO_FLASHBACK_ARCHIVE"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def createTableRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_CREATE_TABLE_REQUIRED")»" run the following statement as SYS:
			GRANT CREATE TABLE TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_CREATE_TABLE_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def createViewRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_CREATE_VIEW_REQUIRED")»" run the following statement as SYS:
			GRANT CREATE VIEW TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_CREATE_VIEW_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def flashbackArchiveAdministerRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_FLASHBACK_ARCHIVE_ADMINISTER_REQUIRED")»" run the following statement as SYS:
			GRANT FLASHBACK ARCHIVE ADMINISTER TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_FLASHBACK_ARCHIVE_ADMINISTER_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}


	@Test
	def dbmsFlashbackArchiveRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_DBMS_FLASHBACK_ARCHIVE_REQUIRED")»" run the following statement as SYS:
			GRANT EXECUTE ON SYS.DBMS_FLASHBACK_ARCHIVE TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_DBMS_FLASHBACK_ARCHIVE_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def dbmsFlashbackRequired() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_DBMS_FLASHBACK_REQUIRED")»" run the following statement as SYS:
			GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_DBMS_FLASHBACK_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

}
