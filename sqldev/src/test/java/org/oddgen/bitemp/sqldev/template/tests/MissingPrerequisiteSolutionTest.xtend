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
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_SELECT_CATALOG_ROLE_REQUIRED"))
		Assert.assertEquals(expected, actual)
	}

	@Test
	def noFlashbackArchive() {
		val template = new MissingPrerequisiteSolution
		val expected = '''
			-- to solve "«BitempResources.get("ERROR_NO_FLASHBACK_ARCHIVE")»" run the following statements as SYS:
			GRANT FLASHBACK ARCHIVE ON FBA1 TO SCOTT;
		'''
		val actual = template.compile(dataSource.connection, BitempResources.get("ERROR_NO_FLASHBACK_ARCHIVE"))
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
