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
package org.oddgen.bitemp.sqldev.templates

import com.jcabi.aspects.Loggable
import java.sql.Connection
import org.oddgen.bitemp.sqldev.dal.SessionDao
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class MissingPrerequisiteSolution {
	def compile(Connection conn, String missingPrerequisite) {
		return switch missingPrerequisite {
			case BitempResources.get("ERROR_ORACLE_12_REQUIRED"): 
				'''
					-- to solve "«BitempResources.get("ERROR_ORACLE_12_REQUIRED")»"
					-- upgrade your database to Oracle 12c, see https://docs.oracle.com/database/121/UPGRD/toc.htm
				'''
			case BitempResources.get("ERROR_SELECT_CATALOG_ROLE_REQUIRED"):
				'''
					-- to solve "«BitempResources.get("ERROR_SELECT_CATALOG_ROLE_REQUIRED")»" run the following statement as SYS:
					GRANT SELECT_CATALOG_ROLE TO «conn.metaData.userName»;
					ALTER USER «conn.metaData.userName» DEFAULT ROLE ALL;
				'''
			case BitempResources.get("ERROR_NO_FLASHBACK_ARCHIVE"):
				'''
					-- to solve "«BitempResources.get("ERROR_NO_FLASHBACK_ARCHIVE")»" run the following statements as SYS:
					«val dao = new SessionDao(conn)»
					«val fbas = dao.allFlashbackArchives»
					«IF fbas.size > 0»
						GRANT FLASHBACK ARCHIVE ON «fbas.get(0)» TO «conn.metaData.userName»;
					«ELSE»
						CREATE TABLESPACE fba DATAFILE '«dao.dataFilePath»fba01.dbf' SIZE 10M REUSE AUTOEXTEND ON NEXT 1M;
						CREATE FLASHBACK ARCHIVE fba TABLESPACE fba RETENTION 1 YEAR;
						GRANT FLASHBACK ARCHIVE ON fba TO «conn.metaData.userName»;
					«ENDIF»
					-- quotas for flashback archive tablespaces required to ensure the background process does not fail with ORA-01950 when creating archive tables
					-- alternatively to the UNLIMITED TABLESPACE privilege you may set quotes on tablespaces defined in DBA_FLASHBACK_ARCHIVE_TS
					GRANT UNLIMITED TABLESPACE TO «conn.metaData.userName»;
				'''
			case BitempResources.get("ERROR_CREATE_TABLE_REQUIRED"):
				'''
					-- to solve "«BitempResources.get("ERROR_CREATE_TABLE_REQUIRED")»" run the following statement as SYS:
					GRANT CREATE TABLE TO «conn.metaData.userName»;
				'''
			case BitempResources.get("ERROR_CREATE_VIEW_REQUIRED"):
				'''
					-- to solve "«BitempResources.get("ERROR_CREATE_VIEW_REQUIRED")»" run the following statement as SYS:
					GRANT CREATE VIEW TO «conn.metaData.userName»;
				'''
			case BitempResources.get("ERROR_FLASHBACK_ARCHIVE_ADMINISTER_REQUIRED"):
				'''
					-- to solve "«BitempResources.get("ERROR_FLASHBACK_ARCHIVE_ADMINISTER_REQUIRED")»" run the following statement as SYS:
					GRANT FLASHBACK ARCHIVE ADMINISTER TO «conn.metaData.userName»;
				'''
			case BitempResources.get("ERROR_DBMS_FLASHBACK_ARCHIVE_REQUIRED"):
				'''
					-- to solve "«BitempResources.get("ERROR_DBMS_FLASHBACK_ARCHIVE_REQUIRED")»" run the following statement as SYS:
					GRANT EXECUTE ON SYS.DBMS_FLASHBACK_ARCHIVE TO «conn.metaData.userName»;
				'''
			case BitempResources.get("ERROR_DBMS_FLASHBACK_REQUIRED"):
				'''
					-- to solve "«BitempResources.get("ERROR_DBMS_FLASHBACK_REQUIRED")»" run the following statement as SYS:
					GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO «conn.metaData.userName»;
				'''
			default:
				'''
					-- no solution found for "«missingPrerequisite»"
				'''
		}
	}
}
