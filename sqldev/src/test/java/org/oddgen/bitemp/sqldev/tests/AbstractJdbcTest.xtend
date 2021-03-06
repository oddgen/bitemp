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
package org.oddgen.bitemp.sqldev.tests

import java.io.StringReader
import java.util.ArrayList
import java.util.Properties
import oracle.dbtools.raptor.newscriptrunner.SQLCommand.StmtType
import oracle.dbtools.worksheet.scriptparser.sqlplus.SQLPlusScriptParser
import org.junit.BeforeClass
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource

class AbstractJdbcTest {
	protected static var SingleConnectionDataSource dataSource
	protected static var JdbcTemplate jdbcTemplate
	protected static var SingleConnectionDataSource sysDataSource
	protected static var JdbcTemplate sysJdbcTemplate
	// static initializer not supported in Xtend, see https://bugs.eclipse.org/bugs/show_bug.cgi?id=429141
	protected static val _staticInitializerForDataSourceAndJdbcTemplate = {
		val p = new Properties()
		p.load(AbstractJdbcTest.getClass().getResourceAsStream(
			"/test.properties"))
		// create dataSource and jdbcTemplate
		dataSource = new SingleConnectionDataSource()
		dataSource.driverClassName = "oracle.jdbc.OracleDriver"
		dataSource.url = '''jdbc:oracle:thin:@«p.getProperty("host")»:«p.getProperty("port")»/«p.getProperty("service")»'''
		dataSource.username = p.getProperty("scott_username")
		dataSource.password = p.getProperty("scott_password")
		jdbcTemplate = new JdbcTemplate(
			dataSource)
		// create dbaDataSource and dbaJdbcTemplate
		sysDataSource = new SingleConnectionDataSource()
		sysDataSource.driverClassName = "oracle.jdbc.OracleDriver"
		sysDataSource.url = '''jdbc:oracle:thin:@«p.getProperty("host")»:«p.getProperty("port")»/«p.getProperty("service")»'''
		sysDataSource.username = p.getProperty("sys_username")
		sysDataSource.password = p.getProperty("sys_password")
		sysJdbcTemplate = new JdbcTemplate(AbstractJdbcTest.sysDataSource)
	}
	
	def static getStatements(String sqlplusScript) {
		var SQLPlusScriptParser p = new SQLPlusScriptParser(new StringReader(sqlplusScript))
		val stmts = new ArrayList<String>
		while (p.hasNext) {
			val stmt = p.next
			if ((stmt.executable || stmt.runnable) && stmt.stmtType != StmtType.G_C_COMMENT) {
				stmts.add(stmt.sql)
			}
		}
		return stmts;
	}
	
	def getObjectStatus(String objectType, String objectName) {
		val status = jdbcTemplate.queryForObject('''
			SELECT status 
			  FROM user_objects
			 WHERE object_type = ?
			   AND object_name = ?
		''', String, #[objectType, objectName])
		return status
	}	
	

	@BeforeClass
	def static void globalSetup() {
		// setup for all test to ensure irrelevance of execution order
		// for table API generation
		sysJdbcTemplate.execute("GRANT CREATE TABLE TO scott")
		sysJdbcTemplate.execute("GRANT CREATE VIEW TO scott")		
		// for FBDA
		sysJdbcTemplate.execute("GRANT EXECUTE ON dbms_flashback_archive TO scott")
		sysJdbcTemplate.execute("GRANT EXECUTE ON dbms_flashback TO scott")
		sysJdbcTemplate.execute("GRANT FLASHBACK ARCHIVE ADMINISTER TO scott")
		sysJdbcTemplate.execute("GRANT UNLIMITED TABLESPACE TO scott")
		// for sys.ku$_fba_period_view
		sysJdbcTemplate.execute("GRANT select_catalog_role TO scott")
		// create FBA if not existing
		sysJdbcTemplate.execute('''
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
	}
	
}
