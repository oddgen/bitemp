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
package org.oddgen.bitemp.sqldev.dal

import java.sql.Connection
import org.oddgen.bitemp.sqldev.model.FlashbackArchiveTable
import org.springframework.jdbc.core.BeanPropertyRowMapper
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource

class FlashbackArchiveTableDao {
	private Connection conn
	private JdbcTemplate jdbcTemplate

	new(Connection conn) {
		this.conn = conn
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(conn, true))
	}

	def FlashbackArchiveTable getArchiveTable(String tableName) {
		val sql = '''
			SELECT flashback_archive_name,
			       archive_table_name,
			       status
			  FROM user_flashback_archive_tables
			 WHERE table_name = ?
		'''
		val result = jdbcTemplate.query(sql, new BeanPropertyRowMapper<FlashbackArchiveTable>(FlashbackArchiveTable),
			#[tableName])
		return if(result.size == 0) null else result.get(0)
	}
}
