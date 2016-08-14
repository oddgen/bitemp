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

import com.jcabi.aspects.Loggable
import java.sql.Connection
import java.util.ArrayList
import java.util.LinkedHashMap
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.Column
import org.oddgen.bitemp.sqldev.model.generator.FlashbackArchiveTable
import org.oddgen.bitemp.sqldev.model.generator.ForeignKeyConstraint
import org.oddgen.bitemp.sqldev.model.generator.PrimaryKeyConstraint
import org.oddgen.bitemp.sqldev.model.generator.Table
import org.oddgen.bitemp.sqldev.model.generator.TemporalValidityPeriod
import org.oddgen.sqldev.LoggableConstants
import org.springframework.jdbc.core.BeanPropertyRowMapper
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource

@Loggable(LoggableConstants.DEBUG)
class TableDao {
	private Connection conn
	private JdbcTemplate jdbcTemplate

	new(Connection conn) {
		this.conn = conn
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(conn, true))
	}

	def getTable(String tableName) {
		return getTable(tableName, true)
	}

	def Table getTable(String tableName, boolean deep) {
		val table = new Table
		table.tableName = tableName
		table.columns = tableName.columns
		table.primaryKeyConstraint = getPrimaryKeyConstraint(tableName, deep)
		table.foreignKeyConstraints = getForeignKeyConstraints(tableName, deep)
		table.historyTable = tableName.historyTable
		table.flashbackArchiveTable = tableName.archiveTable
		table.temporalValidityPeriods = tableName.temporalValidityPeriods
		return table
	}
	
	def getColumns (String tableName) {
		val sql = '''
			SELECT column_name,
			       data_type,
			       data_precision,
			       data_scale,
			       char_length,
			       char_used,
			       nullable,
			       data_default,
			       default_on_null,
			       hidden_column,
			       virtual_column
			  FROM user_tab_cols
			 WHERE table_name = ?
			 ORDER BY internal_column_id
		'''
		val result = jdbcTemplate.query(sql, new BeanPropertyRowMapper<Column>(Column),
			#[tableName])
		val columns = new LinkedHashMap<String, Column>
		for (col : result) {
			columns.put(col.columnName, col)
		}
		return columns
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

	def getTemporalValidityPeriods(String tableName) {
		// requires SELECT_CATALOG_ROLE
		val sql = '''
			SELECT p.periodname,
			       p.periodstart,
			       p.periodend,
			       p.flags
			  FROM sys.ku$_fba_period_view p
			 WHERE p.obj_num IN (SELECT o.object_id
			                       FROM user_objects o
			                      WHERE o.object_type = 'TABLE'
			                        AND o.object_name = ?)
		'''
		val result = jdbcTemplate.query(sql, new BeanPropertyRowMapper<TemporalValidityPeriod>(TemporalValidityPeriod),
			#[tableName])
		return result
	}

	def getPrimaryKeyConstraint(String tableName, boolean deep) {
		val sql = '''
			SELECT constraint_name
			  FROM user_constraints
			 WHERE table_name = ?
			       AND constraint_type = 'P'
		'''
		val result = jdbcTemplate.query(sql, new BeanPropertyRowMapper<PrimaryKeyConstraint>(PrimaryKeyConstraint),
			#[tableName])
		if (result.size == 0) {
			return null
		} else {
			for (pk : result) {
				pk.columnNames = getConstraintColumns(tableName, pk.constraintName)
				if (deep) {
					pk.referencingTables = tableName.referencingTables
				}
			}
			return result.get(0)
		}
	}

	def getReferencingTables(String tableName) {
		val sql = '''
			WITH 
			   cons AS (
			      SELECT constraint_name, constraint_type, table_name, r_constraint_name
			        FROM all_constraints
			       WHERE owner = USER
			   )
			SELECT fk.table_name
			  FROM cons fk
			  JOIN cons pk
			    ON pk.constraint_name = fk.r_constraint_name
			       AND pk.constraint_type = 'P'
			 WHERE pk.table_name = ? 
			       AND  fk.constraint_type = 'R'
		'''
		val result = jdbcTemplate.queryForList(sql, String, #[tableName])
		val finalResult = new ArrayList<Table>()
		for (t : result) {
			finalResult.add(getTable(t, false))
		}
		return finalResult

	}

	def getForeignKeyConstraints(String tableName, boolean deep) {
		val sql = '''
			WITH 
			   cons AS (
			      SELECT constraint_name, constraint_type, table_name, r_constraint_name, status
			        FROM all_constraints
			       WHERE owner = USER
			   )
			SELECT fk.constraint_name AS constraint_name,
			       fk.status          AS status,
			       pk.constraint_name AS referenced_constraint_name,
			       pk.table_name      AS referenced_table_name
			  FROM cons fk
			  JOIN cons pk
			    ON pk.constraint_name = fk.r_constraint_name
			       AND pk.constraint_type = 'P'
			 WHERE fk.table_name = ?
			   AND fk.constraint_type = 'R'
		'''
		val result = jdbcTemplate.query(sql, new BeanPropertyRowMapper<ForeignKeyConstraint>(ForeignKeyConstraint),
			#[tableName])
		for (fk : result) {
			fk.columnNames = getConstraintColumns(tableName, fk.constraintName)
			if (deep) {
				fk.referencedTable = getTable(fk.referencedTableName, false)
			}
		}
		return result
	}

	def getConstraintColumns(String tableName, String constraintName) {
		val sql = '''
			SELECT column_name
			  FROM user_cons_columns
			 WHERE table_name = ?
			       AND constraint_name = ?
			 ORDER BY column_name
		'''
		val result = jdbcTemplate.queryForList(sql, String, #[tableName, constraintName])
		return result
	}

	def isHistoryTable(String tableName) {
		// faster than querying constraint and cons_columns, good enough, BitempRemodeler.HIST_ID_COL_NAME must not be used for other purposes
		val sql = '''
			SELECT COUNT(*) AS found
			  FROM user_tab_columns
			 WHERE table_name = ?
			       AND column_name = '«BitempRemodeler.HISTORY_ID_COL_NAME»'
			       AND identity_column = 'YES'
		'''
		val result = jdbcTemplate.queryForObject(sql, Integer, #[tableName])
		return result == 1
	}

}
