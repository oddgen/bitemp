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
import java.util.ArrayList
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.bitemp.sqldev.model.generator.Table
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class AddFlashbackArchive {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools
	
	def getDisableConstraintStatments(GeneratorModel model) {
		val result = new ArrayList<String>
		val latestTableName = getNewLatestTableName(model.inputTable, model)
		for (table : model.inputTable.primaryKeyConstraint.referencingTables) {
			for (fk : table.foreignKeyConstraints.
				filter[it.status == "ENABLED" && it.referencedTableName == latestTableName]) {
				val sql = '''
					«IF table.flashbackArchiveTable != null»
						BEGIN
						   -- «fk.referencedTableName»
						   dbms_flashback_archive.disassociate_fba(
						      owner_name => USER, 
						      table_name => '«table.tableName.toUpperCase»'
						   );
						END;
						/
					«ENDIF»
					ALTER TABLE «getNewLatestTableName(table, model)» DISABLE CONSTRAINT «fk.constraintName»;
				'''
				result.add(sql)
			}
		}
		return result
	}	

	def getEnableConstraintsStatments(GeneratorModel model) {
		val result = new ArrayList<String>
		val latestTableName = getNewLatestTableName(model.inputTable, model)
		for (table : model.inputTable.primaryKeyConstraint.referencingTables) {
			for (fk : table.foreignKeyConstraints.
				filter[it.status == "ENABLED" && it.referencedTableName == latestTableName]) {
				val sql = '''
					ALTER TABLE «getNewLatestTableName(table, model)» ENABLE CONSTRAINT «fk.constraintName»;
					«IF table.flashbackArchiveTable != null»
						BEGIN
						   -- «fk.referencedTableName»
						   dbms_flashback_archive.reassociate_fba(
						      owner_name => USER, 
						      table_name => '«table.tableName.toUpperCase»'
						   );
						END;
						/
					«ENDIF»
				'''
				result.add(sql)
			}
		}
		return result
	}

	def compile(Table table,
		GeneratorModel model) '''
		«IF table.exists»
			«IF table.flashbackArchiveTable == null»
				«val newTableName = getNewLatestTableName(table, model)»
				«val columns = model.inputTable.columns.values.filter[!it.isTemporalValidityColumn(model) && 
					it.columnName != model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toUpperCase && it.virtualColumn == "NO"
				]»
				«IF model.originModel == ApiType.BI_TEMPORAL && model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME && false»
					«val disableStatements = model.disableConstraintStatments»
					«IF disableStatements.size > 0»
						-- 
						-- Disable enabled referring foreign key constraints
						--
						«FOR stmt : disableStatements»
							«stmt»
						«ENDFOR»
					«ENDIF»
					--
					-- Empty table
					--
					TRUNCATE TABLE «newTableName»;
				«ENDIF»
				--
				-- Add flashback archive
				--
				ALTER TABLE «newTableName.toLowerCase» FLASHBACK ARCHIVE «model.params.get(BitempRemodeler.FLASHBACK_ARCHIVE_NAME).toLowerCase»;
				«IF model.originModel == ApiType.BI_TEMPORAL && model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME && false»
					--
					-- Populate latest data from history table with enabled tracking
					--
					INSERT INTO «newTableName» ( «/*TODO: support identity columns */»
					          «FOR col : columns SEPARATOR ","»
					          	«col.columnName.toLowerCase»
					          «ENDFOR»
					«'       '»)
					SELECT «FOR col : columns SEPARATOR ',' + System.lineSeparator + '       '»«
					       	»«col.columnName.toLowerCase»«
					       »«ENDFOR»
					  FROM «newTableName»$
					WHERE NOT («model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toUpperCase» = 1)
					  AND «model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toUpperCase» IS NULL;
					COMMIT;
					«val enableStatements = model.enableConstraintsStatments»
					«IF enableStatements.size > 0»
						-- 
						-- Enable previously disabled foreign key constraints
						--
						«FOR stmt : enableStatements»
							«stmt»
						«ENDFOR»
					«ENDIF»
				«ENDIF»
			«ENDIF»
		«ENDIF»
	'''
}
