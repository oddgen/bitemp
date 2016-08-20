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
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.ApiType
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class PopulateFlashbackArchive {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			«IF (model.originModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME || model.originModel == ApiType.BI_TEMPORAL) 
				&& (model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME || model.targetModel == ApiType.BI_TEMPORAL)
				&& model.originModel != model.targetModel»
				«val fromTableName = 
					if (model.targetModel == ApiType.BI_TEMPORAL) 
				 		getNewTableName(model.inputTable, model).toLowerCase 
				 	else 
				 		getNewTableName(model.newHistTable, model).toLowerCase»
				«val toTableName = 
					if (model.targetModel == ApiType.BI_TEMPORAL)
						getNewTableName(model.newHistTable, model).toLowerCase
					else
						getNewTableName(model.inputTable, model).toLowerCase»
				«val columns = model.inputTable.columns.values.filter[!it.isTemporalValidityColumn(model) && 
					it.columnName != model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toUpperCase && it.virtualColumn == "NO"
				]»
				«IF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
					--
					-- Delete rows marked as deleted and enforce update in SYS_FBA_TCRV_...
					--
					DELETE FROM «fromTableName»
					 WHERE «model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» = 1;
					DELETE FROM «toTableName»
					 WHERE «model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» = 1;
					--
					-- Enforce update for remaining rows in SYS_FBA_TCRV_...
					--
					UPDATE «toTableName»
					   SET «columns.findFirst[it.identityColumn == "NO"].columnName.toLowerCase» = «columns.get(0).columnName.toLowerCase»;
					--
					-- Commit pending changes
					--
					COMMIT;
				«ENDIF»
				--
				-- Enforce visibility of source flashback archive tables
				--
				BEGIN
				   dbms_flashback_archive.disassociate_fba(
				      owner_name => '«model.conn.metaData.userName»',
				      table_name => '«fromTableName.toUpperCase»'
				   );
				   dbms_flashback_archive.reassociate_fba(
				       owner_name => '«model.conn.metaData.userName»',
				       table_name => '«fromTableName.toUpperCase»'
				   );
				END;
				/
				--
				-- Enable flashback archive table for DML operations
				--
				BEGIN
				   dbms_flashback_archive.disassociate_fba(
				      owner_name => '«model.conn.metaData.userName»', 
				      table_name => '«toTableName.toUpperCase»'
				   );
				END;
				/
				--
				-- Create temporary views on SYS_FBA tables (source$hist, target$hist, ...)
				--
				DECLARE
				   PROCEDURE create_view(
				      in_view_name  IN VARCHAR2,
				      in_table_name IN VARCHAR2
				   ) IS
				   BEGIN
				      EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || in_view_name ||
				                        ' AS SELECT * FROM ' || in_table_name;
				   END create_view;
				   PROCEDURE create_fba_views(
				      in_table_name  IN VARCHAR2,
				      in_view_prefix IN VARCHAR2
				   ) IS
				      l_object_id INTEGER;
				   BEGIN
				      SELECT object_id
				        INTO l_object_id
				        FROM user_objects
				       WHERE object_name = in_table_name;
				      create_view(
				         in_view_name  => in_view_prefix ||'DDL_COLMAP',
				         in_table_name => 'SYS_FBA_DDL_COLMAP_' || l_object_id
				      );
				      create_view(
				         in_view_name  => in_view_prefix || 'HIST',
				         in_table_name => 'SYS_FBA_HIST_' || l_object_id
				      );
				      create_view(
				         in_view_name  => in_view_prefix || 'TCRV',
				         in_table_name => 'SYS_FBA_TCRV_' || l_object_id
				      );
				   END create_fba_views;
				BEGIN
				   create_fba_views(
				      in_table_name => '«fromTableName.toUpperCase»',
				      in_view_prefix => 'SOURCE$'
				   );
				   create_fba_views(
				      in_table_name => '«toTableName.toUpperCase»',
				      in_view_prefix => 'TARGET$'
				   );
				END;
				/
				«IF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
					--
					-- Update history to avoid duplicate results
					--
					UPDATE target$hist
					   SET startscn = endscn
					 WHERE startscn IS NULL;
				«ENDIF»				
				--
				-- Populate flashback archive (part 1)
				--
				INSERT INTO target$hist (
				          rid,
				          startscn,
				          endscn,
				          xid,
				          operation,
				          «FOR col : columns SEPARATOR ","»
				          	«col.columnName.toLowerCase»
				          «ENDFOR»
				«'       '»)
				-- outdated rows in history table
				SELECT rid,
				       startscn,
				       endscn,
				       xid,
				       operation,
				       «FOR col : columns SEPARATOR ","»
				       	«col.columnName.toLowerCase»
				       «ENDFOR»
				  FROM source$hist
				  «IF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
				  	WHERE («model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» IS NULL OR «model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» = 0)
				  «ENDIF»
				-- updated rows in actual/latest table (workaround for missing DML capabilty on TCRV table in 12.1.0.2)
				UNION ALL
				SELECT stcrv.rid,
				       stcrv.startscn,
				       ttcrv.startscn AS endscn,
				       stcrv.xid,
				       stcrv.op AS operation,
				       «FOR col : columns SEPARATOR ","»
				       	t.«col.columnName.toLowerCase»
				       «ENDFOR»
				FROM «fromTableName» s
				JOIN source$tcrv stcrv
				  ON stcrv.rid = s.rowid
				JOIN «toTableName» t
				  ON «FOR col : model.inputTable.primaryKeyConstraint.columnNames SEPARATOR " AND "»t.«col» = s.«col»«ENDFOR»
				JOIN target$tcrv ttcrv
				  ON ttcrv.rid = t.rowid
				WHERE stcrv.endscn IS NULL
				  AND ttcrv.endscn IS NULL«
				  »«IF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
				  	AND (s.«model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» IS NULL OR s.«model.params.get(BitempRemodeler.IS_DELETED_COL_NAME).toLowerCase» = 0)«
				  »«ENDIF»;
				--
				-- Populate flashback archive (part 2)
				--
				INSERT INTO target$hist (
				          rid,
				          startscn,
				          endscn,
				          xid,
				          operation,
				          «FOR col : columns SEPARATOR ","»
				          	«col.columnName.toLowerCase»
				          «ENDFOR»
				«'       '»)
				-- rows in actual/lastest table without history (workaround for missing DML capabilty on TCRV table in 12.1.0.2)
				SELECT ttcrv.rid,
				       NULL AS startscn,
				       ttcrv.startscn AS endscn,
				       NULL AS xid,
				       NULL AS operation,
				       «FOR col : columns SEPARATOR ","»
				       	t.«col.columnName.toLowerCase»
				       «ENDFOR»
				FROM «toTableName» t
				JOIN target$tcrv ttcrv
				  ON ttcrv.rid = t.rowid
				LEFT JOIN target$hist thist
				  ON «FOR col : model.inputTable.primaryKeyConstraint.columnNames SEPARATOR " AND "»thist.«col» = t.«col»«ENDFOR»
				WHERE thist.rid IS NULL;
				--
				-- Commit all changes on archive table
				--
				COMMIT;
				--
				-- Disable flashback archive table for DML operations
				--
				BEGIN
				   dbms_flashback_archive.reassociate_fba(
				      owner_name => '«model.conn.metaData.userName»', 
				      table_name => '«toTableName.toUpperCase»'
				   );
				END;
				/				
				--
				-- Drop temporary views on SYS_FBA tables
				--
				DROP VIEW source$ddl_colmap;
				DROP VIEW source$hist;
				DROP VIEW source$tcrv;
				DROP VIEW target$ddl_colmap;
				DROP VIEW target$hist;
				DROP VIEW target$tcrv;
			«ENDIF»
		«ENDIF»
	'''
}
