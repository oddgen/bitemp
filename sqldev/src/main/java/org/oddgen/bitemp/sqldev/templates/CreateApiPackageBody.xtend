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
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateApiPackageBody {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools
	
	def getColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		if (model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add(BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase)
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase)
			cols.add(model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase)
			cols.add(BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase)
		}
		for (col : model.inputTable.columns.values.filter [
			it.virtualColumn == "NO" && !cols.contains(it.columnName) &&
				it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase &&
				!(it.identityColumn == "YES" && it.generationType == "ALWAYS")
		]) {
			cols.add(col.columnName.toLowerCase)
		}
		return cols
	}
	
	def getPkColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		for (col : model.inputTable.primaryKeyConstraint.columnNames) {
			cols.add(col.toLowerCase)
		}
		return cols
	}
	
	def getUpdateableColumnNames(GeneratorModel model) {
		return model.columnNames.filter[
			it != BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase
		]
	}

	def getLatestColumnNames(GeneratorModel model) {
		return model.columnNames.filter[
			it != BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase &&
			it != model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase	&&
			it != model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase &&
			it != BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase
		]
	}
	
	def getUpdateableLatestColumnNames(GeneratorModel model) {
		return model.latestColumnNames.filter[
			!model.inputTable.primaryKeyConstraint.columnNames.contains(it.toUpperCase)
		]
	}

	
	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			--
			-- Create API package body
			--
			CREATE OR REPLACE PACKAGE BODY «model.apiPackageName» AS
				«val validFrom = model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase»
				«val validTo = model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase»
			   --
			   -- Declarations to handle 'ORA-06508: PL/SQL: could not find program unit being called: "«model.conn.metaData.userName».«model.hookPackageName.toUpperCase»"'
			   --
			   e_hook_body_missing EXCEPTION;
			   PRAGMA exception_init(e_hook_body_missing, -6508);

			   «IF model.targetModel == ApiType.BI_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
			   --
			   -- history rows
			   --
			   g_versions «model.collectionTypeName»;
			   
			   --
			   -- number of history rows originally found
			   --
			   g_versions_count PLS_INTEGER;
			   
			   --
			   -- history rows to be physically deleted
			   --
			   g_versions_del «model.collectionTypeName» := «model.collectionTypeName»();
			   
			   --
			   -- truncate_to_granularity
			   --
			   PROCEDURE truncate_to_granularity (
			      io_row IN OUT «model.objectTypeName»
			   ) IS
			   BEGIN
			      «IF model.granularityRequiresTruncation»
			      	-- truncate validity to «model.params.get(BitempRemodeler.GRANULARITY)»
			      	io_row.«validFrom» := TRUNC(io_row.«validFrom», '«model.granuarityTruncationFormat»');
			      	io_row.«validTo» := TRUNC(io_row.«validTo», '«model.granuarityTruncationFormat»');
			      «ELSE»
			      	-- truncated automatically to «model.params.get(BitempRemodeler.GRANULARITY)» by data type precision
			      	NULL;
			      «ENDIF»
			   END truncate_to_granularity;

			   --
			   -- load_versions
			   --
			   PROCEDURE load_versions (
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      SELECT «model.objectTypeName» (
			                «FOR col : model.columnNames SEPARATOR ','»
			                	«col»
			                «ENDFOR»
			             )
			        BULK COLLECT INTO g_versions
			        FROM «model.historyTableName» AS OF SCN SYS.dbms_flashback.get_system_change_number «
			             »VERSIONS PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE
			       WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_row.«col»«ENDFOR»;
			      g_versions_count := g_versions.count();
			      g_versions_del.delete();
			   END load_versions;

			   --
			   -- handle_predecessor
			   --
			   PROCEDURE handle_predecessor (
			      in_row IN «model.objectTypeName»
			   ) IS
			      i PLS_INTEGER;
			   BEGIN
			      IF g_versions IS NOT NULL AND g_versions.count() > 0 THEN
			         IF in_row.«validFrom» IS NOT NULL THEN
			            -- reduce validity of immediate predecessor
			            i := g_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF (g_versions(i).«validFrom» IS NULL OR g_versions(i).«validFrom» <= in_row.«validFrom»)
			                  AND (g_versions(i).«validTo» IS NULL OR g_versions(i).«validTo» > in_row.«validFrom»)
			               THEN
			                  g_versions(i).«validTo» := in_row.«validFrom»;
			               END IF;
			               i := g_versions.next(i);
			            END LOOP;
			         ELSE
			            -- delete all predecessors
			            i := g_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF g_versions(i).«validTo» < in_row.«validTo» OR in_row.«validTo» IS NULL THEN
			                  g_versions_del.extend();
			                  g_versions_del(g_versions_del.last()) := g_versions(i);
			                  g_versions.delete(i);
			               END IF;
			               i := g_versions.next(i);
			            END LOOP;
			         END IF;
			      END IF;
			   END handle_predecessor;

			   --
			   -- handle_successor
			   --
			   PROCEDURE handle_successor (
			      in_row IN «model.objectTypeName»
			   ) IS
			      i PLS_INTEGER;
			   BEGIN
			      IF g_versions IS NOT NULL AND g_versions.count() > 0 THEN
			         IF in_row.«validTo» IS NOT NULL THEN
			            -- reduce validity of immediate successor
			            i := g_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF (g_versions(i).«validFrom» IS NULL OR g_versions(i).«validFrom» <= in_row.«validTo»)
			                  AND (g_versions(i).«validTo» IS NULL OR g_versions(i).«validTo» > in_row.«validTo»)
			               THEN
			                  g_versions(i).«validFrom» := in_row.«validTo»;
			               END IF;
			               i := g_versions.next(i);
			            END LOOP;
			         ELSE
			            -- delete all successors
			            i := g_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF g_versions(i).«validFrom» > in_row.«validFrom» OR in_row.«validFrom» IS NULL THEN
			                  g_versions_del.extend();
			                  g_versions_del(g_versions_del.last()) := g_versions(i);
			                  g_versions.delete(i);
			               END IF;
			               i := g_versions.next(i);
			            END LOOP;
			         END IF;
			      END IF;
			   END handle_successor;

			   --
			   -- add_version
			   --
			   PROCEDURE add_version (
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      g_versions.extend();
			      g_versions(g_versions.last()) := in_row;
			   END add_version;

			   --
			   -- save_latest
			   --
			   PROCEDURE save_latest IS
			      l_latest_row «model.objectTypeName»;
			      i PLS_INTEGER;
			   BEGIN
			   	  SELECT «model.objectTypeName» (
			                «FOR col : model.columnNames SEPARATOR ','»
			                	«col»
			                «ENDFOR»
			             )
			        INTO l_latest_row
			        FROM TABLE(g_versions)
			       WHERE «validTo» IS NULL;
			      IF g_versions_count = 0 THEN
			            INSERT
			              INTO «model.latestTableName» (
			                      «FOR col : model.latestColumnNames SEPARATOR ','»
			                      	«col»
			                   «ENDFOR»
			                   )
			            VALUES (
			                      «FOR col : model.latestColumnNames SEPARATOR ','»
			                      	l_latest_row.«col»
			                      «ENDFOR»
			                   )
			         RETURNING «FOR col : model.pkColumnNames SEPARATOR ', '»«col»«ENDFOR»
			              INTO «FOR col : model.pkColumnNames SEPARATOR ', '»l_latest_row.«col»«ENDFOR»;
			         i := g_versions.first();
			         WHILE i IS NOT NULL LOOP
			            «FOR col : model.pkColumnNames»
			            	g_versions(i).«col» := l_latest_row.«col»;
			            «ENDFOR»
			            i := g_versions.next(i);
			         END LOOP;
			      ELSE
			         UPDATE «model.latestTableName»
			            SET «FOR col : model.updateableLatestColumnNames SEPARATOR ',' + System.lineSeparator + '    '»«col» = l_latest_row.«col»«ENDFOR»
			          WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = l_latest_row.«col»«ENDFOR»;
			      END IF;
			   END save_latest;

			   --
			   -- save_versions
			   --
			   PROCEDURE save_versions IS
			   BEGIN
			      MERGE 
			       INTO «model.historyTableName» t
			      USING (
			               SELECT NULL AS operation$,
			                      «FOR col : model.columnNames SEPARATOR ","»
			                      	«col»
			                      «ENDFOR»
			                 FROM TABLE(g_versions)
			               UNION ALL
			               SELECT 'D' AS operation$,
			                      «FOR col : model.columnNames SEPARATOR ","»
			                      	«col»
			                      «ENDFOR»
			                 FROM TABLE(g_versions_del)
			            ) s
			         ON (s.«BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase» = t.«BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase»)
			       WHEN MATCHED THEN
			               UPDATE
			                  SET «FOR col : model.updateableColumnNames SEPARATOR ',' + System.lineSeparator + '    '»t.«col» = s.«col»«ENDFOR»
			                WHERE operation$ IS NULL
			               DELETE
			                WHERE operation$ = 'D'
			       WHEN NOT MATCHED THEN
			               INSERT (
			                         «FOR col : model.updateableColumnNames SEPARATOR ","»
			                         	t.«col»
			                         «ENDFOR»
			                      )
			               VALUES (
			                         «FOR col : model.updateableColumnNames SEPARATOR ","»
			                         	s.«col»
			                         «ENDFOR»
			                      );
			   END save_versions;

			   --
			   -- do_ins
			   --
			   PROCEDURE do_ins (
			      io_row IN OUT «model.objectTypeName»
			   ) IS
			   BEGIN
			      truncate_to_granularity(io_row => io_row);
			      load_versions(in_row => io_row);
			      handle_predecessor(in_row => io_row);
			      handle_successor(in_row => io_row);
			      add_version(in_row => io_row);
			      save_latest;
			      save_versions;
			   END do_ins;
			   «ENDIF»
			   --
			   -- ins
			   --
			   PROCEDURE ins (
			      in_new_row IN «model.objectTypeName»
			   ) IS
			      l_new_row «model.objectTypeName»;
			   BEGIN
			      l_new_row := in_new_row;
			      <<trap_pre_ins>>
			      BEGIN
			         «model.hookPackageName».pre_ins(io_new_row => l_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_ins;
			      «IF model.targetModel == ApiType.NON_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
			      	INSERT INTO «model.latestTableName» (
			      	   «FOR col : model.columnNames SEPARATOR ","»
			      	   	«col.toLowerCase»
			      	   «ENDFOR»
			      	) VALUES (
			      	   «FOR col : model.columnNames SEPARATOR ","»
			      	   	l_new_row.«col.toLowerCase»
			      	   «ENDFOR»
			      	);
			      «ELSE»
			      	do_ins(io_row => l_new_row);
			      «ENDIF»
			      <<trap_post_ins>>
			      BEGIN
			         «model.hookPackageName».post_ins(in_new_row => l_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_post_ins;
			   END ins;

			   --
			   -- upd
			   --
			   PROCEDURE upd (
			      in_new_row IN «model.objectTypeName»,
			      in_old_row IN «model.objectTypeName»
			   ) IS
			      l_new_row «model.objectTypeName»;
			   BEGIN
			      l_new_row := in_new_row;
			      <<trap_pre_upd>>
			      BEGIN
			         «model.hookPackageName».pre_upd(
			            io_new_row => l_new_row,
			            in_old_row => in_new_row
			         );
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_upd;
			      «IF model.targetModel == ApiType.NON_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
			      	UPDATE «model.latestTableName»
			      	   SET «FOR col : model.columnNames SEPARATOR ', ' + System.lineSeparator + '    '»«col» = l_new_row.«col»«ENDFOR»
			      	 WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_old_row.«col»«ENDFOR»;
			      «ELSE»
			      	-- TODO temporal update
			      «ENDIF»
			      <<trap_post_upd>>
			      BEGIN
			         «model.hookPackageName».post_upd(
			            in_new_row => l_new_row,
			            in_old_row => in_old_row
			         );
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_post_upd;
			   END upd;

			   --
			   -- del
			   --
			   PROCEDURE del (
			      in_old_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      <<trap_pre_del>>
			      BEGIN
			         «model.hookPackageName».pre_del(in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_del;
			      «IF model.targetModel == ApiType.NON_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
			      	DELETE 
			      	  FROM «model.latestTableName»
			      	 WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '   AND '»«col» = in_old_row.«col»«ENDFOR»;
			      «ELSE»
			      	-- TODO temporal delete
			      «ENDIF»
			      <<trap_post_del>>
			      BEGIN
			         «model.hookPackageName».post_del(in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_post_del;
			   END del;

			END «model.apiPackageName»;
			/
		«ENDIF»
	'''
}
