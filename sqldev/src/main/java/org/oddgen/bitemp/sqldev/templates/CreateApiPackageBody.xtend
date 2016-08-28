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
	
	def getMergeColumnNames(GeneratorModel model) {
		return model.columnNames.filter[
			it != BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase &&
			it != model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase	&&
			it != model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase
		]
	}

	def getDiffColumnNames(GeneratorModel model) {
		return model.columnNames.filter[
			it != BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase
		]
	}

	def getLatestColumnNames(GeneratorModel model) {
		return model.columnNames.filter[
			it != BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase &&
			it != model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase	&&
			it != model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase
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
				«val isDeleted = BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase»
				«val histId = BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase»
				«val operation = BitempRemodeler.OPERATION_COL_NAME.toLowerCase»
				«val groupCols = BitempRemodeler.GROUP_COLS_COL_NAME.toLowerCase»
				«val newGroup = BitempRemodeler.NEW_GROUP_COL_NAME.toLowerCase»
				«val groupNo = BitempRemodeler.GROUP_NO_COL_NAME.toLowerCase»
			   --
			   -- Declarations to handle 'ORA-06508: PL/SQL: could not find program unit being called: "«model.conn.metaData.userName».«model.hookPackageName.toUpperCase»"'
			   --
			   e_hook_body_missing EXCEPTION;
			   PRAGMA exception_init(e_hook_body_missing, -6508);

			   «IF model.targetModel == ApiType.BI_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
			   --
			   -- valid time constants, implicitely truncated to the granularity of «model.params.get(BitempRemodeler.GRANULARITY)»
			   --
			   co_minvalue CONSTANT «model.validTimeDataType» := TO_TIMESTAMP('-4712', 'SYYYY');
			   co_maxvalue CONSTANT «model.validTimeDataType» := TO_TIMESTAMP('9999-12-31 23:59:59.999999999', 'YYYY-MM-DD HH24:MI:SS.FF9');

			   --
			   -- working copy of history rows
			   --
			   g_versions «model.collectionTypeName»;

			   --
			   -- original, unchanged history rows 
			   --
			   g_versions_original «model.collectionTypeName»;

			   --
			   -- print_collection
			   --
			   PROCEDURE print_collection (
			      in_collection IN «model.collectionTypeName»,
			      in_header IN VARCHAR2 DEFAULT NULL
			   )
			   IS
			   BEGIN
			      IF in_header IS NOT NULL THEN
			         dbms_output.put_line(in_header);
			      END IF;
			      FOR i in 1..in_collection.COUNT() LOOP
			         dbms_output.put_line('row ' || i || ':');
			         «FOR col : model.columnNames»
			         	dbms_output.put_line('.. «String.format("%-30s", col)»: ' || in_collection(i).«col»);
			         «ENDFOR»
			      END LOOP;
			   END print_collection;

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
			        BULK COLLECT INTO g_versions_original
			        FROM «model.historyTableName» «
			             »VERSIONS PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE
			       WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_row.«col»«ENDFOR»;
			      g_versions := g_versions_original;
			   END load_versions;

			   --
			   -- get_version_at
			   --
			   FUNCTION get_version_at (
			      in_at IN «model.validTimeDataType»
			   ) RETURN «model.objectTypeName» IS
			      l_version «model.objectTypeName»;
			   BEGIN
			      SELECT version
			        INTO l_version
			        FROM (
			                SELECT «model.objectTypeName» (
			                          «FOR col : model.columnNames SEPARATOR ","»
			                          	«IF col == validTo»
			                          		LEAD («validFrom», 1, valid_to) OVER (ORDER BY «validFrom» NULLS FIRST)
			                          	«ELSE»
			                          		«col»
			                          	«ENDIF»
			                          «ENDFOR»
			                       ) version
			                  FROM TABLE(g_versions)
			             ) v
			       WHERE (v.version.«validFrom» IS NULL OR v.version.«validFrom» <= in_at)
			         AND (v.version.«validTo» IS NULL OR v.version.«validTo» > in_at);
			      RETURN l_version;
			   EXCEPTION
			      WHEN NO_DATA_FOUND THEN
			         RETURN NULL;
			   END get_version_at;

			   --
			   -- changes_history
			   --
			   FUNCTION changes_history RETURN BOOLEAN IS
			      l_diff_count PLS_INTEGER;
			   BEGIN
			      WITH 
			         diff1 AS (
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions)
			             MINUS
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions_original)
			         ),
			         diff2 AS (
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions_original)
			             MINUS
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions)
			         ),
			         diff AS (
			            SELECT COUNT(*) AS count_diff 
			              FROM diff1 
			             WHERE ROWNUM = 1
			            UNION ALL
			            SELECT COUNT(*) AS count_diff
			              FROM diff2
			             WHERE ROWNUM = 1 
			         )
			      SELECT SUM(count_diff)
			        INTO l_diff_count
			        FROM diff
			       WHERE ROWNUM = 1;
			     RETURN l_diff_count > 0;
			   END changes_history;

			   --
			   -- del_enclosed_versions
			   --
			   PROCEDURE del_enclosed_versions (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_versions «model.collectionTypeName»;
			   BEGIN
			      SELECT «model.objectTypeName» (
			             «FOR col : model.columnNames SEPARATOR ","»
			             	«col»
			             «ENDFOR»
			             )
			        BULK COLLECT INTO l_versions
			        FROM TABLE(g_versions)
			       WHERE NOT (
			       			NVL(«validFrom», co_minvalue) >= NVL(in_row.«validFrom», co_minvalue) 
			       			AND NVL(«validTo», co_maxvalue) <= NVL(in_row.«validTo», co_maxvalue)
			       	     );
			       g_versions := l_versions;
			   END del_enclosed_versions;

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
			   -- add_first_version
			   --
			   PROCEDURE add_first_version IS
			      l_version «model.objectTypeName»;
			   BEGIN
			      l_version := get_version_at(co_minvalue);
			      IF l_version IS NULL THEN
			         SELECT «model.objectTypeName» (
			                   «FOR col : model.columnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			                ) version
			           INTO l_version
			           FROM TABLE(g_versions)
			          ORDER BY «validFrom» NULLS FIRST
			          FETCH FIRST ROW ONLY;
			         l_version.«validFrom» := NULL;
			         l_version.«isDeleted» := 1;
			         add_version(in_row => l_version);
			      END IF;
			   END add_first_version;

			   --
			   -- add_last_version
			   --
			   PROCEDURE add_last_version IS
			      l_version «model.objectTypeName»;
			   BEGIN
			      l_version := get_version_at(co_maxvalue);
			      IF l_version IS NULL THEN
			         SELECT «model.objectTypeName» (
			                   «FOR col : model.columnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			                ) version
			           INTO l_version
			           FROM TABLE(g_versions)
			          ORDER BY «validFrom» DESC NULLS LAST
			          FETCH FIRST ROW ONLY;
			         l_version.«validFrom» := l_version.«validTo»;
			         l_version.«validTo» := NULL;
			         l_version.«isDeleted» := 1;
			         add_version(in_row => l_version);
			      END IF;
			   END add_last_version;

			   --
			   -- merge_versions
			   --
			   PROCEDURE merge_versions IS
			      l_merged «model.collectionTypeName»;
			   BEGIN
			      WITH
			         base AS (
			            SELECT «histId»,
			                   NVL(«validFrom», co_minvalue) AS «validFrom»,
			                   NVL(LEAD («validFrom», 1, valid_to) OVER (ORDER BY «validFrom» NULLS FIRST), co_maxvalue) AS «validTo»,
			                   (
			                      «FOR col : model.mergeColumnNames SEPARATOR " || ',' || "»
			                      	«col»
			                      «ENDFOR»
			                   ) AS «groupCols»,
			                   «FOR col : model.mergeColumnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			              FROM TABLE(g_versions)
			         ),
			         group_no_base AS (
			            SELECT «histId»,
			                   «validFrom»,
			                   «validTo»,
			                   CASE
			                      WHEN LAG(«groupCols», 1, «groupCols») OVER (ORDER BY «validFrom») = «groupCols» THEN
			                         0
			                      ELSE
			                         1
			                   END AS «newGroup»,
			                   «FOR col : model.mergeColumnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			              FROM base
			         ),
			         group_no AS (
			            SELECT «histId»,
			                   «validFrom»,
			                   «validTo»,
			                   SUM(«newGroup») OVER (ORDER BY «validFrom») AS «groupNo»,
			                   «FOR col : model.mergeColumnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			              FROM group_no_base
			         ),
			         merged AS (
			            SELECT MAX(«histId») AS «histId»,
			                   MIN(«validFrom») AS «validFrom»,
			                   MAX(«validTo») AS «validTo»,
			                   «FOR col : model.mergeColumnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			              FROM group_no
			             GROUP BY «groupNo»,
			                      «FOR col : model.mergeColumnNames SEPARATOR ","»
			                      	«col»
			                      «ENDFOR»
			         )
			      -- main
			      SELECT «model.objectTypeName» (
			                «histId»,
			                CASE 
			                   WHEN «validFrom» = co_minvalue THEN
			                      NULL
			                   ELSE
			                      «validFrom»
			                END,
			                CASE
			                   WHEN «validTo» = co_maxvalue THEN
			                      NULL
			                   ELSE
			                      «validTo»
			                END,
			                «FOR col : model.mergeColumnNames SEPARATOR ","»
			                	«col»
			                «ENDFOR»
			             )
			        BULK COLLECT INTO l_merged
			        FROM merged;
			       g_versions := l_merged;
			   END merge_versions;

			   --
			   -- save_latest
			   --
			   PROCEDURE save_latest IS
			      l_latest_row «model.objectTypeName»;
			      i PLS_INTEGER;
			   BEGIN
			      l_latest_row := get_version_at(co_maxvalue);
			      IF g_versions_original.COUNT() = 0 THEN
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
			          WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = l_latest_row.«col»«ENDFOR»
			            AND (
			                    «FOR col : model.updateableLatestColumnNames SEPARATOR " OR"»
			                    	(«col» != l_latest_row.«col» OR «col» IS NULL AND l_latest_row.«col» IS NOT NULL OR «col» IS NOT NULL AND l_latest_row.«col» IS NULL)
			                    «ENDFOR»
			                );
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
			               SELECT NULL AS «operation»,
			                      «FOR col : model.columnNames SEPARATOR ","»
			                      	«IF col == validTo»
			                      		LEAD («validFrom», 1, NULL) OVER (ORDER BY «validFrom» NULLS FIRST) AS «validTo»
			                      	«ELSE»
			                      		«col»
			                      	«ENDIF»
			                      «ENDFOR»
			                 FROM TABLE(g_versions)
			               UNION ALL
			               SELECT 'D' AS «operation»,
			                      «FOR col : model.columnNames SEPARATOR ","»
			                      	o.«col»
			                      «ENDFOR»
			                 FROM TABLE(g_versions_original) o
			                 LEFT JOIN TABLE(g_versions) w
			                   ON w.«histId» = o.«histId»
			                WHERE w.«histId» IS NULL
			            ) s
			         ON (s.«histId» = t.«histId»)
			       WHEN MATCHED THEN
			               UPDATE
			                  SET «FOR col : model.updateableColumnNames SEPARATOR ',' + System.lineSeparator + '    '»t.«col» = s.«col»«ENDFOR»
			               DELETE
			                WHERE «operation» = 'D'
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
			      del_enclosed_versions(in_row => io_row);
			      add_version(in_row => io_row);
			      add_first_version;
			      add_last_version;
			      merge_versions;
			      IF changes_history() THEN
			         save_latest;
			         save_versions;
			      END IF;
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
