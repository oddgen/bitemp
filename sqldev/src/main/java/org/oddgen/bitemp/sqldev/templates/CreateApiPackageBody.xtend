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
import org.oddgen.bitemp.sqldev.resources.BitempResources
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
			   «IF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_YEAR")»
			   	co_granule CONSTANT INTERVAL YEAR TO MONTH := INTERVAL '1' YEAR;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MONTH")»
			   	co_granule CONSTANT INTERVAL YEAR TO MONTH := INTERVAL '1' MONTH;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_WEEK")»
			   	co_granule CONSTANT INTERVAL DAY(1) TO SECOND(0) := INTERVAL '7' DAY;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_DAY")»
			   	co_granule CONSTANT INTERVAL DAY(1) TO SECOND(0) := INTERVAL '1' DAY;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_SECOND")»
			   	co_granule CONSTANT INTERVAL DAY(0) TO SECOND(0) := INTERVAL '1' SECOND;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_CENTISECOND")»
			   	co_granule CONSTANT INTERVAL DAY(0) TO SECOND(2) := INTERVAL '1' SECOND / 1E2 ;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MILLIISECOND")»
			   	co_granule CONSTANT INTERVAL DAY(0) TO SECOND(3) := INTERVAL '1' SECOND / 1E3 ;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MICROSECOND")»
			   	co_granule CONSTANT INTERVAL DAY(0) TO SECOND(6) := INTERVAL '1' SECOND / 1E6 ;
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_NANOSECOND")»
			   	co_granule CONSTANT INTERVAL DAY(0) TO SECOND(9) := INTERVAL '1' SECOND / 1E9 ;
			   «ENDIF»

			   --
			   -- update modes evaluated based on old and new values
			   --
			   co_upd_no_change CONSTANT PLS_INTEGER := 0; -- no update necessary since no changes have been made
			   co_upd_all_cols CONSTANT PLS_INTEGER := 1; -- updates all columns in chosen valid time range
			   co_upd_changed_cols CONSTANT PLS_INTEGER := 2; -- updates changed columns in chosen valid time range

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
			         sys.dbms_output.put_line(in_header);
			      END IF;
			      <<all_versions>>
			      FOR i in 1..in_collection.COUNT()
			      LOOP
			         sys.dbms_output.put_line('row ' || i || ':');
			         «FOR col : model.columnNames»
			         	dbms_output.put_line('.. «String.format("%-30s", col)»: ' || in_collection(i).«col»);
			         «ENDFOR»
			      END LOOP all_versions;
			   END print_collection;

			   --
			   -- get_update_mode
			   --
			   FUNCTION get_update_mode (
			      in_new_row IN «model.objectTypeName»,
			      in_old_row IN «model.objectTypeName»
			   ) RETURN PLS_INTEGER IS
			      l_valid_time_range_changed BOOLEAN := FALSE;
			      l_appl_items_changed BOOLEAN := FALSE;
			      l_update_mode PLS_INTEGER;
			   BEGIN
			      IF (in_new_row.«validFrom» != in_old_row.«validFrom» 
			          OR in_new_row.«validFrom» IS NULL AND in_old_row.«validFrom» IS NOT NULL 
			          OR in_new_row.«validFrom» IS NOT NULL AND in_old_row.«validFrom» IS NULL)
			         OR
			         (in_new_row.«validTo» != in_old_row.«validTo»
			          OR in_new_row.«validTo» IS NULL AND in_old_row.«validTo» IS NOT NULL 
			          OR in_new_row.«validTo» IS NOT NULL AND in_old_row.«validTo» IS NULL)
			      THEN
			         l_valid_time_range_changed := TRUE;
			      END IF;
			      IF (
			            «FOR col : model.updateableLatestColumnNames.filter[it != validFrom && it != validTo] SEPARATOR System.lineSeparator + 'OR'»
			            	(in_new_row.«col» != in_old_row.«col» 
			            	 OR in_new_row.«col» IS NULL AND in_old_row.«col» IS NOT NULL
			            	 OR in_new_row.«col» IS NOT NULL AND in_old_row.«col» IS NULL)
			            «ENDFOR»
			         ) 
			      THEN
			         l_appl_items_changed := TRUE;
			      END IF;
			      IF l_appl_items_changed THEN
			         l_update_mode := co_upd_changed_cols;
			      ELSIF l_valid_time_range_changed THEN
			         l_update_mode := co_upd_all_cols;
			      ELSE
			         l_update_mode := co_upd_no_change;
			      END IF;
			      RETURN l_update_mode;
			   END get_update_mode;

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
			       WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_row.«col»«ENDFOR»
			         FOR UPDATE;
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
			   -- upd_affected_version
			   --
			   PROCEDURE upd_affected_version (
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      <<all_versions>>
			      FOR i IN 1..g_versions.COUNT() 
			      LOOP
			         IF g_versions(i).«validFrom» = in_row.«validFrom» THEN
			            g_versions(i).«validFrom» := in_row.«validTo»;
			         END IF;
			      END LOOP all_versions;
			   END upd_affected_version;

			   --
			   -- add_version
			   --
			   PROCEDURE add_version (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_row «model.objectTypeName»;
			   BEGIN
			      l_row := in_row;
			      l_row.«histId» := NULL;
			      g_versions.extend();
			      g_versions(g_versions.last()) := l_row;
			   END add_version;

			   --
			   -- split_version
			   --
			   PROCEDURE split_version (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_version «model.objectTypeName»;
			      l_copy «model.objectTypeName»;
			   BEGIN
			      IF in_row.«validTo» IS NOT NULL THEN
			         l_version := get_version_at(in_at => NVL(in_row.«validFrom», co_minvalue));
			         IF l_version IS NOT NULL THEN
			            IF NVL(l_version.«validTo», co_maxvalue) > in_row.«validTo» THEN
			               l_copy := l_version;
			               l_copy.«validFrom» := in_row.«validTo»;
			               add_version(in_row => l_copy);
			            END IF;
			         END IF;
			      END IF;
			   END split_version;

			   --
			   -- add_first_version
			   --
			   PROCEDURE add_first_version IS
			      l_version «model.objectTypeName»;
			   BEGIN
			      l_version := get_version_at(in_at => co_minvalue);
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
			      l_version := get_version_at(in_at => co_maxvalue);
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
			   -- add_version_at_start
			   --
			   PROCEDURE add_version_at_start (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_version «model.objectTypeName»;
			   BEGIN
			      IF in_row.«validFrom» IS NOT NULL THEN
			         l_version := get_version_at(in_at => in_row.«validFrom»);
			         IF l_version.«validFrom» != in_row.«validFrom» OR l_version.«validFrom» IS NULL THEN
			            l_version.«validFrom» := in_row.«validFrom»;
			            add_version(in_row => l_version);
			         END IF;
			      END IF;
			   END add_version_at_start;

			   --
			   -- add_version_at_end
			   --
			   PROCEDURE add_version_at_end (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_version «model.objectTypeName»;
			   BEGIN
			      IF in_row.«validTo» IS NOT NULL THEN
			         l_version := get_version_at(in_at => in_row.«validTo»);
			         IF l_version.«validFrom» != in_row.«validTo» OR l_version.«validFrom» IS NULL THEN
			            l_version.«validFrom» := in_row.«validTo»;
			            add_version(in_row => l_version);
			         END IF;
			      END IF;
			   END add_version_at_end;

			   --
			   -- upd_all_cols
			   --
			   PROCEDURE upd_all_cols (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_at «model.validTimeDataType»;
			   BEGIN
			      <<all_versions>>
			      FOR i in 1..g_versions.COUNT()
			      LOOP
			         l_at := NVL(g_versions(i).«validFrom», co_minvalue);
			         IF (in_row.«validFrom» IS NULL OR in_row.«validFrom» <= l_at)
			            AND (in_row.«validTo» IS NULL OR in_row.«validTo» > l_at)
			         THEN
			            -- update period 
			            «FOR col : model.updateableColumnNames.filter[it != validFrom && it != validTo]»
			            	g_versions(i).«col» := in_row.«col»;
			            «ENDFOR»
			         END IF;
			      END LOOP all_versions;
			   END upd_all_cols;

			   --
			   -- upd_changed_cols
			   --
			   PROCEDURE upd_changed_cols (
			      in_new_row IN «model.objectTypeName»,
			      in_old_row IN «model.objectTypeName»
			   ) IS
			      l_at «model.validTimeDataType»;
			   BEGIN
			      <<all_versions>>
			      FOR i in 1..g_versions.COUNT()
			      LOOP
			         l_at := NVL(g_versions(i).«validFrom», co_minvalue);
			         IF (in_new_row.«validFrom» IS NULL OR in_new_row.«validFrom» <= l_at)
			            AND (in_new_row.«validTo» IS NULL OR in_new_row.«validTo» > l_at)
			         THEN
			            -- update period
			            «FOR col : model.updateableColumnNames.filter[it != validFrom && it != validTo]»
			            	IF in_new_row.«col» != in_old_row.«col» 
			            	   OR in_new_row.«col» IS NULL AND in_old_row.«col» IS NOT NULL
			            	   OR in_new_row.«col» IS NOT NULL AND in_old_row.«col» IS NULL
			            	THEN
			            	   -- update changed column
			            	   g_versions(i).«col» := in_new_row.«col»;
			            	END IF;
			            «ENDFOR»
			         END IF;
			      END LOOP all_versions;
			   END upd_changed_cols;

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
			   BEGIN
			      l_latest_row := get_version_at(in_at => co_maxvalue);
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
			         <<all_versions>>
			         FOR i in 1..g_versions.COUNT()
			         LOOP
			            «FOR col : model.pkColumnNames»
			            	g_versions(i).«col» := l_latest_row.«col»;
			            «ENDFOR»
			         END LOOP all_versions;
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
			               SELECT 'D' AS «operation», -- NOSONAR, PL/SQL Cop guideline 27, false positive
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
			      upd_affected_version(in_row => io_row);
			      split_version(in_row => io_row);
			      add_version(in_row => io_row);
			      add_first_version;
			      add_last_version;
			      merge_versions;
			      IF changes_history() THEN
			         save_latest;
			         save_versions;
			      END IF;
			   END do_ins;

			   --
			   -- do_upd
			   --
			   PROCEDURE do_upd (
			      io_new_row IN OUT «model.objectTypeName»,
			      in_old_row IN «model.objectTypeName»
			   ) IS
			      l_update_mode PLS_INTEGER;
			   BEGIN
			      truncate_to_granularity(io_row => io_new_row);
			      l_update_mode := get_update_mode(in_new_row => io_new_row, in_old_row => in_old_row);
			      IF l_update_mode IN (co_upd_all_cols, co_upd_changed_cols) THEN
			         load_versions(in_row => in_old_row);
			         split_version(in_row => io_new_row);
			         add_version_at_start(in_row => io_new_row);
			         add_version_at_end(in_row => io_new_row);
			         IF l_update_mode = co_upd_all_cols THEN
			            upd_all_cols(in_row => io_new_row);
			         ELSE
			            upd_changed_cols(in_new_row => io_new_row, in_old_row => in_old_row);
			         END IF;
			         merge_versions;
			         IF changes_history() THEN
			            save_latest;
			            save_versions;
			         END IF;
			      END IF;
			   END do_upd;

			   --
			   -- do_del
			   --
			   PROCEDURE do_del (
			      in_row IN «model.objectTypeName»
			   ) IS
			      l_new_row «model.objectTypeName»;
			      l_old_row «model.objectTypeName»;
			   BEGIN
			      l_new_row := NEW «model.objectTypeName»();
			      l_new_row.«validFrom» := in_row.«validFrom»;
			      l_new_row.«validTo» := in_row.«validTo»;
			      «FOR col : model.pkColumnNames»
			      	l_new_row.«col» := in_row.«col»;
			      «ENDFOR»
			      l_old_row := l_new_row;
			      l_new_row.«isDeleted» := 1;
			      do_upd(io_new_row => l_new_row, in_old_row => l_old_row);
			   END do_del;

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
			         «model.hookPackageName».pre_upd(io_new_row => l_new_row, in_old_row => in_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_upd;
			      «IF model.targetModel == ApiType.NON_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME»
			      	UPDATE «model.latestTableName»
			      	   SET «FOR col : model.columnNames SEPARATOR ', ' + System.lineSeparator + '    '»«col» = l_new_row.«col»«ENDFOR»
			      	 WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_old_row.«col»«ENDFOR»;
			      «ELSE»
			      	do_upd(io_new_row => l_new_row, in_old_row => in_old_row);
			      «ENDIF»
			      <<trap_post_upd>>
			      BEGIN
			         «model.hookPackageName».post_upd(in_new_row => l_new_row, in_old_row => in_old_row);
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
			      	do_del(in_row => in_old_row);
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
