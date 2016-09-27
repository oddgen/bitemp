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
	
	def getAllColumnNames(GeneratorModel model) {
		val cols = new ArrayList<String>
		if (model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME || model.targetModel == ApiType.BI_TEMPORAL) {
			cols.add(BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase)
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase)
			cols.add(model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase)
			cols.add(BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase)
		}
		for (col : model.inputTable.columns.values.filter [
			it.virtualColumn == "NO" && !cols.contains(it.columnName) &&
				it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase
		]) {
			cols.add(col.columnName.toLowerCase)
		}
		return cols
	}

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
		return model.allColumnNames.filter[
			it != BitempRemodeler.HISTORY_ID_COL_NAME.toLowerCase
		]
	}
	
	def getMergeColumnNames(GeneratorModel model) {
		return model.allColumnNames.filter[
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
	
	def getErrTagExpr(GeneratorModel model, String procedureName, String tableLiteral, String startLiteral) {
		val errorTag = ''''«model.apiPackageName».«procedureName» from ' || «tableLiteral»«
		» || ' started at ' || TO_CHAR(«startLiteral», 'YYYY-MM-DD HH24:MI:SS.FF6')'''
		return errorTag
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
			   «val gapStart = BitempRemodeler.GAP_START_COL_NAME.toLowerCase»
			   «val gapEnd = BitempRemodeler.GAP_END_COL_NAME.toLowerCase»
			   «val errorNumber = -20501»
			   «val minDate = "TO_TIMESTAMP('-4712', 'SYYYY')"»
			   «val maxDate = "TO_TIMESTAMP('9999-12-31 23:59:59.999999999', 'YYYY-MM-DD HH24:MI:SS.FF9')"»
			   --
			   -- Note: SQL Developer 4.1.3 cannot produce a complete outline of this package body, because it cannot handle
			   --       the complete flashback_query_clause. The following expression breaks SQL Developer:
			   --
			   --          VERSIONS PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE
			   --
			   --       It's expected that future versions will be able to handle the flashback_query_clause accordingly.
			   --       See "Bug 24608738 - OUTLINE OF PL/SQL PACKAGE BODY BREAKS WHEN USING PERIOD FOR OF FLASHBACK_QUERY_"
			   --       on MOS for details.
			   --

			   --
			   -- Declarations to handle 'ORA-06508: PL/SQL: could not find program unit being called: "«model.conn.metaData.userName».«model.hookPackageName.toUpperCase»"'
			   --
			   e_hook_body_missing EXCEPTION;
			   PRAGMA exception_init(e_hook_body_missing, -6508);

			   --
			   -- Debugging output level
			   --
			   g_debug_output_level dbms_output_level_type := co_off;

			   «IF model.targetModel == ApiType.BI_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
			   --
			   -- valid time constants, implicitely truncated to the granularity of «model.params.get(BitempRemodeler.GRANULARITY)»
			   --
			   co_minvalue CONSTANT «model.validTimeDataType» := TO_TIMESTAMP('-4712', 'SYYYY');
			   co_maxvalue CONSTANT «model.validTimeDataType» := TO_TIMESTAMP('9999-12-31 23:59:59.999999999', 'YYYY-MM-DD HH24:MI:SS.FF9');
			   «IF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_YEAR")»
			   	co_format CONSTANT VARCHAR2(5 CHAR) := 'SYYYY';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MONTH")»
			   	co_format CONSTANT VARCHAR2(8 CHAR) := 'SYYYY-MM';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_WEEK")»
			   	co_format CONSTANT VARCHAR2(11 CHAR) := 'SYYYY-MM-DD';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_DAY")»
			   	co_format CONSTANT VARCHAR2(11 CHAR) := 'SYYYY-MM-DD';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_HOUR")»
			   	co_format CONSTANT VARCHAR2(16 CHAR) := 'SYYYY-MM-DD HH24';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MINUTE")»
			   	co_format CONSTANT VARCHAR2(19 CHAR) := 'SYYYY-MM-DD HH24:MI';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_SECOND")»
			   	co_format CONSTANT VARCHAR2(22 CHAR) := 'SYYYY-MM-DD HH24:MI:SS';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_CENTISECOND")»
			   	co_format CONSTANT VARCHAR2(26 CHAR) := 'SYYYY-MM-DD HH24:MI:SS.FF2';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MILLIISECOND")»
			   	co_format CONSTANT VARCHAR2(26 CHAR) := 'SYYYY-MM-DD HH24:MI:SS.FF3';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_MICROSECOND")»
			   	co_format CONSTANT VARCHAR2(26 CHAR) := 'SYYYY-MM-DD HH24:MI:SS.FF6';
			   «ELSEIF model.params.get(BitempRemodeler.GRANULARITY) == BitempResources.getString("PREF_GRANULARITY_NANOSECOND")»
			   	co_format CONSTANT VARCHAR2(26 CHAR) := 'SYYYY-MM-DD HH24:MI:SS.FF9';
			   «ENDIF»

			   --
			   -- working copy of history rows
			   --
			   g_versions «model.collectionTypeName»;

			   --
			   -- original, unchanged history rows 
			   --
			   g_versions_original «model.collectionTypeName»;

			   «ENDIF»
			   --
			   -- print_line
			   --
			   PROCEDURE print_line (
			      in_proc  IN VARCHAR2,
			      in_level IN dbms_output_level_type,
			      in_line  IN VARCHAR2
			   ) IS
			   BEGIN
			      IF in_level <= g_debug_output_level THEN
			         sys.dbms_output.put(to_char(systimestamp, 'HH24:MI:SS.FF6'));
			         CASE in_level
			            WHEN co_info THEN
			               sys.dbms_output.put(' INFO  ');
			            WHEN co_debug THEN
			               sys.dbms_output.put(' DEBUG ');
			            ELSE
			               sys.dbms_output.put(' TRACE ');
			         END CASE;
			         sys.dbms_output.put(substr(rpad(in_proc,27), 1, 27) || ' ');
			         sys.dbms_output.put_line(substr(in_line, 1, 250));
			      END IF;
			   END print_line;

			   --
			   -- print_lines
			   --
			   PROCEDURE print_lines (
			      in_proc  IN VARCHAR2,
			      in_level IN dbms_output_level_type,
			      in_lines IN CLOB
			   ) IS
			   BEGIN
			      IF in_level <= g_debug_output_level THEN
			         <<all_lines>>
			         FOR r_line IN (
			            SELECT regexp_substr(in_lines, '[^' || chr(10) || ']+', 1, level) AS line       
			              FROM dual
			           CONNECT BY instr(in_lines, chr(10), 1, level - 1) BETWEEN 1 AND length(in_lines) - 1
			         ) LOOP
			            print_line(in_proc => in_proc, in_level => in_level, in_line => r_line.line);
			         END LOOP all_lines;
			      END IF;
			   END print_lines;

			   «IF model.targetModel == ApiType.BI_TEMPORAL || model.targetModel == ApiType.UNI_TEMPORAL_VALID_TIME»
			   --
			   -- print_collection
			   --
			   PROCEDURE print_collection (
			      in_proc       IN VARCHAR2,
			      in_collection IN «model.collectionTypeName»
			   ) IS
			   BEGIN
			      <<all_versions>>
			      FOR i in 1..in_collection.COUNT()
			      LOOP
			         print_line(in_proc => in_proc, in_level => co_trace, in_line => 'row ' || i || ':');
			         «val dates = model.inputTable.columns.filter[
			         	k, v| v.dataType == "DATE" || v.dataType == "TIMESTAMP"
			         ]»
			         «FOR col : model.allColumnNames»
			         	print_line(
			         	   in_proc  => in_proc,
			         	   in_level => co_trace,
			         	   in_line => '   - «String.format("%-30s", col)»: ' || «
			         	      IF col == validFrom || col == validTo || dates.get(col.toUpperCase) != null
			         	      	»TO_CHAR(in_collection(i).«col», co_format)«
			         	      ELSE
			         	      	»in_collection(i).«col
			         	      »«ENDIF»
			         	);
			         «ENDFOR»
			      END LOOP all_versions;
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
			   -- check_period
			   -- 
			   PROCEDURE check_period (
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      IF NOT (in_row.«validFrom» < in_row.«validTo» 
			              OR in_row.«validFrom» IS NULL AND in_row.«validTo» IS NOT NULL
			              OR in_row.«validFrom» IS NOT NULL AND in_row.«validTo» IS NULL) 
			      THEN
			         raise_application_error(«errorNumber», 'Invalid period. «validFrom» (' 
			            || TO_CHAR(in_row.«validFrom», co_format)
			            || ') must be less than «validTo» ('
			            || TO_CHAR(in_row.«validTo», co_format)
			            || ').');
			      END IF;
			   END check_period;

			   --
			   -- load_versions
			   --
			   PROCEDURE load_versions (
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      -- In 12.1.0.2 locked rows are recognized as a change and may lead to unwanted FBA history
			      -- To avoid this the row in the master table will be locked instead (issue #1)
			      <<lock_latest>>
			      DECLARE
			         l_rid UROWID;
			      BEGIN
			         SELECT ROWID
			           INTO l_rid
			           FROM «model.latestTableName»
			          WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_row.«col»«ENDFOR»
			            FOR UPDATE;
			         print_line(
			            in_proc  => 'load_version',
			            in_level => co_debug,
			            in_line  => 'Locked Latest row.'
			         );
			         EXCEPTION
			            WHEN NO_DATA_FOUND THEN
			               print_line(
			                  in_proc  => 'load_version',
			                  in_level => co_debug,
			                  in_line  => 'Latest row does not exist, therefore cannot lock it.'
			               );
			            
			      END lock_latest;
			      -- use VERSIONS PERIOD FOR to ensure no periods of the target table are filtered 
			      -- on session level by DBMS_FLASHBACK_ARCHIVE.ENABLE_AT_VALID_TIME
			      SELECT «model.objectTypeName» (
			                «FOR col : model.allColumnNames SEPARATOR ','»
			                	«col»
			                «ENDFOR»
			             )
			        BULK COLLECT INTO g_versions_original
			        FROM «model.historyTableName» «
			             »VERSIONS PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE
			       WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_row.«col»«ENDFOR»;
			      print_line(
			         in_proc  => 'load_version',
			         in_level => co_debug,
			         in_line  => SQL%ROWCOUNT || ' rows locked and loaded.'
			      );
			      g_versions := g_versions_original;
			      print_collection(in_proc => 'load_version', in_collection => g_versions);
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
			                          «FOR col : model.allColumnNames SEPARATOR ","»
			                          	«IF col == validTo»
			                          		LEAD («validFrom», 1, «validTo») OVER (ORDER BY «validFrom» NULLS FIRST)
			                          	«ELSE»
			                          		«col»
			                          	«ENDIF»
			                          «ENDFOR»
			                       ) version
			                  FROM TABLE(g_versions)
			             ) v
			       WHERE (v.version.«validFrom» IS NULL OR v.version.«validFrom» <= in_at)
			         AND (v.version.«validTo» IS NULL OR v.version.«validTo» > in_at);
			      print_line(in_proc => 'get_version_at', in_level => co_debug, in_line => SQL%ROWCOUNT || ' rows found at ' || to_char(in_at, co_format));
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
			            -- current MINUS original
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions)
			             MINUS
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions_original)
			         ),
			         diff2 AS (
			            -- original MINUS current
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions_original)
			             MINUS
			            SELECT «FOR col : model.diffColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM TABLE(g_versions)
			         ),
			         diff AS (
			            -- combined differences
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
			     print_line(in_proc => 'changes_history', in_level => co_debug, in_line => SQL%ROWCOUNT || ' differences found.');
			     RETURN l_diff_count > 0;
			   END changes_history;

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
			            l_version.«isDeleted» := NULL;
			            add_version(in_row => l_version);
			            print_line(in_proc => 'add_version_at_start', in_level => co_debug, in_line => 'added period at start');
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
			            print_line(in_proc => 'add_version_at_end', in_level => co_debug, in_line => 'added period at end');
			         END IF;
			      END IF;
			   END add_version_at_end;

			   --
			   -- merge_versions
			   --
			   PROCEDURE merge_versions IS
			      l_merged «model.collectionTypeName»;
			   BEGIN
			      print_collection(
			         in_proc       => 'merge_versions',
			         in_collection => g_versions
			      );
			      WITH
			         base AS (
			            -- calculate «validFrom» since this is not maintained within the API package
			            SELECT «histId»,
			                   «validFrom»,
			                   LEAD («validFrom», 1, «validTo») OVER (ORDER BY «validFrom» NULLS FIRST) AS «validTo»,
			                   «FOR col : model.mergeColumnNames SEPARATOR ","»
			                   	«col»
			                   «ENDFOR»
			              FROM TABLE(g_versions)
			         ),
			         valid AS (
			            -- filter invalid periods, e.g. produced by truncation
			            SELECT «FOR col : model.allColumnNames 
			                    SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM base
			             WHERE «validFrom» < «validTo»
			                OR «validFrom» IS NULL AND «validTo» IS NOT NULL
			                OR «validFrom» IS NOT NULL AND «validTo» IS NULL
			                OR «validFrom» IS NULL AND «validTo» IS NULL
			         ),
			         merged AS (
			            -- merge periods with identical column values into a single row
			            SELECT «FOR col : model.allColumnNames 
			                    SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM valid
			                   MATCH_RECOGNIZE (
			                      PARTITION BY «FOR col : model.mergeColumnNames
			                                    SEPARATOR ", "»«col»«ENDFOR»
			                      ORDER BY «validFrom» NULLS FIRST
			                      MEASURES «histId» AS «histId»,
			                               FIRST(«validFrom») AS «validFrom»,
			                               LAST(«validTo») AS «validTo»
			                      ONE ROW PER MATCH
			                      PATTERN ( strt nxt* )
			                      DEFINE nxt AS «validFrom» = PREV(«validTo»)
			                   )
			         )
			      -- main
			      SELECT «model.objectTypeName» (
			                «FOR col : model.allColumnNames SEPARATOR ","»
			                	«col»
			                «ENDFOR»
			             )
			        BULK COLLECT INTO l_merged
			        FROM merged;
			       print_line(
			          in_proc => 'merge_versions',
			          in_level => co_debug,
			          in_line => g_versions.COUNT() - l_merged.COUNT() || ' periods merged.'
			       );
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
			         INSERT INTO «model.latestTableName» (
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
			         print_line(in_proc => 'save_latest', in_level => co_debug, in_line => SQL%ROWCOUNT || ' row inserted.');
			         <<all_versions>>
			         FOR i in 1..g_versions.COUNT()
			         LOOP
			            «FOR col : model.pkColumnNames»
			            	g_versions(i).«col» := l_latest_row.«col»;
			            «ENDFOR»
			         END LOOP all_versions;
			         print_line(in_proc => 'save_latest', in_level => co_debug, in_line => 'set primary key for all periods.');
			      ELSE
			         UPDATE «model.latestTableName»
			            SET «FOR col : model.updateableLatestColumnNames SEPARATOR ',' + System.lineSeparator + '    '»«
			                	col» = l_latest_row.«col»«
			                ENDFOR»
			          WHERE «FOR col : model.pkColumnNames 
			                 SEPARATOR System.lineSeparator + '  AND '»«col» = l_latest_row.«col»«ENDFOR»
			            AND (
			                    «FOR col : model.updateableLatestColumnNames SEPARATOR " OR"»
			                    	(«col» != l_latest_row.«col» OR «
			                    	col» IS NULL AND l_latest_row.«col» IS NOT NULL OR «
			                    	col» IS NOT NULL AND l_latest_row.«col» IS NULL)
			                    «ENDFOR»
			                );
			         print_line(
			            in_proc  => 'save_latest', 
			            in_level => co_debug, 
			            in_line  => SQL%ROWCOUNT || ' rows updated.'
			         );
			      END IF;
			   END save_latest;

			   --
			   -- save_versions
			   --
			   PROCEDURE save_versions IS
			   BEGIN
			      print_collection(in_proc => 'save_versions', in_collection => g_versions);
			      -- dedicated delete step due to issue #1
			      DELETE
			        FROM «model.historyTableName»
			       WHERE «histId» IN (
			                SELECT o.«histId»
			                  FROM TABLE(g_versions_original) o
			                  LEFT JOIN TABLE(g_versions) w
			                    ON w.«histId» = o.«histId»
			                 WHERE w.«histId» IS NULL
			             );
			      print_line(
			         in_proc  => 'save_versions', 
			         in_level => co_debug, 
			         in_line  => SQL%ROWCOUNT || ' rows deleted.'
			      );
			      MERGE 
			       INTO (
			               -- use VERSIONS PERIOD FOR to ensure no periods of the target table are filtered 
			               -- on session level by DBMS_FLASHBACK_ARCHIVE.ENABLE_AT_VALID_TIME
			               SELECT «FOR col : model.allColumnNames SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			                 FROM «model.historyTableName» «
			                      »VERSIONS PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE
			            ) t
			      USING (
			               SELECT «FOR col : model.allColumnNames SEPARATOR ","»
			                      	«IF col == validTo»
			                      		LEAD («validFrom», 1, NULL) OVER (ORDER BY «validFrom» NULLS FIRST) AS «validTo»
			                      	«ELSE»
			                      		«col»
			                      	«ENDIF»
			                      «ENDFOR»
			                 FROM TABLE(g_versions)
			            ) s
			         ON (s.«histId» = t.«histId»)
			       WHEN MATCHED THEN
			               UPDATE
			                  SET «FOR col : model.updateableColumnNames
			                       SEPARATOR ',' + System.lineSeparator + '    '»t.«col» = s.«col»«ENDFOR»
			                WHERE (
			                         «FOR col : model.updateableColumnNames 
			                          SEPARATOR System.lineSeparator + 'OR'»
			                         	(s.«col» != t.«col»
			                         	 OR s.«col» IS NULL AND t.«col» IS NOT NULL
			                         	 OR s.«col» IS NOT NULL AND t.«col» IS NULL)
			                         «ENDFOR»
			                      )
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
			      print_line(
			         in_proc  => 'save_versions', 
			         in_level => co_debug, 
			         in_line  => SQL%ROWCOUNT || ' rows merged.'
			      );
			   END save_versions;

			   --
			   -- do_ins
			   --
			   PROCEDURE do_ins (
			      io_row IN OUT «model.objectTypeName»
			   ) IS
			      --
			      -- do_ins.del_enclosed_versions
			      --
			      PROCEDURE del_enclosed_versions IS
			         l_versions «model.collectionTypeName»;
			      BEGIN
			         SELECT «model.objectTypeName» (
			                «FOR col : model.allColumnNames SEPARATOR ","»
			                	«col»
			                «ENDFOR»
			                )
			           BULK COLLECT INTO l_versions
			           FROM TABLE(g_versions)
			          WHERE NOT (
			                       NVL(«validFrom», co_minvalue) >= NVL(io_row.«validFrom», co_minvalue) 
			                       AND NVL(«validTo», co_maxvalue) <= NVL(io_row.«validTo», co_maxvalue)
			                    );
			         print_line(
			            in_proc  => 'do_ins.del_enclosed_versions', 
			            in_level => co_debug, 
			            in_line  => g_versions.COUNT() - l_versions.COUNT() || ' enclosed periods deleted.'
			         );
			         g_versions := l_versions;
			     END del_enclosed_versions;
			      --
			      -- do_ins.split_version
			      --
			      PROCEDURE split_version IS
			         l_version «model.objectTypeName»;
			         l_copy «model.objectTypeName»;
			      BEGIN
			         IF io_row.«validTo» IS NOT NULL THEN
			            l_version := get_version_at(in_at => NVL(io_row.«validFrom», co_minvalue));
			            IF l_version IS NOT NULL THEN
			               IF NVL(l_version.«validTo», co_maxvalue) > io_row.«validTo» THEN
			                  l_copy := l_version;
			                  l_copy.«validFrom» := io_row.«validTo»;
			                  add_version(in_row => l_copy);
			                  print_line(
			                     in_proc  => 'do_ins.split_version', 
			                     in_level => co_debug, 
			                     in_line => 'splitted version at '|| TO_CHAR(io_row.«validTo», co_format) || '.'
			                  );
			               END IF;
			            END IF;
			         END IF;
			      END split_version;
			      --
			      -- do_ins.upd_affected_version
			      --
			      PROCEDURE upd_affected_version IS
			      BEGIN
			         <<all_versions>>
			         FOR i IN 1..g_versions.COUNT() 
			         LOOP
			            IF g_versions(i).«validFrom» >= io_row.«validFrom»
			               AND g_versions(i).«validFrom» < NVL(io_row.«validTo», co_maxvalue)
			            THEN
			               g_versions(i).«validFrom» := io_row.«validTo»;
			               print_line(
			                  in_proc  => 'do_ins.upd_affected_version',
			                  in_level => co_debug,
			                  in_line  => 'updated affected period.'
			               );
			            END IF;
			         END LOOP all_versions;
			      END upd_affected_version;
			      --
			      -- do_ins.add_first_version
			      --
			      PROCEDURE add_first_version IS
			         l_version «model.objectTypeName»;
			      BEGIN
			         l_version := get_version_at(in_at => co_minvalue);
			         IF l_version IS NULL THEN
			            SELECT «model.objectTypeName» (
			                      «FOR col : model.allColumnNames SEPARATOR ","»
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
			            print_line(
			               in_proc  => 'do_ins.add_first_version',
			               in_level => co_debug, 
			               in_line  => 'first period added.'
			            );
			         END IF;
			      END add_first_version;
			      --
			      -- do_ins.add_last_version
			      --
			      PROCEDURE add_last_version IS
			         l_version «model.objectTypeName»;
			      BEGIN
			         l_version := get_version_at(in_at => co_maxvalue);
			         IF l_version IS NULL THEN
			            SELECT «model.objectTypeName» (
			                      «FOR col : model.allColumnNames SEPARATOR ","»
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
			            print_line(
			               in_proc  => 'do_ins.add_last_version',
			               in_level => co_debug,
			               in_line  => 'last period added.'
			            );
			         END IF;
			      END add_last_version;
			      --
			      -- do_ins.upd_version_at_start
			      --
			      PROCEDURE upd_version_at_start IS
			         l_updated BOOLEAN := FALSE;
			      BEGIN
			         <<all_versions>>
			         FOR i IN 1..g_versions.COUNT() 
			         LOOP
			            IF g_versions(i).«validFrom» = io_row.«validFrom» 
			               OR g_versions(i).«validFrom» IS NULL AND io_row.«validFrom» IS NULL
			            THEN
			               «FOR col : model.allColumnNames.filter[it != histId]»
			               	g_versions(i).«col» := io_row.«col»;
			               «ENDFOR»
			               l_updated := TRUE;
			            END IF;
			         END LOOP all_versions;
			         IF l_updated THEN
			            print_line(
			               in_proc  => 'do_ins.upd_version_at_start',
			               in_level => co_debug,
			               in_line  => 'updated row at ' || TO_CHAR(io_row.«validFrom», co_format)
			            );
			         ELSE
			            add_version(in_row => io_row);
			            print_line(
			               in_proc  => 'do_ins.upd_version_at_start',
			               in_level => co_debug,
			               in_line  => 'added row at ' || TO_CHAR(io_row.«validFrom», co_format)
			            );
			         END IF;
			      END upd_version_at_start;
			   BEGIN
			      truncate_to_granularity(io_row => io_row);
			      check_period(in_row => io_row);
			      load_versions(in_row => io_row);
			      del_enclosed_versions;
			      split_version;
			      upd_affected_version;
			      upd_version_at_start;
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
			      --
			      -- update modes evaluated based on old and new values
			      --
			      co_upd_no_change    CONSTANT PLS_INTEGER := 0; -- no update necessary since no changes have been made
			      co_upd_all_cols     CONSTANT PLS_INTEGER := 1; -- updates all columns in chosen valid time range
			      co_upd_changed_cols CONSTANT PLS_INTEGER := 2; -- updates changed columns in chosen valid time range
			      --
			      -- do_upd.set_update_mode
			      --
			      PROCEDURE set_update_mode IS
			         l_valid_time_range_changed BOOLEAN := FALSE;
			         l_appl_items_changed BOOLEAN := FALSE;
			      BEGIN
			         IF (io_new_row.«validFrom» != in_old_row.«validFrom» 
			             OR io_new_row.«validFrom» IS NULL AND in_old_row.«validFrom» IS NOT NULL 
			             OR io_new_row.«validFrom» IS NOT NULL AND in_old_row.«validFrom» IS NULL)
			            OR
			            (io_new_row.«validTo» != in_old_row.«validTo»
			             OR io_new_row.«validTo» IS NULL AND in_old_row.«validTo» IS NOT NULL 
			             OR io_new_row.«validTo» IS NOT NULL AND in_old_row.«validTo» IS NULL)
			         THEN
			            l_valid_time_range_changed := TRUE;
			         END IF;
			         IF (
			               «FOR col : model.updateableLatestColumnNames.filter[it != isDeleted] 
			                SEPARATOR System.lineSeparator + 'OR'»
			               	(io_new_row.«col» != in_old_row.«col» 
			               	 OR io_new_row.«col» IS NULL AND in_old_row.«col» IS NOT NULL
			               	 OR io_new_row.«col» IS NOT NULL AND in_old_row.«col» IS NULL)
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
			      END set_update_mode;
			      --
			      -- do_upd.upd_all_cols
			      --
			      PROCEDURE upd_all_cols IS
			         l_at «model.validTimeDataType»;
			      BEGIN
			         <<all_versions>>
			         FOR i in 1..g_versions.COUNT()
			         LOOP
			            l_at := NVL(g_versions(i).«validFrom», co_minvalue);
			            IF (io_new_row.«validFrom» IS NULL OR io_new_row.«validFrom» <= l_at)
			               AND (io_new_row.«validTo» IS NULL OR io_new_row.«validTo» > l_at)
			            THEN
			               -- update period 
			               «FOR col : model.updateableColumnNames.filter[it != validFrom && it != validTo]»
			               	g_versions(i).«col» := io_new_row.«col»;
			               «ENDFOR»
			               print_line(
			                  in_proc  => 'do_upd.upd_all_cols',
			                  in_level => co_debug,
			                  in_line  => 'all columns updated.'
			               );
			            END IF;
			         END LOOP all_versions;
			      END upd_all_cols;
			      --
			      -- do_upd.upd_changed_cols
			      --
			      PROCEDURE upd_changed_cols IS
			         l_at «model.validTimeDataType»;
			      BEGIN
			         <<all_versions>>
			         FOR i in 1..g_versions.COUNT()
			         LOOP
			            IF g_versions(i).«isDeleted» IS NULL THEN
			               l_at := NVL(g_versions(i).«validFrom», co_minvalue);
			               IF (io_new_row.«validFrom» IS NULL OR io_new_row.«validFrom» <= l_at)
			                  AND (io_new_row.«validTo» IS NULL OR io_new_row.«validTo» > l_at)
			               THEN
			                  -- update period
			                  «FOR col : model.updateableColumnNames.filter[it != validFrom && it != validTo && it != isDeleted]»
			                  	IF io_new_row.«col» != in_old_row.«col» 
			                  	   OR io_new_row.«col» IS NULL AND in_old_row.«col» IS NOT NULL
			                  	   OR io_new_row.«col» IS NOT NULL AND in_old_row.«col» IS NULL
			                  	THEN
			                  	   -- update changed column
			                  	   g_versions(i).«col» := io_new_row.«col»;
			                  	END IF;
			                  «ENDFOR»
			                  print_line(
			                     in_proc  => 'do_upd.upd_changed_cols',
			                     in_level => co_debug,
			                     in_line  => 'all changed columns updated.');
			               END IF;
			            END IF;
			         END LOOP all_versions;
			      END upd_changed_cols;
			   BEGIN
			      truncate_to_granularity(io_row => io_new_row);
			      check_period(in_row => io_new_row);
			      set_update_mode;
			      IF l_update_mode IN (co_upd_all_cols, co_upd_changed_cols) THEN
			         load_versions(in_row => in_old_row);
			         add_version_at_start(in_row => io_new_row);
			         add_version_at_end(in_row => io_new_row);
			         IF l_update_mode = co_upd_all_cols THEN
			            upd_all_cols;
			         ELSE
			            upd_changed_cols;
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
			      l_row «model.objectTypeName»;
			      --
			      -- do_del.set_deleted
			      --
			      PROCEDURE set_deleted IS
			         l_at «model.validTimeDataType»;
			      BEGIN
			         <<all_versions>>
			         FOR i in 1..g_versions.COUNT()
			         LOOP
			            IF g_versions(i).«isDeleted» IS NULL THEN
			               l_at := NVL(g_versions(i).«validFrom», co_minvalue);
			               IF (l_row.«validFrom» IS NULL OR l_row.«validFrom» <= l_at)
			                  AND (l_row.«validTo» IS NULL OR l_row.«validTo» > l_at)
			               THEN
			                  -- update period
			                  g_versions(i).«isDeleted» := 1;
			                  print_line(
			                     in_proc  => 'do_del.set_deleted',
			                     in_level => co_debug,
			                     in_line  => 'period starting at "' 
			                                 || TO_CHAR(g_versions(i).«validFrom», co_format)
			                                 || '" deleted.'
			                  );
			               END IF;
			            END IF;
			         END LOOP all_versions;
			      END set_deleted;
			   BEGIN
			      l_row := in_row;
			      truncate_to_granularity(io_row => l_row);
			      check_period(in_row => l_row);
			      load_versions(in_row => l_row);
			      add_version_at_start(in_row => l_row);
			      add_version_at_end(in_row => l_row);
			      set_deleted;
			      merge_versions;
			      IF changes_history() THEN
			         save_latest;
			         save_versions;
			      END IF;
			   END do_del;

			   --
			   -- table_exists
			   --
			   FUNCTION table_exists (
			      in_owner IN VARCHAR2,
			      in_table IN VARCHAR2
			   ) RETURN BOOLEAN IS
			      l_found PLS_INTEGER;
			   BEGIN
			      SELECT COUNT(*)
			        INTO l_found
			        FROM user_tables
			       WHERE table_name = UPPER(in_table);
			      RETURN l_found > 0;
			   END table_exists;

			   --
			   -- check_table_prerequisites
			   --
			   PROCEDURE check_table_prerequisites (
			      in_owner     IN VARCHAR2,
			      in_sta_table IN VARCHAR2,
			      in_log_table IN VARCHAR2
			   ) IS
			   BEGIN
			      IF NOT table_exists(in_owner => in_owner, in_table => in_sta_table) THEN
			         raise_application_error(«errorNumber
			            », 'staging table ' || in_owner || '.' || in_sta_table || ' not found.'
			         );
			      END IF;
			      IF NOT table_exists(in_owner => in_owner, in_table => in_log_table) THEN
			         raise_application_error(«errorNumber
			            », 'logging table ' || in_owner || '.' || in_log_table || ' not found.'
			         );
			      END IF;
			   END check_table_prerequisites;

			   --
			   -- check_reject_limit
			   --
			   PROCEDURE check_reject_limit (
			      in_reject_limit IN VARCHAR2
			   ) IS
			      l_reject_limit VARCHAR2(100 CHAR);
			   BEGIN
			      IF in_reject_limit IS NULL THEN
			         raise_application_error(«errorNumber
			            », 'in_reject_limit must not be NULL.'
			         );
			      END IF;
			      l_reject_limit := regexp_replace(UPPER(substr(in_reject_limit,1,100)), '[0-9]+', NULL);
			      IF NOT (l_reject_limit IS NULL OR l_reject_limit = 'UNLIMITED') THEN
			         raise_application_error(«errorNumber
			            », 'invalid value for in_reject_limit defined. '
			            || 'Valid is any integer value and UNLIMITED.');
			      END IF;
			   END check_reject_limit;

			   --
			   -- create_load_tables
			   --
			   PROCEDURE create_load_tables (
			      in_sta_table IN VARCHAR2 DEFAULT '«model.stagingTableName.toUpperCase»',
			      in_log_table IN VARCHAR2 DEFAULT '«model.loggingTableName.toUpperCase»',
			      in_drop_existing IN BOOLEAN DEFAULT TRUE
			   ) IS
			      l_stmt CLOB;
			      --
			      PROCEDURE exec_stmt IS
			      BEGIN
			         print_lines(in_proc => 'create_load_tables.exec_stmt', in_level => co_trace, in_lines => l_stmt);
			         EXECUTE IMMEDIATE l_stmt;
			      END exec_stmt;
			      --
			      PROCEDURE drop_table (in_table IN VARCHAR2) IS
			      BEGIN
			         l_stmt := 'DROP TABLE ' || in_table;
			         exec_stmt;
			         print_line(
			            in_proc  => 'create_load_tables.drop_table', 
			            in_level => co_debug, 
			            in_line  => in_table || ' dropped.'
			         );
			      END drop_table;
			      --
			      PROCEDURE create_sta_table IS
			      BEGIN
			         l_stmt := q'[
			            CREATE TABLE ]' || in_sta_table || q'[ (
			               «model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase» «model.validTimeDataType» NULL,
			               «model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase» «model.validTimeDataType» NULL,
			               «BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» NUMBER(1,0) NULL,
			               CHECK («BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» = 1),
			               «FOR col : model.inputTable.columns.values.filter[!it.isTemporalValidityColumn(model) && 
			               	it.columnName != BitempRemodeler.IS_DELETED_COL_NAME.toUpperCase && it.virtualColumn == "NO"
			               ] SEPARATOR ","»
			               	«col.columnName.toLowerCase» «col.fullDataType»«
			               	»«IF !col.defaultClause.empty» «col.defaultClause»«ENDIF» «
			               	»«IF model.pkColumnNames.contains(col.columnName.toLowerCase)»«col.notNull»«ENDIF»
			               «ENDFOR»
			            )
			         ]';
			         exec_stmt;
			         print_line(
			            in_proc => 'create_load_tables.create_sta_table', 
			            in_level => co_debug, 
			            in_line => in_sta_table || ' created.'
			         );
			      END create_sta_table;
			      --
			      PROCEDURE create_log_table IS
			      BEGIN
			         sys.dbms_errlog.create_error_log(
			            dml_table_name     => '«model.historyTableName»',
			            err_log_table_name => in_log_table, 
			            skip_unsupported   => TRUE);
			         print_line(
			            in_proc  => 'create_load_tables.create_log_table', 
			            in_level => co_debug, 
			            in_line  => in_log_table || ' created.'
			         );
			      END create_log_table;
			   BEGIN
			      print_line(in_proc => 'create_load_tables', in_level => co_info, in_line => 'started.');
			      IF in_drop_existing THEN
			         IF table_exists(in_owner => USER, in_table => in_sta_table) THEN
			            drop_table(in_table => in_sta_table);
			         END IF;
			         IF table_exists(in_owner => USER, in_table => in_log_table) THEN
			            drop_table(in_table => in_log_table);
			         END IF;
			      END IF;
			      create_sta_table;
			      create_log_table;
			      print_line(in_proc => 'create_load_tables', in_level => co_info, in_line => 'completed.');
			   END create_load_tables;

			   --
			   -- init_load
			   --
			   PROCEDURE init_load (
			      in_owner        IN VARCHAR2 DEFAULT USER,
			      in_sta_table    IN VARCHAR2 DEFAULT '«model.stagingTableName.toUpperCase»',
			      in_log_table    IN VARCHAR2 DEFAULT '«model.loggingTableName.toUpperCase»',
			      in_reject_limit IN VARCHAR2 DEFAULT '0'
			   ) IS
			      CURSOR c_fk IS
			         SELECT constraint_name 
			           FROM user_constraints 
			          WHERE table_name = '«model.historyTableName.toUpperCase»'
			            AND constraint_type = 'R';
			      CURSOR c_uk IS
			         SELECT constraint_name 
			           FROM user_constraints 
			          WHERE table_name = '«model.historyTableName.toUpperCase»'
			            AND constraint_type = 'U';
			      --
			      -- init_load.check_tables_empty
			      --
			      PROCEDURE check_tables_empty IS
			         l_rows_found PLS_INTEGER;
			      BEGIN
			         SELECT COUNT(*) 
			           INTO l_rows_found
			           FROM «model.latestTableName»
			          WHERE ROWNUM = 1;
			         IF l_rows_found > 0 THEN
			            raise_application_error(«errorNumber
			               », 'latest table «model.latestTableName» is not empty.'
			            );
			         END IF;
			         SELECT COUNT(*) 
			           INTO l_rows_found
			           FROM «model.historyTableName»
			          WHERE ROWNUM = 1;
			         IF l_rows_found > 0 THEN
			            raise_application_error(«errorNumber
			               », 'history table «model.historyTableName» is not empty.'
			            );
			         END IF;
			      END check_tables_empty;
			      --
			      -- init_load.disable_fk_constraints
			      --
			      PROCEDURE disable_fk_constraints IS
			      BEGIN
			         -- "ALTER SESSION SET CONSTRAINTS = DEFERRED" becomes slow on large datasets, e.g 78 vs. 14 seconds
			         <<all_fk_constraints>>
			         FOR r_fk IN c_fk LOOP
			            EXECUTE IMMEDIATE 'ALTER TABLE «model.historyTableName» MODIFY CONSTRAINT "' 
			               || r_fk.constraint_name || '" DISABLE';
			            print_line(
			               in_proc  => 'init_load.enable_fk_constraints', 
			               in_level => co_debug, 
			               in_line  => 'fk constraint ' || r_fk.constraint_name || ' disabled.'
			            );
			         END LOOP all_fk_constraints;
			      END disable_fk_constraints;
			      --
			      -- init_load.enable_fk_constraints
			      --
			      PROCEDURE enable_fk_constraints IS
			      BEGIN
			         <<all_fk_constraints>>
			         FOR r_fk IN c_fk LOOP
			            EXECUTE IMMEDIATE 'ALTER TABLE «model.historyTableName» MODIFY CONSTRAINT "' 
			               || r_fk.constraint_name || '" ENABLE';
			            print_line(
			               in_proc  => 'init_load.enable_fk_constraints', 
			               in_level => co_debug, 
			               in_line  => 'fk constraint ' || r_fk.constraint_name || ' enabled.'
			            );
			         END LOOP all_fk_constraints;
			      END enable_fk_constraints;
			      --
			      -- init_load.drop_uk_constraints
			      --
			      PROCEDURE drop_uk_constraints IS
			      BEGIN
			         -- It is just too expensive to keep UK constraints during load
			         -- All UK constraints are dropped, even just one is expected.
			         <<all_uk_constraints>>
			         FOR r_uk IN c_uk LOOP
			            EXECUTE IMMEDIATE 'ALTER TABLE «model.historyTableName» DROP CONSTRAINT "' 
			               || r_uk.constraint_name || '" DROP INDEX';
			            print_line(
			               in_proc  => 'init_load.drop_uk_constraints', 
			               in_level => co_debug, 
			               in_line  => 'uk constraint ' || r_uk.constraint_name || ' and dropped (including index).'
			            );
			         END LOOP all_uk_constraints;
			      END drop_uk_constraints;
			      --
			      -- init_load.create_uk_constraint
			      --
			      PROCEDURE create_uk_constraint IS
			      BEGIN
			         EXECUTE IMMEDIATE 'ALTER TABLE «model.historyTableName» ADD UNIQUE («
			            FOR col : model.historyUkColumnNames SEPARATOR ", "»«col»«ENDFOR») DEFERRABLE INITIALLY DEFERRED';
			         print_line(
			            in_proc  => 'init_load.create_uk_constraint', 
			            in_level => co_debug, 
			            in_line  => 'uk constraint created.'
			         );
			      END create_uk_constraint;
			      --
			      -- init_load.do
			      --
			      PROCEDURE do IS
			         l_stmt    CLOB;
			         l_err_tag VARCHAR2(1000 CHAR);
			      BEGIN
			         l_err_tag := «model.getErrTagExpr("init_load", "in_sta_table", "SYSTIMESTAMP")»;
			         l_stmt := q'[
			            INSERT /*+append */ ALL
			               WHEN 1=1 THEN
			                    INTO «model.historyTableName» (
			                            «FOR col : model.columnNames.filter[it != histId] SEPARATOR ","»
			                            	«col»
			                            «ENDFOR»
			                         )
			                  VALUES (
			                            «FOR col : model.columnNames.filter[it != histId] SEPARATOR ","»
			                            	«col»
			                            «ENDFOR»
			                         )
			                     LOG ERRORS INTO ]' || in_log_table || q'[(']' || l_err_tag || q'[') REJECT LIMIT ]' || in_reject_limit || q'[
			               WHEN «validTo» IS NULL THEN
			                    INTO «model.latestTableName» (
			                           «FOR col : model.latestColumnNames SEPARATOR ","»
			                           	«col»
			                           «ENDFOR»
			                         ) 
			                  VALUES (
			                           «FOR col : model.latestColumnNames SEPARATOR ","»
			                           	«col»
			                           «ENDFOR»
			                          )
			                     LOG ERRORS INTO ]' || in_log_table || q'[(']' || l_err_tag || q'[') REJECT LIMIT ]' || in_reject_limit || q'[
			            WITH
			               active AS (
			                  -- truncate period columns if granularity is different to the data type default
			                  «IF model.granularityRequiresTruncation»
			                  	SELECT TRUNC(«validFrom», '«model.granuarityTruncationFormat»') AS «validFrom»,
			                  	       TRUNC(«validTo», '«model.granuarityTruncationFormat»') AS «validTo»,
			                  «ELSE»
			                  	SELECT «validFrom»,
			                  	       «validTo»,
			                  «ENDIF»
			                         «FOR col : model.columnNames.filter[
			                         	it != histId && it != validFrom && it != validTo
			                         ] SEPARATOR ","»
			                          «col»
			                         «ENDFOR»
			                    FROM ]' || in_sta_table || q'[
			                   WHERE «isDeleted» IS NULL
			               ),
			               valid AS (
			                  -- filter invalid periods, e.g. produced by truncation
			                  SELECT «FOR col : model.columnNames.filter[it != histId] 
			                          SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			                    FROM active
			                   WHERE «validFrom» < «validTo»
			                      OR «validFrom» IS NULL AND «validTo» IS NOT NULL
			                      OR «validFrom» IS NOT NULL AND «validTo» IS NULL
			                      OR «validFrom» IS NULL AND «validTo» IS NULL
			               ),
			               merged AS (
			                  -- merge periods with identical column values into a single row
			                  SELECT «validFrom»,
			                         LAG («validTo», 1, NULL) OVER (PARTITION BY «
			                            FOR col : model.pkColumnNames 
			                         	SEPARATOR ", "»«col»«ENDFOR» ORDER BY «validFrom» NULLS FIRST) AS «gapStart»,
			                         «validTo»,
			                         LEAD («validFrom», 1, NULL) OVER (PARTITION BY «
			                         	FOR col : model.pkColumnNames 
			                         	SEPARATOR ", "»«col»«ENDFOR» ORDER BY «validFrom» NULLS FIRST) AS «gapEnd»,
			                         «FOR col : model.columnNames.filter[
			                         	it != validFrom && it != validTo && it != histId
			                         ] SEPARATOR ","»
			                         	«col»
			                         «ENDFOR»
			                    FROM valid
			                         MATCH_RECOGNIZE (
			                            PARTITION BY «FOR col : model.columnNames.filter[it != validFrom && it != validTo && it != histId] 
			                                          SEPARATOR ", "»«col»«ENDFOR»
			                            ORDER BY «validFrom» NULLS FIRST
			                            MEASURES FIRST(«validFrom») AS «validFrom», LAST(«validTo») AS «validTo»
			                            ONE ROW PER MATCH
			                            PATTERN ( strt nxt* )
			                            DEFINE nxt AS «validFrom» = PREV(«validTo»)
			                         )
			               ),
			               combined AS (
			                  -- active periods
			                  SELECT «validFrom»,
			                         «validTo»,
			                         NULL AS «isDeleted»,
			                         «FOR col : model.columnNames.filter[
			                         	it != validFrom && it != validTo && it != isDeleted && it != histId
			                         ] SEPARATOR ","»
			                         	«col»
			                         «ENDFOR»
			                    FROM merged
			                  UNION ALL
			                  -- deleted start periods
			                  SELECT «gapStart» AS «validFrom»,
			                         «validFrom» as «validTo»,
			                         1 AS «isDeleted»,
			                         «FOR col : model.columnNames.filter[
			                         	it != validFrom && it != validTo && it != isDeleted && it != histId
			                         ] SEPARATOR ","»
			                         	«col»
			                         «ENDFOR»
			                    FROM merged
			                   WHERE «validFrom» IS NOT NULL AND «gapStart» IS NULL
			                  UNION ALL
			                  -- deleted non-starting periods
			                  SELECT «validTo» AS «validFrom»,
			                         «gapEnd» as «validTo»,
			                         1 AS «isDeleted»,
			                         «FOR col : model.columnNames.filter[
			                         	it != validFrom && it != validTo && it != isDeleted && it != histId
			                         ] SEPARATOR ","»
			                         	«col»
			                         «ENDFOR»
			                    FROM merged
			                   WHERE «validTo» != «gapEnd» 
			                      OR «validTo» IS NULL AND «gapEnd» IS NOT NULL
			                      OR «validTo» IS NOT NULL AND «gapEnd» IS NULL
			               )
			            -- main
			            SELECT «FOR col : model.columnNames.filter[it != histId] 
			                    SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			              FROM combined
			         ]';
			         print_lines(
			            in_proc  => 'init_load.do',
			            in_level => co_trace, 
			            in_lines => l_stmt
			         );
			         EXECUTE IMMEDIATE l_stmt;
			         print_line(
			            in_proc  => 'init_load.do',
			            in_level => co_debug,
			            in_line  => SQL%ROWCOUNT || ' rows inserted.'
			         );
			      END do;
			   BEGIN
			      print_line(in_proc => 'init_load', in_level => co_info, in_line => 'started.');
			      check_table_prerequisites(
			         in_owner => in_owner,
			         in_sta_table => in_sta_table,
			         in_log_table => in_log_table
			      );
			      check_tables_empty;
			      check_reject_limit (in_reject_limit => in_reject_limit);
			      disable_fk_constraints;
			      drop_uk_constraints;
			      do;
			      enable_fk_constraints;
			      create_uk_constraint;
			      print_line(in_proc => 'init_load', in_level => co_info, in_line => 'completed.');
			   END init_load;

			   --
			   -- delta_load
			   --
			   PROCEDURE delta_load (
			      in_owner        IN VARCHAR2 DEFAULT USER,
			      in_sta_table    IN VARCHAR2 DEFAULT '«model.stagingTableName.toUpperCase»',
			      in_log_table    IN VARCHAR2 DEFAULT '«model.loggingTableName.toUpperCase»',
			      in_reject_limit IN VARCHAR2 DEFAULT '0'
			   ) IS
			      l_start_at TIMESTAMP(6) := SYSTIMESTAMP;
			      --
			      -- delta_load.log_error
			      --
			      PROCEDURE log_error (
			         in_row             IN «model.objectTypeName»,
			         in_ora_err_number$ IN NUMBER,
			         in_ora_err_mesg$   IN VARCHAR2,
			         in_ora_err_rowid$  IN VARCHAR2, -- using UROWID causes error during insert on 12.1.2.0
			         in_ora_err_optyp$  IN VARCHAR2
			      ) IS
			         PRAGMA AUTONOMOUS_TRANSACTION;
			         l_log_err_stmt CLOB;
			      BEGIN
			         l_log_err_stmt := q'[
			            INSERT INTO ]' || in_log_table || q'[ (
			                           ora_err_number$,
			                           ora_err_mesg$,
			                           ora_err_rowid$,
			                           ora_err_optyp$,
			                           ora_err_tag$,
			                           «FOR col : model.columnNames SEPARATOR ","»
			                           	«col»
			                           «ENDFOR»
			                        )
			                 VALUES (
			                           :ora_err_number$,
			                           :ora_err_mesg$,
			                           CHARTOROWID(:ora_err_rowid$),
			                           :ora_err_optyp$,
			                           :ora_err_tag$,
			                           «FOR col : model.columnNames SEPARATOR ","»
			                           	:«col»
			                           «ENDFOR»
			                        )
			         ]';
			         EXECUTE IMMEDIATE l_log_err_stmt
			                     USING IN in_ora_err_number$,
			                           IN in_ora_err_mesg$,
			                           IN in_ora_err_rowid$,
			                           IN in_ora_err_optyp$,
			                           «model.getErrTagExpr("delta_load", "in_sta_table", "l_start_at")»,
			                           «FOR col : model.columnNames 
			                            SEPARATOR "," + System.lineSeparator»IN in_row.«col»«ENDFOR»;
			         COMMIT;
			      END log_error;
			      --
			      -- delta_load.do
			      --
			      PROCEDURE do IS
			         l_stmt         CLOB;
			         c_sta          SYS_REFCURSOR;
			         l_ok           PLS_INTEGER := 0;
			         l_nok          PLS_INTEGER := 0;
			         l_reject_limit PLS_INTEGER := 2**31-1;
			         l_new_row      «model.objectTypeName»;
			         l_old_row      «model.objectTypeName»;
			         l_«operation»  VARCHAR2(1 CHAR);
			         l_rowid        VARCHAR2(100 CHAR);
			      BEGIN
			         IF UPPER(in_reject_limit) != 'UNLIMITED' THEN
			            l_reject_limit := TO_NUMBER(in_reject_limit);
			         END IF;
			         l_stmt := q'[
			            WITH
			               truncated AS (
			                  -- truncate period columns if granularity is different to the data type default
			                  «IF model.granularityRequiresTruncation»
			                  	SELECT TRUNC(«validFrom», '«model.granuarityTruncationFormat»') AS «validFrom»,
			                  	       TRUNC(«validTo», '«model.granuarityTruncationFormat»') AS «validTo»,
			                  «ELSE»
			                  	SELECT «validFrom»,
			                  	       «validTo»,
			                  «ENDIF»
			                         «FOR col : model.columnNames.filter[
			                         	it != histId && it != validFrom && it != validTo
			                         ] SEPARATOR ","»
			                          «col»
			                         «ENDFOR»
			                    FROM ]' || in_sta_table || q'[
			               ),
			               sta AS (
			                  -- filter invalid periods, e.g. produced by truncation
			                  SELECT «FOR col : model.columnNames.filter[it != histId] 
			                          SEPARATOR ',' + System.lineSeparator + '       '»«col»«ENDFOR»
			                    FROM truncated
			                   WHERE «validFrom» < «validTo»
			                      OR «validFrom» IS NULL AND «validTo» IS NOT NULL
			                      OR «validFrom» IS NOT NULL AND «validTo» IS NULL
			                      OR «validFrom» IS NULL AND «validTo» IS NULL
			               )
			            -- main
			            SELECT «model.objectTypeName» (
			                      «histId»,
			                      «FOR col : model.allColumnNames.filter[it != histId] 
			                       SEPARATOR ","»
			                      	sta.«col»
			                      «ENDFOR»
			                   ) new_row,
			                   «model.objectTypeName» (
			                      «histId»,
			                      «FOR col : model.allColumnNames.filter[it != histId] 
			                       SEPARATOR ","»
			                      	ht.«col»
			                      «ENDFOR»
			                   ) old_row,
			                   sta.ROWID,
			                   CASE
			                      WHEN sta.«isDeleted» = 1 THEN
			                         'D'
			                      WHEN «FOR col : model.pkColumnNames 
			                            SEPARATOR ' AND '»ht.«col» IS NULL«ENDFOR» THEN
			                         'I'
			                      ELSE
			                         'U'
			                    END AS «operation»
			              FROM sta
			              LEFT JOIN «model.historyTableName» ht
			                ON «FOR col : model.pkColumnNames
			                    SEPARATOR System.lineSeparator + '   AND '»ht.«col» = sta.«col»«ENDFOR»
			             WHERE ( -- overlapping periods
			                     -- sta      |--------|
			                     -- ht           |--------|
			                      NVL(ht.«validFrom», «minDate») < NVL(sta.«validTo», «maxDate»)
			                      AND NVL(ht.«validTo», «maxDate») > NVL(sta.«validTo», «maxDate»)
			                     -- sta      |--------|
			                     -- ht   |--------|
			                      OR NVL(ht.«validTo», «maxDate») > NVL(sta.«validFrom», «minDate») 
			                      AND NVL(ht.«validFrom», «minDate») < NVL(sta.«validFrom», «minDate»)
			                     -- sta      |--------|
			                     -- ht   |...----------...|
			                      OR NVL(ht.«validFrom», «minDate») <= NVL(sta.«validFrom», «minDate»)
			                      AND NVL(ht.«validTo», «maxDate») >= NVL(sta.«validTo», «maxDate»)
			                     -- sta  |...----------...|
			                     -- ht       |--------|
			                      OR NVL(ht.«validFrom», «minDate») >= NVL(sta.«validFrom», «minDate»)
			                      AND NVL(ht.«validTo», «maxDate») <= NVL(sta.«validTo», «maxDate»)
			                   )
			               AND ( -- changed column values
			                     «FOR col : model.updateableLatestColumnNames 
			                      SEPARATOR System.lineSeparator + 'OR '»
			                      (
			                         sta.«col» != ht.«col»
			                         OR sta.«col» IS NULL AND ht.«col» IS NOT NULL
			                         OR sta.«col» IS NOT NULL AND ht.«col» IS NULL
			                      )
			                     «ENDFOR»
			                   )
			                OR ( -- new periods
			                      «FOR col : model.pkColumnNames 
			                       SEPARATOR System.lineSeparator + 'AND '»ht.«col» IS NULL«ENDFOR»
			                   )
			         ]';
			         print_lines(
			            in_proc  => 'delta_load.do',
			            in_level => co_trace, 
			            in_lines => l_stmt
			         );
			         OPEN c_sta FOR l_stmt;
			         <<all_updates>>
			         LOOP
			            FETCH c_sta INTO l_new_row, l_old_row, l_rowid, l_«operation»;
			            EXIT all_updates WHEN c_sta%NOTFOUND;
			            <<dml_trap>>
			            BEGIN
			               CASE l_«operation» 
			                  WHEN 'I' THEN
			                     ins(in_new_row => l_new_row);
			                  WHEN 'U' THEN
			                     upd(in_new_row => l_new_row, in_old_row => l_old_row);
			                  WHEN 'D' THEN
			                     -- delete the period according new row, might produce new period(s)
			                     del(in_old_row => l_new_row);
			               END CASE;
			               l_ok := l_ok + 1;
			            EXCEPTION
			               WHEN OTHERS THEN
			                  l_nok := l_nok + 1;
			                  log_error (
			                     in_row             => l_new_row,
			                     in_ora_err_number$ => SQLCODE,
			                  	 in_ora_err_mesg$   => SQLERRM,
			                  	 in_ora_err_rowid$  => l_rowid,
			                  	 in_ora_err_optyp$  => l_«operation»
			                  );
			                  IF l_nok >= l_reject_limit THEN
			                     CLOSE c_sta;
			                     raise_application_error(«errorNumber», 'Limit of ' 
			                        || l_reject_limit || ' errors reached. See '
			                        || in_log_table || ' for details.');
			                  END IF;
			            END dml_trap;
			         END LOOP all_updates;
			         CLOSE c_sta;
			         print_line(
			            in_proc  => 'delta_load.do',
			            in_level => co_info,
			            in_line  => l_ok || ' temporal DML operations completed successfully.'
			         );
			         print_line(
			            in_proc  => 'delta_load.do',
			            in_level => co_info,
			            in_line  => l_nok || ' temporal DML operations failed. See '
			                           || in_log_table || ' for details.'
			         );
			      END do;
			   BEGIN
			      print_line(in_proc => 'delta_load', in_level => co_info, in_line => 'started.');
			      check_table_prerequisites(
			         in_owner => in_owner,
			         in_sta_table => in_sta_table,
			         in_log_table => in_log_table
			      );
			      check_reject_limit (in_reject_limit => in_reject_limit);
			      do;
			      print_line(in_proc => 'delta_load', in_level => co_info, in_line => 'completed.');
			   END delta_load;

			   «ELSE»

			   --
			   -- do_ins
			   --
			   PROCEDURE do_ins (
			      io_row IN OUT «model.objectTypeName»
			   ) IS
			   BEGIN
			      INSERT INTO «model.latestTableName» (
			                     «FOR col : model.columnNames SEPARATOR ","»
			                     	«col.toLowerCase»
			                     «ENDFOR»
			                  )
			           VALUES (
			                     «FOR col : model.columnNames SEPARATOR ","»
			                        io_row.«col.toLowerCase»
			                     «ENDFOR»
			                  )
			        RETURNING «FOR col : model.pkColumnNames SEPARATOR ', '»«col»«ENDFOR»
			             INTO «FOR col : model.pkColumnNames SEPARATOR ', '»io_row.«col»«ENDFOR»;
			      print_line(
			         in_proc  => 'do_ins', 
			         in_level => co_debug, 
			         in_line  => SQL%ROWCOUNT || ' rows inserted.'
			      );
			   END do_ins;

			   --
			   -- do_upd
			   --
			   PROCEDURE do_upd (
			      io_new_row IN OUT «model.objectTypeName»,
			      in_old_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      UPDATE «model.latestTableName»
			         SET «FOR col : model.columnNames 
			              SEPARATOR ', ' + System.lineSeparator + '    '»«col» = io_new_row.«col»«ENDFOR»
			       WHERE «FOR col : model.pkColumnNames 
			              SEPARATOR System.lineSeparator + '  AND '»«col» = in_old_row.«col»«ENDFOR»
			         AND (
			                 «FOR col : model.updateableLatestColumnNames SEPARATOR " OR"»
			                 	(«col» != io_new_row.«col» OR «
			                 	col» IS NULL AND io_new_row.«col» IS NOT NULL OR «
			                 	col» IS NOT NULL AND io_new_row.«col» IS NULL)
			                 «ENDFOR»
			             );
			      print_line(
			         in_proc  => 'do_upd', 
			         in_level => co_debug, 
			         in_line  => SQL%ROWCOUNT || ' rows updated.'
			      );
			   END do_upd;

			   --
			   -- do_del
			   --
			   PROCEDURE do_del (
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      DELETE 
			        FROM «model.latestTableName»
			       WHERE «FOR col : model.pkColumnNames 
			              SEPARATOR System.lineSeparator + '   AND '»«col» = in_row.«col»«ENDFOR»;
			      print_line(
			         in_proc  => 'do_del', 
			         in_level => co_debug, 
			         in_line  => SQL%ROWCOUNT || ' rows deleted.'
			      );
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
			      print_line(in_proc => 'ins', in_level => co_info, in_line => 'started.');
			      l_new_row := in_new_row;
			      <<pre_ins>>
			      BEGIN
			         «model.hookPackageName».pre_ins(io_new_row => l_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END pre_ins;
			      do_ins(io_row => l_new_row);
			      <<post_ins>>
			      BEGIN
			         «model.hookPackageName».post_ins(in_new_row => l_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END post_ins;
			      print_line(in_proc => 'ins', in_level => co_info, in_line => 'completed.');
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
			      print_line(in_proc => 'upd', in_level => co_info, in_line => 'started.');
			      l_new_row := in_new_row;
			      <<pre_upd>>
			      BEGIN
			         «model.hookPackageName».pre_upd(io_new_row => l_new_row, in_old_row => in_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END pre_upd;
			      do_upd(io_new_row => l_new_row, in_old_row => in_old_row);
			      <<post_upd>>
			      BEGIN
			         «model.hookPackageName».post_upd(in_new_row => l_new_row, in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END post_upd;
			      print_line(in_proc => 'upd', in_level => co_info, in_line => 'completed.');
			   END upd;

			   --
			   -- del
			   --
			   PROCEDURE del (
			      in_old_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      print_line(in_proc => 'del', in_level => co_info, in_line => 'started.');
			      <<pre_del>>
			      BEGIN
			         «model.hookPackageName».pre_del(in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END pre_del;
			      do_del(in_row => in_old_row);
			      <<post_del>>
			      BEGIN
			         «model.hookPackageName».post_del(in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END post_del;
			      print_line(in_proc => 'del', in_level => co_info, in_line => 'completed.');
			   END del;

			   --
			   -- set_debug_output
			   --
			   PROCEDURE set_debug_output (
			      in_level IN dbms_output_level_type DEFAULT co_off
			   ) IS
			   BEGIN
			      g_debug_output_level := in_level;
			   END set_debug_output;

			END «model.apiPackageName»;
			/
		«ENDIF»
	'''
}
