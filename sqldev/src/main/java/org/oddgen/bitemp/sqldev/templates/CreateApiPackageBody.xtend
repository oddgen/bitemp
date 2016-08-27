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
			   -- get_versions
			   --
			   FUNCTION get_versions (
			      in_row IN «model.objectTypeName»
			   ) return «model.collectionTypeName» IS
			      l_versions «model.collectionTypeName»;
			   BEGIN
			      SELECT «model.objectTypeName» (
			                «FOR col : model.columnNames SEPARATOR ','»
			                	«col»
			                «ENDFOR»
			             )
			        BULK COLLECT INTO l_versions
			        FROM «model.historyTableName» AS OF SCN SYS.dbms_flashback.get_system_change_number «
			             »VERSIONS PERIOD FOR «BitempRemodeler.VALID_TIME_PERIOD_NAME.toLowerCase» BETWEEN MINVALUE AND MAXVALUE
			       WHERE «FOR col : model.pkColumnNames SEPARATOR System.lineSeparator + '  AND '»«col» = in_row.«col»«ENDFOR»;
			      RETURN l_versions;
			   END get_versions;

			   --
			   -- handle_predecessor
			   --
			   PROCEDURE handle_predecessor (
			      io_versions IN OUT «model.collectionTypeName»,
			      in_row IN «model.objectTypeName»
			   ) IS
			      i PLS_INTEGER;
			   BEGIN
			      IF io_versions IS NOT NULL AND io_versions.count() > 0 THEN
			         IF in_row.«validFrom» IS NOT NULL THEN
			            -- reduce validity of immediate predecessor
			            i := io_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF (io_versions(i).«validFrom» IS NULL OR io_versions(i).«validFrom» <= in_row.«validFrom»)
			                  AND (io_versions(i).«validTo» IS NULL OR io_versions(i).«validTo» > in_row.«validFrom»)
			               THEN
			                  io_versions(i).«validTo» := in_row.«validFrom»;
			               END IF;
			               i := io_versions.next(i);
			            END LOOP;
			         ELSE
			            -- delete all predecessors
			            i := io_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF io_versions(i).«validTo» < in_row.«validTo» OR in_row.«validTo» IS NULL THEN
			                  io_versions.delete(i);
			               END IF;
			               i := io_versions.next(i);
			            END LOOP;
			         END IF;
			      END IF;
			   END handle_predecessor;

			   --
			   -- handle_successor
			   --
			   PROCEDURE handle_successor (
			      io_versions IN OUT «model.collectionTypeName»,
			      in_row IN «model.objectTypeName»
			   ) IS
			      i PLS_INTEGER;
			   BEGIN
			      IF io_versions IS NOT NULL AND io_versions.count() > 0 THEN
			         IF in_row.«validTo» IS NOT NULL THEN
			            -- reduce validity of immediate successor
			            i := io_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF (io_versions(i).«validFrom» IS NULL OR io_versions(i).«validFrom» <= in_row.«validTo»)
			                  AND (io_versions(i).«validTo» IS NULL OR io_versions(i).«validTo» > in_row.«validTo»)
			               THEN
			                  io_versions(i).«validFrom» := in_row.«validTo»;
			               END IF;
			               i := io_versions.next(i);
			            END LOOP;
			         ELSE
			            -- delete all successors
			            i := io_versions.first();
			            WHILE i IS NOT NULL LOOP
			               IF io_versions(i).«validFrom» > in_row.«validFrom» OR in_row.«validFrom» IS NULL THEN
			                  io_versions.delete(i);
			               END IF;
			               i := io_versions.next(i);
			            END LOOP;
			         END IF;
			      END IF;
			   END handle_successor;

			   --
			   -- add_version
			   --
			   PROCEDURE add_version (
			      io_versions IN OUT «model.collectionTypeName»,
			      in_row IN «model.objectTypeName»
			   ) IS
			   BEGIN
			      io_versions.extend();
			      io_versions(io_versions.last()) := in_row;
			   END add_version;

			   --
			   -- save_versions
			   --
			   PROCEDURE save_versions (
			      in_versions IN «model.collectionTypeName»
			   ) IS
			   BEGIN
			      -- TODO
			      NULL;
			   END save_versions;

			   --
			   -- do_ins
			   --
			   PROCEDURE do_ins (
			      io_row IN OUT «model.objectTypeName»
			   ) IS
			      l_versions «model.collectionTypeName»;
			   BEGIN
			      truncate_to_granularity(io_row => io_row);
			      l_versions := get_versions(in_row => io_row);
			      handle_predecessor(io_versions => l_versions, in_row => io_row);
			      handle_successor(io_versions => l_versions, in_row => io_row);
			      add_version(io_versions => l_versions, in_row => io_row);
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
