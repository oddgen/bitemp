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
			cols.add(model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase)
			cols.add(model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase)
			cols.add(BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase)
		}
		for (col : model.inputTable.columns.values.filter [it.virtualColumn == "NO" && !cols.contains(it.columnName)]) {
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
			
			   --
			   -- Declarations to handle 'ORA-06508: PL/SQL: could not find program unit being called: "«model.conn.metaData.userName».«model.hookPackageName.toUpperCase»"'
			   --
			   e_hook_body_missing EXCEPTION;
			   PRAGMA exception_init(e_hook_body_missing, -6508);
			
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
			      	-- TODO temporal insert
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
