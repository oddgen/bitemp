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
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateApiPackageBody {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(GeneratorModel model) '''
		«val apiName = model.baseTableName.toLowerCase + model.params.get(BitempRemodeler.API_PACKAGE_SUFFIX).toLowerCase»
		«val hookName = model.baseTableName.toLowerCase + model.params.get(BitempRemodeler.HOOK_PACKAGE_SUFFIX).toLowerCase»
		«val otName = model.baseTableName.toLowerCase + model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»
		«IF model.inputTable.exists»
			--
			-- Create API package body
			--
			CREATE OR REPLACE PACKAGE BODY «apiName» AS
			
			   --
			   -- Declarations to handle 'ORA-06508: PL/SQL: could not find program unit being called: "«hookName»"'
			   --
			   e_hook_body_missing EXCEPTION;
			   PRAGMA exception_init(e_hook_body_missing, -6508);
			
			   --
			   -- ins
			   --
			   PROCEDURE ins (
			      in_new_row «otName»
			   ) IS
			   BEGIN
			      <<trap_pre_ins>>
			      BEGIN
			         «hookName».pre_ins(in_new_row => in_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_ins;
			      -- TODO: insert
			      <<trap_post_ins>>
			      BEGIN
			         «hookName».post_ins(in_new_row => in_new_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_post_ins;
			   END ins;

			   --
			   -- upd
			   --
			   PROCEDURE upd (
			      in_new_row «otName»,
			      in_old_row «otName»
			   ) IS
			   BEGIN
			      <<trap_pre_upd>>
			      BEGIN
			         «hookName».pre_upd(
			            in_new_row => in_new_row,
			            in_old_row => in_new_row
			         );
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_upd;
			      -- TODO: update
			      <<trap_post_upd>>
			      BEGIN
			         «hookName».post_upd(
			            in_new_row => in_new_row,
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
			      in_old_row «otName»
			   ) IS
			   BEGIN
			      <<trap_pre_del>>
			      BEGIN
			         «hookName».pre_del(in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_pre_del;
			      -- TODO: delete
			      <<trap_post_del>>
			      BEGIN
			         «hookName».post_del(in_old_row => in_old_row);
			      EXCEPTION
			         WHEN e_hook_body_missing THEN
			            NULL;
			      END trap_post_del;
			   END del;

			END «apiName»;
			/
		«ENDIF»
	'''
}
