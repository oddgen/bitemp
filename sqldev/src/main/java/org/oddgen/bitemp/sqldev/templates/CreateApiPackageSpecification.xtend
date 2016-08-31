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
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateApiPackageSpecification {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def compile(GeneratorModel model) '''
		«IF model.inputTable.exists»
			--
			-- Create API package specification
			--
			«val tableName = model.inputTable.getNewTableName(model).toLowerCase»
			«val histTableName = if (model.isTemporalValidity) model.newHistTable.getNewTableName(model).toLowerCase»
			CREATE OR REPLACE PACKAGE «model.apiPackageName» AS
			   /** 
			   * «model.targetModel.apiTypeToString.toFirstUpper» API for table «tableName»«IF model.isTemporalValidity» (including history table «histTableName»)«ENDIF»
			   * generated by «BitempResources.get("EXTENSION_NAME")».
			   *
			   * @headcom
			   */
			
			   /**
			   * Insert into «model.targetModel.apiTypeToString» table «tableName».
			   *
			   * @param in_new_row new Row to be inserted
			   */
			   PROCEDURE ins (
			      in_new_row IN «model.objectTypeName»
			   );

			   /**
			   * Update «model.targetModel.apiTypeToString» table «tableName».
			   *
			   * @param in_new_row IN Row with updated column values
			   * @param in_old_row IN Row with original column values
			   */
			   PROCEDURE upd (
			      in_new_row IN «model.objectTypeName»,
			      in_old_row IN «model.objectTypeName»
			   );

			   /**
			   * Delete from «model.targetModel.apiTypeToString» table «tableName».
			   «IF model.isTemporalValidity»
			   * Please note that instead of a physical delete operation the column «BitempRemodeler.IS_DELETED_COL_NAME.toLowerCase» is set to 1.
			   «ENDIF»
			   *
			   * @param in_old_row Row with original column values. 
			   *                   For the core delete operation the following columns are relevant
			   «FOR col : model.inputTable.primaryKeyConstraint.columnNames»
			   *                      - «col.toLowerCase» - primary key to identify object for the delete operation
			   «ENDFOR»
			   «IF model.isTemporalValidity»
			   *                      - «model.params.get(BitempRemodeler.VALID_FROM_COL_NAME).toLowerCase» - start of the period to be marked as deleted
			   *                      - «model.params.get(BitempRemodeler.VALID_TO_COL_NAME).toLowerCase» - end of the period to be marked as deleted
			   «ENDIF»
			   *                   All other columns are provided for the use within pre- and post-delete hooks.
			   */
			   PROCEDURE del (
			      in_old_row IN «model.objectTypeName»
			   );

			END «model.apiPackageName»;
			/
		«ENDIF»
	'''
}
