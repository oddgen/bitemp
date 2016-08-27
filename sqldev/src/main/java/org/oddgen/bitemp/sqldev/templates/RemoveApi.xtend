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
import org.oddgen.bitemp.sqldev.dal.SessionDao
import org.oddgen.bitemp.sqldev.generators.BitempRemodeler
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModel
import org.oddgen.bitemp.sqldev.model.generator.GeneratorModelTools
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class RemoveApi {
	private extension GeneratorModelTools generatorModelTools = new GeneratorModelTools

	def getFullHistoryViewName(
		GeneratorModel model) {
		val name = '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.FULL_HISTORY_VIEW_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getHistoryViewName(
		GeneratorModel model) {
		val name = '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.HISTORY_VIEW_SUFFIX).toLowerCase»'''
		return name.toString
	}
	
	def getLatestViewName(GeneratorModel model) {
		if (model.params.get(BitempRemodeler.CRUD_COMPATIBILITY_ORIGINAL_TABLE) == "1") {
			return model.getBaseTableName.toLowerCase
		} else {
			return '''«model.getBaseTableName.toLowerCase»«model.params.get(BitempRemodeler.LATEST_VIEW_SUFFIX).toLowerCase»'''
		}
	}
	
	def getApiPackageName(GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.API_PACKAGE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getHookPackageName(GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.HOOK_PACKAGE_SUFFIX).toLowerCase»'''
		return name.toString
	}
	
	def getObjectTypeName(GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.OBJECT_TYPE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def getCollectionTypeName(GeneratorModel model) {
		val name = '''«model.baseTableName.toLowerCase»«model.params.get(BitempRemodeler.COLLECTION_TYPE_SUFFIX).toLowerCase»'''
		return name.toString
	}

	def compile(GeneratorModel model) '''
		«val dao = new SessionDao(model.conn)»
		«IF dao.existsObject("VIEW", model.fullHistoryViewName.toUpperCase)»
			--
			-- DROP full history view
			--
			DROP VIEW «model.fullHistoryViewName»;
		«ENDIF»
		«IF dao.existsObject("VIEW", model.historyViewName.toUpperCase)»
			--
			-- DROP history view including instead of trigger
			--
			DROP VIEW «model.historyViewName»;
		«ENDIF»
		«IF dao.existsObject("VIEW", model.latestViewName.toUpperCase)»
			--
			-- DROP latest view including instead of trigger
			--
			DROP VIEW «model.latestViewName»;
		«ENDIF»
		«IF dao.existsObject("PACKAGE", model.apiPackageName.toUpperCase)»
			--
			-- DROP API package specification and body
			--
			DROP PACKAGE «model.apiPackageName»;
		«ENDIF»		
		«IF dao.existsObject("PACKAGE", model.hookPackageName.toUpperCase) && !dao.existsObject("PACKAGE BODY", model.hookPackageName.toUpperCase)»
			--
			-- DROP HOOK package specification (body does not exists)
			--
			DROP PACKAGE «model.hookPackageName»;
		«ENDIF»		
		«IF dao.existsObject("TYPE", model.collectionTypeName.toUpperCase)»
			--
			-- DROP collection type
			--
			DROP TYPE «model.collectionTypeName»;
		«ENDIF»		
		«IF dao.existsObject("TYPE", model.objectTypeName.toUpperCase)»
			--
			-- DROP object type
			--
			DROP TYPE «model.objectTypeName»;
		«ENDIF»		
	'''
}
