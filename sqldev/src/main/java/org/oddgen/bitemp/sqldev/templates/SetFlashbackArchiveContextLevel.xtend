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
import org.oddgen.bitemp.sqldev.resources.BitempResources
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class SetFlashbackArchiveContextLevel {

	/*
	 * The the context_level is stored in SYS_FBA_CONTEXT_LIST (namespace FBA_CONTEXT) 
	 * of the CDB. It is valid for all containers, but the transactions are stored per 
	 * container in table SYS_FBA_CONTEXT_AUD in the SYSTEM tablespace.
	 * 
	 * dbms_flashback_archive.purge_context deletes all entries in SYS_FBA_CONTEXT_AUD.
	 * 
	 * dbms_flashback_archive.get_sys_context retrieves an attribute per XID, but the 
	 * XID must not be NULL, otherwise an "ORA-01405: fetched column value is NULL" 
	 * is thrown. Passing non-existent XID values will return NULL. Use it as follows:
	 *    CASE 
	 *       WHEN versions_xid IS NOT NULL THEN 
	 *          dbms_flashback_archive.get_sys_context(versions_xid, 'USERENV','OS_USER')
	 *    END
	 */
	def compile(
		GeneratorModel model) '''
		«IF model.targetModel == ApiType.UNI_TEMPORAL_TRANSACTION_TIME || model.targetModel == ApiType.BI_TEMPORAL»
			«IF model.params.get(BitempRemodeler.FLASHBACK_ARCHIVE_CONTEXT_LEVEL) != BitempResources.getString("PREF_CONTEXT_LEVEL_KEEP")»
				--
				-- Configure storage of SYS_CONTEXT attributes in SYS_FBA_CONTEXT_AUD per transaction (XID)
				--
				BEGIN
				   «IF model.params.get(BitempRemodeler.FLASHBACK_ARCHIVE_CONTEXT_LEVEL) == BitempResources.getString("PREF_CONTEXT_LEVEL_ALL")»
				   	dbms_flashback_archive.set_context_level(level => 'ALL');
				   «ELSEIF model.params.get(BitempRemodeler.FLASHBACK_ARCHIVE_CONTEXT_LEVEL) == BitempResources.getString("PREF_CONTEXT_LEVEL_TYPICAL")»
				   	dbms_flashback_archive.set_context_level(level => 'TYPICAL');
				   «ELSE»
				   	dbms_flashback_archive.set_context_level(level => 'NONE');
				   «ENDIF»
				END;
				/
			«ENDIF»
		«ENDIF»
	'''
}
