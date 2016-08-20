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
import org.oddgen.sqldev.LoggableConstants

@Loggable(LoggableConstants.DEBUG)
class CreateApi {

	def compile(GeneratorModel model) '''
		«/*
		 * Table API
		 * - Object type
		 * - Collection type
		 * - API Package Spec
		 * - API Package Body
		 * - Hook Package Spec (do not generate Body, dedicated Generator)
		 * - Latest view
		 * - History view (using context)
		 * - Full history view
		 * - Instead-of-Trigger on latest view
		 * - Instead-of-Trigger on history view
		 * - Instead-of-Trigger on full history view
		 * 
		 */»
		«IF model.params.get(BitempRemodeler.GEN_API) == "1"»
			«(new CreateObjectType).compile(model)»
			«(new CreateApiPackageSpecification).compile(model)»
			«(new CreateHookPackageSpecification).compile(model)»
			«(new CreateApiPackageBody).compile(model)»
			«(new CreateLatestView).compile(model)»
		«ENDIF»
	'''
}
