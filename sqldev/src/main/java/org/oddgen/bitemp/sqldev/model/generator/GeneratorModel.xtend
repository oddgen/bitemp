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
package org.oddgen.bitemp.sqldev.model.generator

import java.sql.Connection
import java.util.HashMap
import java.util.LinkedHashMap
import org.eclipse.xtend.lib.annotations.Accessors
import org.oddgen.bitemp.sqldev.model.AbstractModel

@Accessors
class GeneratorModel extends AbstractModel {
	LinkedHashMap<String, String> params
	HashMap<String, Boolean> paramStates
	ApiType originModel
	ApiType targetModel
	Table inputTable
	Connection conn
}
