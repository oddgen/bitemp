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

import java.util.LinkedHashMap
import java.util.List
import org.eclipse.xtend.lib.annotations.Accessors
import org.oddgen.bitemp.sqldev.model.AbstractModel

@Accessors
class Table extends AbstractModel {
	String tableName
	LinkedHashMap<String, Column> columns
	Boolean historyTable
	FlashbackArchiveTable flashbackArchiveTable
	List<TemporalValidityPeriod> temporalValidityPeriods
	PrimaryKeyConstraint primaryKeyConstraint
	List<ForeignKeyConstraint> foreignKeyConstraints
}
