/*
//
// Licensed to Benedikt Kämpgen under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Benedikt Kämpgen licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package org.olap4j.driver.ld;

import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.driver.ld.LdOlap4jCatalog;
import org.olap4j.driver.ld.LdOlap4jDatabase;
import org.olap4j.driver.ld.LdOlap4jSchema;
import org.olap4j.impl.Named;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;

/**
 * Implementation of {@link org.olap4j.metadata.Catalog} for XML/A providers.
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jCatalog.java 455 2011-05-24 10:01:26Z jhyde $
 * @since May 23, 2007
 */
class LdOlap4jCatalog implements Catalog, Named {
	final LdOlap4jDatabaseMetaData olap4jDatabaseMetaData;
	private final String name;
	final DeferredNamedListImpl<LdOlap4jSchema> schemas;
	private final LdOlap4jDatabase database;

	LdOlap4jCatalog(LdOlap4jDatabaseMetaData olap4jDatabaseMetaData,
			LdOlap4jDatabase database, String name) {
		this.database = database;
		assert olap4jDatabaseMetaData != null;
		assert name != null;
		this.olap4jDatabaseMetaData = olap4jDatabaseMetaData;
		this.name = name;

		this.schemas = new DeferredNamedListImpl<LdOlap4jSchema>(
				LdOlap4jConnection.MetadataRequest.DBSCHEMA_SCHEMATA,
				new LdOlap4jConnection.Context(
						olap4jDatabaseMetaData.olap4jConnection,
						olap4jDatabaseMetaData, this, null, null, null, null,
						null), new LdOlap4jConnection.CatalogSchemaHandler(
						this.name), null);
		LdOlap4jSchema pop = schemas.get(0);
	}

	public int hashCode() {
		return name.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj instanceof LdOlap4jCatalog) {
			LdOlap4jCatalog that = (LdOlap4jCatalog) obj;
			return this.name.equals(that.name);
		}
		return false;
	}

	public NamedList<Schema> getSchemas() throws OlapException {
		LdOlap4jUtil._log.info("getSchemas()...");

		// ResultSet cubes = olap4jDatabaseMetaData.getCubes(null, null, null);
		// ResultSet dimensions = olap4jDatabaseMetaData.getDimensions(null,
		// null, null, null);

		// XmlaOlap4jConnection olapConnection = (XmlaOlap4jConnection)
		// database.getOlapConnection();
		// return
		// olapConnection.myOlapServer.getOlapConnection().getOlapSchemas();
		return Olap4jUtil.cast(this.schemas);
	}

	public String getName() {
		return name;
	}

	public OlapDatabaseMetaData getMetaData() {
		return olap4jDatabaseMetaData;
	}

	public LdOlap4jDatabase getDatabase() {
		return database;
	}
}

// End XmlaOlap4jCatalog.java

