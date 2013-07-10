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
package org.olap4j.driver.olap4ld;

import java.sql.SQLException;

import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldCatalog;
import org.olap4j.driver.olap4ld.Olap4ldDatabase;
import org.olap4j.driver.olap4ld.Olap4ldSchema;
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
class Olap4ldCatalog implements Catalog, Named {
	final Olap4ldDatabaseMetaData olap4jDatabaseMetaData;
	private final String name;
	final DeferredNamedListImpl<Olap4ldSchema> schemas;
	private final Olap4ldDatabase database;

	Olap4ldCatalog(Olap4ldDatabaseMetaData olap4jDatabaseMetaData,
			Olap4ldDatabase database, String name) {
		this.database = database;
		assert olap4jDatabaseMetaData != null;
		assert name != null;
		this.olap4jDatabaseMetaData = olap4jDatabaseMetaData;
		this.name = name;

		this.schemas = new DeferredNamedListImpl<Olap4ldSchema>(
				Olap4ldConnection.MetadataRequest.DBSCHEMA_SCHEMATA,
				new Olap4ldConnection.Context(
						olap4jDatabaseMetaData.olap4jConnection,
						olap4jDatabaseMetaData, this, null, null, null, null,
						null), new Olap4ldConnection.CatalogSchemaHandler(
						this.name), null);
		//Olap4ldSchema pop = schemas.get(0);
	}

	public int hashCode() {
		return name.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj instanceof Olap4ldCatalog) {
			Olap4ldCatalog that = (Olap4ldCatalog) obj;
			return this.name.equals(that.name);
		}
		return false;
	}

	public NamedList<Schema> getSchemas() throws OlapException {
		Olap4ldUtil._log.info("getSchemas()...");

		// Set back
		try {
			Olap4ldUtil._log.info("rollback OlapConnection...");
			database.getOlapConnection().rollback();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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

	public Olap4ldDatabase getDatabase() {
		return database;
	}
}

// End XmlaOlap4jCatalog.java

