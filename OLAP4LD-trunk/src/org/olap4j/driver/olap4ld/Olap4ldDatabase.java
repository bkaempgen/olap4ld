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

import java.util.Collections;
import java.util.List;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldCatalog;
import org.olap4j.impl.Named;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.NamedList;

/**
 * XMLA implementation of a database metadata object.
 * 
 * @version $Id:$
 * @author LBoudreau, bkaempgen
 */
class Olap4ldDatabase implements Database, Named {

	private final NamedList<Olap4ldCatalog> catalogs;
	private final Olap4ldConnection olap4jConnection;
	private final String name;
	private final String description;
	private final String providerName;
	private final String url;
	private final String dataSourceInfo;
	private final List<ProviderType> providerType;
	private final List<AuthenticationMode> authenticationMode;

	public Olap4ldDatabase(Olap4ldConnection olap4jConnection,
			String name, String description, String providerName, String url,
			String dataSourceInfo, List<ProviderType> providerType,
			List<AuthenticationMode> authenticationMode) {
		this.olap4jConnection = olap4jConnection;
		this.name = name;
		this.description = description;
		this.providerName = providerName;
		this.url = url;
		this.dataSourceInfo = dataSourceInfo;
		this.providerType = Collections.unmodifiableList(providerType);
		this.authenticationMode = Collections
				.unmodifiableList(authenticationMode);

		// Let the catalogs get created if they are needed
		this.catalogs = new DeferredNamedListImpl<Olap4ldCatalog>(
				Olap4ldConnection.MetadataRequest.DBSCHEMA_CATALOGS,
				new Olap4ldConnection.Context(olap4jConnection,
						(Olap4ldDatabaseMetaData) olap4jConnection
								.getMetaData(), null, null, null, null, null,
						null), new Olap4ldConnection.CatalogHandler(this),
				null);
		//Olap4ldCatalog pop = catalogs.get(0);
	}

	public List<AuthenticationMode> getAuthenticationModes()
			throws OlapException {
		return authenticationMode;
	}

	public NamedList<Catalog> getCatalogs() throws OlapException {
		// TODO: We could give the catalogs, here.
		Olap4ldUtil._log.info("getCatalogs()...");
		return Olap4jUtil.cast(catalogs);
	}

	public String getDescription() throws OlapException {
		return this.description;
	}

	public String getName() {
		return this.name;
	}

	public OlapConnection getOlapConnection() {
		return this.olap4jConnection;
	}

	public String getProviderName() throws OlapException {
		return this.providerName;
	}

	public List<ProviderType> getProviderTypes() throws OlapException {
		return this.providerType;
	}

	public String getURL() throws OlapException {
		return this.url;
	}

	public String getDataSourceInfo() throws OlapException {
		return this.dataSourceInfo;
	}
}
// End XmlaOlap4jDatabase.java

