/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.ld;

import java.util.Collections;
import java.util.List;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.driver.ld.LdOlap4jCatalog;
import org.olap4j.impl.Named;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.NamedList;

/**
 * XMLA implementation of a database metadata object.
 * 
 * @version $Id:$
 * @author LBoudreau
 */
class LdOlap4jDatabase implements Database, Named {

	private final NamedList<LdOlap4jCatalog> catalogs;
	private final LdOlap4jConnection olap4jConnection;
	private final String name;
	private final String description;
	private final String providerName;
	private final String url;
	private final String dataSourceInfo;
	private final List<ProviderType> providerType;
	private final List<AuthenticationMode> authenticationMode;

	public LdOlap4jDatabase(LdOlap4jConnection olap4jConnection,
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
		this.catalogs = new DeferredNamedListImpl<LdOlap4jCatalog>(
				LdOlap4jConnection.MetadataRequest.DBSCHEMA_CATALOGS,
				new LdOlap4jConnection.Context(olap4jConnection,
						(LdOlap4jDatabaseMetaData) olap4jConnection
								.getMetaData(), null, null, null, null, null,
						null), new LdOlap4jConnection.CatalogHandler(this),
				null);
		LdOlap4jCatalog pop = catalogs.get(0);
	}

	public List<AuthenticationMode> getAuthenticationModes()
			throws OlapException {
		return authenticationMode;
	}

	public NamedList<Catalog> getCatalogs() throws OlapException {
		// TODO: We could give the catalogs, here.
		LdOlap4jUtil._log.info("getCatalogs()...");
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

