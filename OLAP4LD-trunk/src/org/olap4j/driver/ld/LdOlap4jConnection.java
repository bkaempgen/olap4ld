/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.ld;

import static org.olap4j.driver.ld.LdOlap4jUtil.integerElement;
import static org.olap4j.driver.ld.LdOlap4jUtil.stringElement;

import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.olap4j.OlapConnection;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.PreparedOlapStatement;
import org.olap4j.Scenario;
import org.olap4j.driver.ld.proxy.XmlaOlap4jCachedProxy;
import org.olap4j.driver.ld.proxy.XmlaOlap4jProxy;
import org.olap4j.impl.ConnectStringParser;
import org.olap4j.impl.Named;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.impl.UnmodifiableArrayList;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeWriter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.mdx.parser.impl.DefaultMdxParserImpl;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database.ProviderType;
import org.olap4j.metadata.Database.AuthenticationMode;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Dimension.Type;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Measure.Aggregator;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;
import org.olap4j.metadata.Schema;
import org.semanticweb.yars.nx.Node;
import org.w3c.dom.Element;

/**
 * Implementation of {@link org.olap4j.OlapConnection} for XML/A providers.
 * 
 * <p>
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs; it is
 * instantiated using {@link Factory#newConnection}.
 * </p>
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jConnection.java 471 2011-08-03 21:46:56Z jhyde $
 * @since May 23, 2007
 */
abstract class LdOlap4jConnection implements OlapConnection {
	/**
	 * Handler for errors.
	 */
	final LdHelper helper = new LdHelper();

	/**
	 * <p>
	 * Current database.
	 */
	private LdOlap4jDatabase olap4jDatabase;

	/**
	 * <p>
	 * Current catalog.
	 */
	private LdOlap4jCatalog olap4jCatalog;

	/**
	 * <p>
	 * Current schema.
	 */
	private LdOlap4jSchema olap4jSchema;

	final LdOlap4jDatabaseMetaData olap4jDatabaseMetaData;

	// TODO: Other is different.
	// private static final String CONNECT_STRING_PREFIX = "jdbc:xmla:";
	private static final String CONNECT_STRING_PREFIX = "jdbc:ld:";

	final LdOlap4jDriver driver;

	final Factory factory;

	final XmlaOlap4jProxy proxy;

	private boolean closed = false;

	/**
	 * URL of the HTTP server to which to send XML requests.
	 */
	final XmlaOlap4jServerInfos serverInfos;

	private Locale locale;

	/**
	 * Name of the catalog to which the user wishes to bind this connection.
	 * This value can be set through the JDBC URL or via
	 * {@link LdOlap4jConnection#setCatalog(String)}
	 */
	private String catalogName;

	/**
	 * Name of the schema to which the user wishes to bind this connection to.
	 * This value can also be set through the JDBC URL or via
	 * {@link LdOlap4jConnection#setSchema(String)}
	 */
	private String schemaName;

	/**
	 * Name of the role that this connection impersonates.
	 */
	private String roleName;

	/**
	 * Name of the database to which the user wishes to bind this connection.
	 * This value can be set through the JDBC URL or via
	 * {@link LdOlap4jConnection#setCatalog(String)}
	 */
	private String databaseName;

	private boolean autoCommit;
	private boolean readOnly;

	/**
	 * Root of the metadata hierarchy of this connection.
	 */
	private final NamedList<LdOlap4jDatabase> olapDatabases;

	private final URL serverUrlObject;

	// ///////Needed for LD///////////////////
	/**
	 * The back end OLAP.
	 * 
	 * It basically allows for serialization of a Multidimensional Model and
	 * queries to it.
	 */
	// MondrianOlapServerEngine myOlapServer;

	/**
	 * The data sources from Linked Data for ETL and the domain model.
	 * 
	 * It basically allows for connecting to Linked Data, managing of resources.
	 */
	LinkedDataEngine myLinkedData;

	/**
	 * Creates an Olap4j connection an XML/A provider.
	 * 
	 * <p>
	 * This method is intentionally package-protected. The public API uses the
	 * traditional JDBC {@link java.sql.DriverManager}. See
	 * {@link org.olap4j.driver.ld.LdOlap4jDriver} for more details.
	 * 
	 * <p>
	 * Note that this constructor should make zero non-trivial calls, which
	 * could cause deadlocks due to java.sql.DriverManager synchronization
	 * issues.
	 * 
	 * @pre acceptsURL(url)
	 * 
	 * @param factory
	 *            Factory
	 * @param driver
	 *            Driver
	 * @param proxy
	 *            Proxy object which receives XML requests
	 * @param url
	 *            Connect-string URL
	 * @param info
	 *            Additional properties
	 * @throws java.sql.SQLException
	 *             if there is an error
	 */
	LdOlap4jConnection(Factory factory, LdOlap4jDriver driver,
			XmlaOlap4jProxy proxy, String url, Properties info)
			throws SQLException {

		if (!acceptsURL(url)) {
			// This is not a URL we can handle.
			// DriverManager should not have invoked us.
			throw new AssertionError("does not start with '"
					+ CONNECT_STRING_PREFIX + "'");
		}

		this.factory = factory;
		this.driver = driver;
		this.proxy = proxy;

		// Initialize Linked Data and Olap server part

		// Required for the logic below to work.
		assert url.startsWith("jdbc:ld");

		// We do not need the mondrian server anymore.
		// String newurl = url.replaceAll("jdbc:ld:", "jdbc:mondrian:");

		// This is the OLAP server
		// myOlapServer = new MondrianOlapServerEngine(driver, newurl, info);

		// fills map with databaseName, catalogName, schemaName, serverUrlObject
		final Map<String, String> map = parseConnectString(url, info);

		this.databaseName = map.get(LdOlap4jDriver.Property.DATABASE.name());

		this.catalogName = map.get(LdOlap4jDriver.Property.CATALOG.name());

		this.schemaName = map.get(LdOlap4jDriver.Property.SCHEMA.name());

		// Set URL of Triple Store (HTTP)
		final String serverUrl = map.get(LdOlap4jDriver.Property.SERVER.name());
		if (serverUrl == null) {
			throw getHelper().createException(
					"Connection property '"
							+ LdOlap4jDriver.Property.SERVER.name()
							+ "' must be specified");
		}
		try {
			this.serverUrlObject = new URL(serverUrl);
		} catch (MalformedURLException e) {
			throw getHelper().createException(e);
		}

		String datasetString = map.get(LdOlap4jDriver.Property.DATASETS.name());
		// TODO: Tokenizer using Komma ",".
		StringTokenizer urltokenizer = new StringTokenizer(datasetString, ",");
		ArrayList<String> datasets = new ArrayList<String>();
		while (urltokenizer.hasMoreTokens()) {
			datasets.add(urltokenizer.nextToken());
		}

		String dsdString = map
				.get(LdOlap4jDriver.Property.DATASTRUCTUREDEFINITIONS.name());
		// TODO: Tokenizer using Komma ",".
		StringTokenizer urltokenizer1 = new StringTokenizer(dsdString, ",");
		ArrayList<String> datastructuredefinitions = new ArrayList<String>();
		while (urltokenizer1.hasMoreTokens()) {
			datastructuredefinitions.add(urltokenizer1.nextToken());
		}
		// This is the SPARQL engine
		// Depending on the databaseName, we create a different Linked Data
		// Engine

		if (databaseName.equals("QCRUMB")) {
			myLinkedData = new OpenVirtuosoEngine(serverUrlObject,
					datastructuredefinitions, datasets, databaseName);
		}
		if (databaseName.equals("OPENVIRTUOSO")) {
			myLinkedData = new OpenVirtuosoEngine(serverUrlObject,
					datastructuredefinitions, datasets, databaseName);
		}
		if (databaseName.equals("OPENVIRTUOSORULESET")) {
			myLinkedData = new OpenVirtuosoEngine(serverUrlObject,
					datastructuredefinitions, datasets, databaseName);
		}
		if (databaseName.equals("SESAME")) {
			myLinkedData = new SesameEngine(serverUrlObject,
					datastructuredefinitions, datasets, databaseName);
		}

		// Initialize the SOAP cache if needed
		// TODO remove?
		initSoapCache(map);

		this.serverInfos = new XmlaOlap4jServerInfos() {
			private String sessionId = null;

			public String getUsername() {
				return map.get("user");
			}

			public String getPassword() {
				return map.get("password");
			}

			public URL getUrl() {
				return serverUrlObject;
			}

			public String getSessionId() {
				return sessionId;
			}

			public void setSessionId(String sessionId) {
				this.sessionId = sessionId;
			}
		};

		this.olap4jDatabaseMetaData = factory.newDatabaseMetaData(this);

		// We do not need this at the beginning, as we directly query mondrian
		// in get Databases
		// The deferred list allows us to only populate the database if needed.
		/*
		 * What is done here:
		 * 
		 * The XMLA interface is queried with discover datasources The current
		 * context only contains the databasemetadata Each row of the result is
		 * wrapped into an object and put into a list
		 */
		this.olapDatabases = new DeferredNamedListImpl<LdOlap4jDatabase>(
				LdOlap4jConnection.MetadataRequest.DISCOVER_DATASOURCES,
				new LdOlap4jConnection.Context(this,
						this.olap4jDatabaseMetaData, null, null, null, null,
						null, null),
				// With the database handler, we create a database object. This
				// is the wrapper
				// Julian has told me about
				new LdOlap4jConnection.DatabaseHandler(), null);
		LdOlap4jDatabase pop = olapDatabases.get(0);
	}

	/**
	 * Returns the error-handler
	 * 
	 * @return Error-handler
	 */
	private LdHelper getHelper() {
		return helper;
	}

	/**
	 * Initializes a cache object and configures it if cache parameters were
	 * specified in the jdbc url.
	 * 
	 * @param map
	 *            The parameters from the jdbc url.
	 * @throws OlapException
	 *             Thrown when there is an error encountered while creating the
	 *             cache.
	 */
	private void initSoapCache(Map<String, String> map) throws OlapException {
		// Test if a SOAP cache class was defined
		if (map.containsKey(LdOlap4jDriver.Property.CACHE.name().toUpperCase())) {
			// Create a properties object to pass to the proxy
			// so it can configure it's cache
			Map<String, String> props = new HashMap<String, String>();
			// Iterate over map entries to find those related to
			// the cache config
			for (Entry<String, String> entry : map.entrySet()) {
				// Check if the current entry relates to cache config.
				if (entry.getKey().startsWith(
						LdOlap4jDriver.Property.CACHE.name() + ".")) {
					props.put(
							entry.getKey().substring(
									LdOlap4jDriver.Property.CACHE.name()
											.length() + 1), entry.getValue());
				}
			}

			// Init the cache
			((XmlaOlap4jCachedProxy) this.proxy).setCache(map, props);
		}
	}

	static Map<String, String> parseConnectString(String url, Properties info) {
		String x = url.substring(CONNECT_STRING_PREFIX.length());
		Map<String, String> map = ConnectStringParser.parseConnectString(x);
		for (Map.Entry<String, String> entry : toMap(info).entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

	static boolean acceptsURL(String url) {
		return url.startsWith(CONNECT_STRING_PREFIX);
	}

	/**
	 * Creates statement
	 */
	public OlapStatement createStatement() {
		/*
		 * TODO: Here, we need to create the XML (maybe even not) and fill the
		 * OLAP database
		 * 
		 * We can even get additional information to be able to fill the table
		 */
		return new LdOlap4jStatement(this);

		/*
		 * Instead of parsing the mdx by ourselves and creating sparql queries,
		 * which might be useful later, we "manifest" the current schema in
		 * Mondrian and allow queries there.
		 * 
		 * On the one hand, we need to create a proper XML file that the
		 * mondrian server connects to. On the other hand, we need to fill in
		 * the MySQL database.
		 */
		// File xslFile = new File(
		// "C:/Users/b-kaempgen/Documents/Workspaces/Eclipse_SLD/OLAP4LD/resources/xml2nx.xsl");
		//
		// try {
		// return myOlapServer.getOlapConnection().createStatement();
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// return null;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String nativeSQL(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		this.autoCommit = autoCommit;
	}

	public boolean getAutoCommit() throws SQLException {
		return autoCommit;
	}

	public void commit() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void rollback() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void close() throws SQLException {
		closed = true;
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public OlapDatabaseMetaData getMetaData() {
		return olap4jDatabaseMetaData;
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		this.readOnly = readOnly;
	}

	public boolean isReadOnly() throws SQLException {
		return readOnly;
	}

	public void setDatabase(String databaseName) throws OlapException {
		if (databaseName == null) {
			throw new OlapException("Database name cannot be null.");
		}
		this.olap4jDatabase = (LdOlap4jDatabase) getOlapDatabases().get(
				databaseName);
		if (this.olap4jDatabase == null) {
			throw new OlapException("No database named " + databaseName
					+ " could be found.");
		}
		this.databaseName = databaseName;
		this.olap4jCatalog = null;
		this.olap4jSchema = null;
	}

	public String getDatabase() throws OlapException {
		return getOlapDatabase().getName();
	}

	public Database getOlapDatabase() throws OlapException {
		if (this.olap4jDatabase == null) {
			if (this.databaseName == null) {
				List<Database> databases = getOlapDatabases();
				if (databases.size() == 0) {
					throw new OlapException("No database found.");
				}
				this.olap4jDatabase = (LdOlap4jDatabase) databases.get(0);
				this.databaseName = this.olap4jDatabase.getName();
				this.olap4jCatalog = null;
				this.olap4jSchema = null;
			} else {
				this.olap4jDatabase = (LdOlap4jDatabase) getOlapDatabases()
						.get(this.databaseName);
				this.olap4jCatalog = null;
				this.olap4jSchema = null;
				if (this.olap4jDatabase == null) {
					throw new OlapException("No database named "
							+ this.databaseName + " could be found.");
				}
			}
		}
		return olap4jDatabase;
	}

	public NamedList<Database> getOlapDatabases() throws OlapException {
		// TODO: Here, I use myOlapServer
		// Maybe I should rather return XmlaOlap4jDatabase

		// return Olap4jUtil.cast(this.myOlapServer.getOlapConnection()
		// .getOlapDatabases());
		LdOlap4jUtil._log.info("getOlapDatabases()...");
		return Olap4jUtil.cast(this.olapDatabases);
	}

	public void setCatalog(String catalogName) throws OlapException {
		if (catalogName == null) {
			throw new OlapException("Catalog name cannot be null.");
		}
		this.olap4jCatalog = (LdOlap4jCatalog) getOlapCatalogs().get(
				catalogName);
		if (this.olap4jCatalog == null) {
			throw new OlapException("No catalog named " + catalogName
					+ " could be found.");
		}
		this.catalogName = catalogName;
		this.olap4jSchema = null;
	}

	public String getCatalog() throws OlapException {
		return getOlapCatalog().getName();
	}

	public Catalog getOlapCatalog() throws OlapException {
		if (this.olap4jCatalog == null) {
			final Database database = getOlapDatabase();
			if (this.catalogName == null) {
				if (database.getCatalogs().size() == 0) {
					throw new OlapException("No catalogs could be found.");
				}
				this.olap4jCatalog = (LdOlap4jCatalog) database.getCatalogs()
						.get(0);
				this.catalogName = this.olap4jCatalog.getName();
				this.olap4jSchema = null;
			} else {
				this.olap4jCatalog = (LdOlap4jCatalog) database.getCatalogs()
						.get(this.catalogName);
				if (this.olap4jCatalog == null) {
					throw new OlapException("No catalog named "
							+ this.catalogName + " could be found.");
				}
				this.olap4jSchema = null;
			}
		}
		return olap4jCatalog;
	}

	public NamedList<Catalog> getOlapCatalogs() throws OlapException {
		return getOlapDatabase().getCatalogs();
	}

	public String getSchema() throws OlapException {
		return getOlapSchema().getName();
	}

	public void setSchema(String schemaName) throws OlapException {
		if (schemaName == null) {
			throw new OlapException("Schema name cannot be null.");
		}
		final Catalog catalog = getOlapCatalog();
		this.olap4jSchema = (LdOlap4jSchema) catalog.getSchemas().get(
				schemaName);
		if (this.olap4jSchema == null) {
			throw new OlapException("No schema named " + schemaName
					+ " could be found in catalog " + catalog.getName());
		}
		this.schemaName = schemaName;
	}

	public synchronized Schema getOlapSchema() throws OlapException {
		if (this.olap4jSchema == null) {
			final Catalog catalog = getOlapCatalog();
			if (this.schemaName == null) {
				if (catalog.getSchemas().size() == 0) {
					throw new OlapException("No schemas could be found.");
				}
				this.olap4jSchema = (LdOlap4jSchema) catalog.getSchemas()
						.get(0);
			} else {
				this.olap4jSchema = (LdOlap4jSchema) catalog.getSchemas().get(
						this.schemaName);
				if (this.olap4jSchema == null) {
					throw new OlapException("No schema named "
							+ this.schemaName + " could be found.");
				}
			}
		}
		return olap4jSchema;
	}

	public NamedList<Schema> getOlapSchemas() throws OlapException {
		return getOlapCatalog().getSchemas();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getTransactionIsolation() throws SQLException {
		return TRANSACTION_NONE;
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void clearWarnings() throws SQLException {
		// this driver does not support warnings, so nothing to do
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setHoldability(int holdability) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Savepoint setSavepoint() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public PreparedStatement prepareStatement(String sql, int columnIndexes[])
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public PreparedStatement prepareStatement(String sql, String columnNames[])
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	// implement Wrapper

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this)) {
			return iface.cast(this);
		}
		throw getHelper().createException("does not implement '" + iface + "'");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	// implement OlapConnection

	public PreparedOlapStatement prepareOlapStatement(String mdx)
			throws OlapException {
		return factory.newPreparedStatement(mdx, this);
	}

	public MdxParserFactory getParserFactory() {
		return new MdxParserFactory() {
			public MdxParser createMdxParser(OlapConnection connection) {
				return new DefaultMdxParserImpl();
			}

			public MdxValidator createMdxValidator(OlapConnection connection) {
				return new XmlaOlap4jMdxValidator(connection);
			}
		};
	}

	public static Map<String, String> toMap(final Properties properties) {
		return new AbstractMap<String, String>() {
			public Set<Entry<String, String>> entrySet() {
				return Olap4jUtil.cast(properties.entrySet());
			}
		};
	}

	/**
	 * Returns the URL which was used to create this connection.
	 * 
	 * @return URL
	 */
	String getURL() {
		throw Olap4jUtil.needToImplement(this);
	}

	public void setLocale(Locale locale) {
		if (locale == null) {
			throw new IllegalArgumentException("locale must not be null");
		}
		final Locale previousLocale = this.locale;
		this.locale = locale;

		// If locale has changed, clear the cache. This is necessary because
		// metadata elements (e.g. Cubes) only store the caption & description
		// of the current locale. The SOAP cache, if enabled, will speed things
		// up a little if a client JVM uses connections to the same server with
		// different locales.
		if (!Olap4jUtil.equal(previousLocale, locale)) {
			clearCache();
		}
	}

	/**
	 * Clears the cache.
	 */
	private void clearCache() {
		// clearCache probably does not work
		// TODO: remove or improve logging
		LdOlap4jUtil._log.warning("Caching olapDatabases... does this work?");
		((DeferredNamedListImpl) this.olapDatabases).reset();
		this.olap4jCatalog = null;
		this.olap4jDatabase = null;
		this.olap4jSchema = null;
	}

	public Locale getLocale() {
		if (locale == null) {
			return Locale.getDefault();
		}
		return locale;
	}

	public void setRoleName(String roleName) throws OlapException {
		this.roleName = roleName;
	}

	public String getRoleName() {
		return roleName;
	}

	public List<String> getAvailableRoleNames() {
		// List of available roles is not known. Could potentially add an XMLA
		// call to populate this list if useful to a client.
		return null;
	}

	public Scenario createScenario() {
		throw new UnsupportedOperationException();
	}

	public void setScenario(Scenario scenario) {
		throw new UnsupportedOperationException();
	}

	public Scenario getScenario() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Here, a request is generated and executed. With that, we populate the
	 * objects.
	 * 
	 * For creating the metadata: There are two points, where Linked Data is
	 * read from: getMetadataLd (to create Resultset), and populateList (to fill
	 * in objects).
	 * 
	 * @param list
	 * @param context
	 * @param metadataRequest
	 * @param handler
	 * @param restrictions
	 * @throws OlapException
	 */
	<T extends Named> void populateList(List<T> list, Context context,
			MetadataRequest metadataRequest, Handler<T> handler,
			Object[] restrictions) throws OlapException {
		// String request = generateRequest(context, metadataRequest,
		// restrictions);
		// Element root = executeMetadataRequest(request);

		// Here, we now query Linked Data
		// Here, the context helps, since deferred lists are filled with
		// contexts.
		List<org.semanticweb.yars.nx.Node[]> root = executeMetadataRequestOnLd(
				context, metadataRequest, restrictions);

		// Go through Nodes and change handlers to do so, as well.
		boolean isFirst = true;
		Map<String, Integer> mapFields = new HashMap<String, Integer>();
		for (org.semanticweb.yars.nx.Node[] o : root) {
			// The first Node gives the fields
			if (isFirst) {
				mapFields = LdOlap4jUtil.getNodeResultFields(o);
				isFirst = false;
				continue;
			}
			handler.handle(o, mapFields, context, list);
		}
		// for (Element o : childElements(root)) {
		// if (o.getLocalName().equals("row")) {
		// /*
		// * The results of the metadata request are wrapped as objects
		// */
		// handler.handle(o, context, list);
		// }
		// }
		handler.sortList(list);
	}

	/**
	 * 
	 * Querying Linked Data: There are two points, where Linked Data is read
	 * from: getMetadataLd (to create Resultset), and populateList (to fill in
	 * objects).
	 * 
	 * Both are filled by calling methods in LinkedDataEngine. The problem is
	 * that sometimes values returned by LinkedDataEngine need to be further
	 * processed to be usable in ResultSet and metadata objects:
	 * 
	 * * URIs used for unique names need to be translated into an MDX friendly
	 * format * Instead of null values, nx format uses "null" (TODO: or by now
	 * something else?) For certain returned values this needs to be transformed
	 * into a proper null.
	 * 
	 * This means: LinkedDataEngine should always return literal values if no
	 * encoding needs to be done. URI that it returns should be transformed for
	 * use in MDX. If LinkedDataEngine returns "null" (or similar), it is
	 * transformed into proper null. All program logic, e.g., if no CAPTION is
	 * available (so that CAPTION returns null) then use UNIQUE_NAME is either
	 * implemented by the client or LinkedDataEngine.
	 * 
	 * TODO: For DatabaseMetaData, we should replace values in here.
	 * 
	 * @param context
	 * @param metadataRequest
	 * @param restrictions
	 * @return Nodes to work with internally.
	 */
	protected List<org.semanticweb.yars.nx.Node[]> executeMetadataRequestOnLd(
			Context context, MetadataRequest metadataRequest,
			Object[] restrictions) {

			LdOlap4jUtil._log.info("********************************************");
			LdOlap4jUtil._log.info("** SENDING REQUEST :");
			String restrictionString = "";
			for (int i = 0; i < restrictions.length; i = i + 2) {
				if ("CATALOG_NAME".equals((String) restrictions[i])) {
					restrictionString += "catalog_name = "+(String) restrictions[i + 1];
					// we do not consider catalogs for now.
					continue;
				}
				if ("SCHEMA_NAME".equals((String) restrictions[i])) {
					restrictionString += "schema_name = "+(String) restrictions[i + 1];
					// we do not consider schema for now
					continue;
				}
				if ("CUBE_NAME".equals((String) restrictions[i])) {
					restrictionString += "cube_name = "+(String) restrictions[i + 1];
					continue;
				}
				if ("DIMENSION_UNIQUE_NAME".equals((String) restrictions[i])) {
					restrictionString += "dimension_unique_name = "+(String) restrictions[i + 1];
					continue;
				}
				if ("HIERARCHY_UNIQUE_NAME".equals((String) restrictions[i])) {
					restrictionString += "hierarchy_unique_name = "+(String) restrictions[i + 1];
					continue;
				}
				if ("LEVEL_UNIQUE_NAME".equals((String) restrictions[i])) {
					restrictionString += "level_unique_name = "+(String) restrictions[i + 1];
					continue;
				}
				if ("MEMBER_UNIQUE_NAME".equals((String) restrictions[i])) {
					restrictionString += "member_unique_name = "+(String) restrictions[i + 1];
					continue;
				}
				if ("TREE_OP".equals((String) restrictions[i])) {
					restrictionString += "tree_op = "+new Integer((String) restrictions[i + 1]);
					// treeOps erstellen wie in OpenVirtuoso
					continue;
				}
				
			}

			LdOlap4jUtil._log.info("executeMetadataRequestOnLd("+metadataRequest.name()+") with "+restrictionString+";");
			LdOlap4jUtil._log.info("********************************************");

		/*
		 * Specification of those metadata requests, see MetaDataRequest,
		 * further below
		 */

		/*
		 * TODO: We should throw proper OLAP execptions coming from the engine,
		 * e.g. (see above):
		 * 
		 * throw getHelper().createException( "XMLA provider gave exception: " +
		 * XmlaOlap4jUtil.prettyPrint(fault) + "\n" + "Request was:\n" +
		 * request);
		 */
			
		// Restrictions are wrapped in own object
		Restrictions myRestrictionsObject = new Restrictions(restrictions);

		// Create result
		switch (metadataRequest) {
		case DISCOVER_DATASOURCES:

			return this.myLinkedData.getDatabases(myRestrictionsObject);

		case DBSCHEMA_CATALOGS:

			return this.myLinkedData.getCatalogs(myRestrictionsObject);

		case DBSCHEMA_SCHEMATA:

			return this.myLinkedData.getSchemas(myRestrictionsObject);

		case MDSCHEMA_CUBES:

			return this.myLinkedData.getCubes(myRestrictionsObject);

		case MDSCHEMA_DIMENSIONS:
			/*
			 * Now, we have two possibilities: Either, here, the shared
			 * dimensions are asked for, or the dimensions of certain cubes.
			 */

			return this.myLinkedData.getDimensions(myRestrictionsObject);

		case MDSCHEMA_MEASURES:

			return this.myLinkedData.getMeasures(myRestrictionsObject);

		case MDSCHEMA_SETS:

			return this.myLinkedData.getSets(myRestrictionsObject);

		case MDSCHEMA_HIERARCHIES:

			return this.myLinkedData.getHierarchies(myRestrictionsObject);

		case MDSCHEMA_LEVELS:

			return this.myLinkedData.getLevels(myRestrictionsObject);

		case MDSCHEMA_MEMBERS:

			return this.myLinkedData.getMembers(myRestrictionsObject);

		default:
			/*
			 * If we haven't implemented it, yet, simply return empty list.
			 * 
			 * In this case, an empty ResultSet should be created.
			 */
			List<org.semanticweb.yars.nx.Node[]> result = new ArrayList<org.semanticweb.yars.nx.Node[]>();
			return result;
		}
	}

	/**
	 * Encodes a string for use in an XML CDATA section.
	 * 
	 * @param value
	 *            Value to be xml encoded
	 * @param buf
	 *            Buffer to append to
	 */
	private static void xmlEncode(StringBuilder buf, String value) {
		final int n = value.length();
		for (int i = 0; i < n; ++i) {
			char c = value.charAt(i);
			switch (c) {
			case '&':
				buf.append("&amp;");
				break;
			case '<':
				buf.append("&lt;");
				break;
			case '>':
				buf.append("&gt;");
				break;
			case '"':
				buf.append("&quot;");
				break;
			case '\'':
				buf.append("&apos;");
				break;
			default:
				buf.append(c);
			}
		}
	}

	// ~ inner classes --------------------------------------------------------
	static class DatabaseHandler extends HandlerImpl<LdOlap4jDatabase> {
		@Override
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jDatabase> list) throws OlapException {
			// I want to ask first for the field names
			String dsName = row[mapFields
					.get("?DATA_SOURCE_NAME")].toString();
			String dsDesc = row[mapFields
					.get("?DATA_SOURCE_DESCRIPTION")].toString();
			String url = row[mapFields
					.get("?URL")].toString();
			String dsInfo = row[mapFields
					.get("?DATA_SOURCE_INFO")].toString();
			String providerName = row[mapFields
					.get("?PROVIDER_NAME")].toString();

			// Simply empty
			List<ProviderType> pTypesList = new ArrayList<ProviderType>();
			// Empty
			List<AuthenticationMode> aModesList = new ArrayList<AuthenticationMode>();
			list.add(new LdOlap4jDatabase(context.olap4jConnection, dsName,
					dsDesc, providerName, url, dsInfo, pTypesList, aModesList));
		}
	}

	static class CatalogHandler extends HandlerImpl<LdOlap4jCatalog> {
		private final LdOlap4jDatabase database;

		public CatalogHandler(LdOlap4jDatabase database) {
			this.database = database;
		}

		@Override
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jCatalog> list) throws OlapException {
			// TODO Auto-generated method stub
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>FoodMart</CATALOG_NAME> <DESCRIPTION>No
			 * description available</DESCRIPTION> <ROLES>California manager,No
			 * HR Cube</ROLES> </row>
			 */
			String catalogName = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?TABLE_CAT")]);
			// Unused: DESCRIPTION, ROLES
			list.add(new LdOlap4jCatalog(context.olap4jDatabaseMetaData,
					database, catalogName));
		}
	}

	static class CubeHandler extends HandlerImpl<LdOlap4jCube> {

		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jCube> list) throws OlapException {

			// We need to make the cubeName MDX compatible
			String cubeName = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?CUBE_NAME")]);

			String caption = LdOlap4jUtil.makeCaption(LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?CUBE_CAPTION")]));

			String description = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?DESCRIPTION")]);

			list.add(new LdOlap4jCube(context.olap4jSchema, cubeName, caption,
					description));
		}
	}

	static class DimensionHandler extends HandlerImpl<LdOlap4jDimension> {
		private final LdOlap4jCube cubeForCallback;

		public DimensionHandler(LdOlap4jCube cube) {
			this.cubeForCallback = cube;
		}

		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jDimension> list) {

			// This typically should be a uri so that we need to make it MDX
			// ready
			String dimensionName = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?DIMENSION_NAME")]);
			// This should always be a URI
			String dimensionUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields
							.get("?DIMENSION_UNIQUE_NAME")]);
			String dimensionCaption = LdOlap4jUtil
					.makeCaption(LdOlap4jUtil.convertNodeToMDX(row[mapFields
							.get("?DIMENSION_CAPTION")]));

			String description = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?DESCRIPTION")]);
			// From this result only integer values should be returned (can we
			// actually do this with SPARQL?
			// TODO: SO far, only unkown(0) is returned, here.
			final int dimensionType = new Integer(row[mapFields
							.get("?DIMENSION_TYPE")].toString());
			final Dimension.Type type = Dimension.Type.getDictionary()
					.forOrdinal(dimensionType);
			// the max of hierarchies
			// final String defaultHierarchyUniqueName = row[mapFields
			// .get("?DEFAULT_HIERARCHY")].toString();
			// TODO: For default hierarchy it is similar...
			final String defaultHierarchyUniqueName = null;
			// simply 0 is returned here
			final Integer dimensionOrdinal = new Integer(row[mapFields
							.get("?DIMENSION_ORDINAL")].toString());
			LdOlap4jDimension dimension = new LdOlap4jDimension(
					context.olap4jCube, dimensionUniqueName, dimensionName,
					dimensionCaption, description, type,
					defaultHierarchyUniqueName, dimensionOrdinal == null ? 0
							: dimensionOrdinal);
			list.add(dimension);
			if (dimensionOrdinal != null) {
				Collections.sort(list, new Comparator<LdOlap4jDimension>() {
					public int compare(LdOlap4jDimension d1,
							LdOlap4jDimension d2) {
						if (d1.getOrdinal() == d2.getOrdinal()) {
							return 0;
						} else if (d1.getOrdinal() > d2.getOrdinal()) {
							return 1;
						} else {
							return -1;
						}
					}
				});
			}
			this.cubeForCallback.dimensionsByUname.put(
					dimension.getUniqueName(), dimension);
		}
	}

	static class HierarchyHandler extends HandlerImpl<LdOlap4jHierarchy> {
		private final LdOlap4jCube cubeForCallback;

		public HierarchyHandler(LdOlap4jCube cubeForCallback) {
			this.cubeForCallback = cubeForCallback;
		}

		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jHierarchy> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>FoodMart</CATALOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME> <CUBE_NAME>Sales</CUBE_NAME>
			 * <DIMENSION_UNIQUE_NAME>[Customers]</DIMENSION_UNIQUE_NAME>
			 * <HIERARCHY_NAME>Customers</HIERARCHY_NAME>
			 * <HIERARCHY_UNIQUE_NAME>[Customers]</HIERARCHY_UNIQUE_NAME>
			 * <HIERARCHY_CAPTION>Customers</HIERARCHY_CAPTION>
			 * <DIMENSION_TYPE>3</DIMENSION_TYPE>
			 * <HIERARCHY_CARDINALITY>10407</HIERARCHY_CARDINALITY>
			 * <DEFAULT_MEMBER>[Customers].[All Customers]</DEFAULT_MEMBER>
			 * <ALL_MEMBER>[Customers].[All Customers]</ALL_MEMBER>
			 * <DESCRIPTION>Sales Cube - Customers Hierarchy</DESCRIPTION>
			 * <STRUCTURE>0</STRUCTURE> <IS_VIRTUAL>false</IS_VIRTUAL>
			 * <IS_READWRITE>false</IS_READWRITE>
			 * <DIMENSION_UNIQUE_SETTINGS>0</DIMENSION_UNIQUE_SETTINGS>
			 * <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
			 * <HIERARCHY_ORDINAL>9</HIERARCHY_ORDINAL>
			 * <DIMENSION_IS_SHARED>true</DIMENSION_IS_SHARED>
			 * <PARENT_CHILD>false</PARENT_CHILD> </row>
			 */

			final String hierarchyUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields
							.get("?HIERARCHY_UNIQUE_NAME")]);
			// SAP BW (and LinkedDataEngine) doesn't return a HIERARCHY_NAME
			// attribute,
			// so try to use the unique name instead
			final String hierarchyName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?HIERARCHY_NAME")]) == null ? hierarchyUniqueName
					: LdOlap4jUtil.convertNodeToMDX(row[mapFields
							.get("?HIERARCHY_NAME")]);
			final String hierarchyCaption = LdOlap4jUtil
					.makeCaption(LdOlap4jUtil.convertNodeToMDX(row[mapFields
							.get("?HIERARCHY_CAPTION")]));
			final String description = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?DESCRIPTION")]);
			// TODO: For us, the all member is always null
			// final String allMember = row[mapFields.get("?ALL_MEMBER")]
			// .toString();
			final String allMember = null;
			// TODO: Is not set for measure hierarchies
			// Problem: The aggregation function max for finding a default
			// member would not work, since it would aggregate to early. For
			// now, I simply set the default member to null.
			// final String defaultMemberUniqueName = row[mapFields
			// .get("?DEFAULT_MEMBER")].toString();
			final String defaultMemberUniqueName = null;
			LdOlap4jHierarchy hierarchy = new LdOlap4jHierarchy(
					context.getDimension(row, mapFields), hierarchyUniqueName,
					hierarchyName, hierarchyCaption, description,
					allMember != null, defaultMemberUniqueName);
			list.add(hierarchy);
			cubeForCallback.hierarchiesByUname.put(hierarchy.getUniqueName(),
					hierarchy);
		}
	}

	static class LevelHandler extends HandlerImpl<LdOlap4jLevel> {
		public static final int MDLEVEL_TYPE_CALCULATED = 0x0002;
		private final LdOlap4jCube cubeForCallback;

		public LevelHandler(LdOlap4jCube cubeForCallback) {
			this.cubeForCallback = cubeForCallback;
		}

		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jLevel> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>FoodMart</CATALOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME> <CUBE_NAME>Sales</CUBE_NAME>
			 * <DIMENSION_UNIQUE_NAME>[Customers]</DIMENSION_UNIQUE_NAME>
			 * <HIERARCHY_UNIQUE_NAME>[Customers]</HIERARCHY_UNIQUE_NAME>
			 * <LEVEL_NAME>(All)</LEVEL_NAME>
			 * <LEVEL_UNIQUE_NAME>[Customers].[(All)]</LEVEL_UNIQUE_NAME>
			 * <LEVEL_CAPTION>(All)</LEVEL_CAPTION>
			 * <LEVEL_NUMBER>0</LEVEL_NUMBER>
			 * <LEVEL_CARDINALITY>1</LEVEL_CARDINALITY>
			 * <LEVEL_TYPE>1</LEVEL_TYPE>
			 * <CUSTOM_ROLLUP_SETTINGS>0</CUSTOM_ROLLUP_SETTINGS>
			 * <LEVEL_UNIQUE_SETTINGS>3</LEVEL_UNIQUE_SETTINGS>
			 * <LEVEL_IS_VISIBLE>true</LEVEL_IS_VISIBLE> <DESCRIPTION>Sales Cube
			 * - Customers Hierarchy - (All) Level</DESCRIPTION> </row>
			 */

			final String levelUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?LEVEL_UNIQUE_NAME")]);
			// SAP BW doesn't return a HIERARCHY_NAME attribute,
			// so try to use the unique name instead
			final String levelName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?LEVEL_NAME")]) == null ? levelUniqueName
					: LdOlap4jUtil.convertNodeToMDX(row[mapFields
							.get("?LEVEL_NAME")]);
			final String levelCaption = LdOlap4jUtil.makeCaption(LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?LEVEL_CAPTION")]));
			final String description = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?DESCRIPTION")]);
			// The distance of the level from the root of the hierarchy. Root
			// level is zero (0).
			// Default is root level.
			int levelNumber = 0;
			if (LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?LEVEL_NUMBER")]) != null) {
				// Since we use depth, we need to reduce the size with 1.
				levelNumber = new Integer(row[mapFields
								.get("?LEVEL_NUMBER")].toString()) - 1;
				if (levelNumber < 0) {
					throw new UnsupportedOperationException(
							"The levelNumber cannot be negative!");
				}
			}
			// Is Hexadecimal therefore little more complicated
			String level_type = row[mapFields
					.get("?LEVEL_TYPE")].toString();
			final Integer levelTypeCode = Integer.parseInt(
					level_type.substring(2, level_type.length()), 16);
			
			final Level.Type levelType = Level.Type.getDictionary().forOrdinal(
					levelTypeCode);
			boolean calculated = (levelTypeCode & MDLEVEL_TYPE_CALCULATED) != 0;
			final int levelCardinality = new Integer(
					row[mapFields
							.get("?LEVEL_CARDINALITY")].toString());
			LdOlap4jLevel level = new LdOlap4jLevel(context.getHierarchy(row,
					mapFields), levelUniqueName, levelName, levelCaption,
					description, levelNumber, levelType, calculated,
					levelCardinality);
			list.add(level);
			cubeForCallback.levelsByUname.put(level.getUniqueName(), level);

		}
	}

	static class MeasureHandler extends HandlerImpl<LdOlap4jMeasure> {

		public void sortList(List<LdOlap4jMeasure> list) {
			Collections.sort(list, new Comparator<LdOlap4jMeasure>() {
				public int compare(LdOlap4jMeasure o1, LdOlap4jMeasure o2) {
					return o1.getOrdinal() - o2.getOrdinal();
				}
			});
		}

		@Override
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jMeasure> list) throws OlapException {
			/*
			 * Example: <row> <CATALOG_NAME>FoodMart</CATALOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME> <CUBE_NAME>Sales</CUBE_NAME>
			 * <MEASURE_NAME>Profit</MEASURE_NAME>
			 * <MEASURE_UNIQUE_NAME>[Measures].[Profit]</MEASURE_UNIQUE_NAME>
			 * <MEASURE_CAPTION>Profit</MEASURE_CAPTION>
			 * <MEASURE_AGGREGATOR>127</MEASURE_AGGREGATOR>
			 * <DATA_TYPE>130</DATA_TYPE>
			 * <MEASURE_IS_VISIBLE>true</MEASURE_IS_VISIBLE> <DESCRIPTION>Sales
			 * Cube - Profit Member</DESCRIPTION> </row>
			 */
			
			// Here, we need to have a certain number
			/*
			 * Here, we get a string with the aggregation function, which we
			 * need to translate into an Aggregator. If no aggregation function
			 * is given, we use COUNT.
			 */
			Aggregator aggregator = Measure.Aggregator.COUNT;
			ParseTreeNode expression = null;
			Measure.Aggregator measureAggregator = aggregator;
			final Datatype datatype;
			Node rowAggregator = row[mapFields.get("?MEASURE_AGGREGATOR")];
			if (rowAggregator.toString().toLowerCase().equals("sum")) {
				measureAggregator = Measure.Aggregator.SUM;
			} else if (rowAggregator.toString().toLowerCase().equals("avg")) {
				measureAggregator = Measure.Aggregator.AVG;
			} else if (rowAggregator.toString().toLowerCase().equals("max")) {
				measureAggregator = Measure.Aggregator.MIN;
			} else if (rowAggregator.toString().toLowerCase().equals("min")) {
				measureAggregator = Measure.Aggregator.MAX;
			} else if (rowAggregator.toString().toLowerCase().equals("count")) {
				measureAggregator = Measure.Aggregator.COUNT;
			} else if (rowAggregator.toString().toLowerCase().equals("calculated")) {
				measureAggregator = Measure.Aggregator.CALCULATED;
				// In this case, we should also have an expression.
				// expression =
				// LdOlap4jUtil.convertNodeToMDX(row[mapFields.get("?EXPRESSION")]);
				LdOlap4jUtil._log
						.info("Calculated members from the DSD are not supported, yet.");
			}
			
			// Names
			final String measureName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?MEASURE_NAME")]);
			final String measureUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?MEASURE_UNIQUE_NAME")]);
			// Measure caption will contain the measure property
			final String measureCaption = LdOlap4jUtil.makeCaption(LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?MEASURE_CAPTION")]));

			/*
			 * Node.toString is not enough. Integer/Boolean also
			 */
			final String description = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?MEASURE_CAPTION")]);

			// Here, for a certain name, we get a datatype
			// TODO: How to read DataType from RDF?
			Datatype ordinalDatatype = Datatype.getDictionary().forName(
					LdOlap4jUtil.convertNodeToMDX(row[mapFields
							.get("?DATA_TYPE")]));
			if (ordinalDatatype == null) {

				// If it is encoded as number, fine also.
				datatype = Datatype.getDictionary().forOrdinal(
						new Integer(row[mapFields
								.get("?DATA_TYPE")].toString()));
			} else {
				datatype = ordinalDatatype;
			}

			// Here, we need a boolean
			final boolean measureIsVisible = new Boolean(row[mapFields
							.get("?MEASURE_IS_VISIBLE")].toString());

			/*
			 * Apparently, for each single measure in a cube, i need a member in
			 * the same cube And that member should be in a proper level. Thus,
			 * we need to create levels for the measures, as well.
			 */
			// TODO: Does this work?
			/*
			 * TODO: Null is fine because getCube does not use its parameter
			 */
			// We are looking for the member representing the measure. We cannot
			// simply ask the measures dimension, since it queries the measures
			// and not the members.
			final Member member = context.getCube(null).getMetadataReader()
					.lookupMemberByUniqueName(measureUniqueName);

			if (member == null) {
				throw new OlapException(
						"The server failed to resolve a member with the same unique name as a measure named "
								+ measureUniqueName);
			}

			list.add(new LdOlap4jMeasure((LdOlap4jLevel) member.getLevel(),
					measureUniqueName, measureName, measureCaption,
					description, null, measureAggregator, expression, datatype,
					measureIsVisible, member.getOrdinal()));

		}
	}

	static class MemberHandler extends HandlerImpl<LdOlap4jMember> {

		/**
		 * Collection of nodes to ignore because they represent standard
		 * built-in properties of Members.
		 */
		private static final Set<String> EXCLUDED_PROPERTY_NAMES = new HashSet<String>(
				Arrays.asList(Property.StandardMemberProperty.CATALOG_NAME
						.name(), Property.StandardMemberProperty.CUBE_NAME
						.name(),
						Property.StandardMemberProperty.DIMENSION_UNIQUE_NAME
								.name(),
						Property.StandardMemberProperty.HIERARCHY_UNIQUE_NAME
								.name(),
						Property.StandardMemberProperty.LEVEL_UNIQUE_NAME
								.name(),
						Property.StandardMemberProperty.PARENT_LEVEL.name(),
						Property.StandardMemberProperty.PARENT_COUNT.name(),
						Property.StandardMemberProperty.MEMBER_KEY.name(),
						Property.StandardMemberProperty.IS_PLACEHOLDERMEMBER
								.name(),
						Property.StandardMemberProperty.IS_DATAMEMBER.name(),
						Property.StandardMemberProperty.LEVEL_NUMBER.name(),
						Property.StandardMemberProperty.MEMBER_ORDINAL.name(),
						Property.StandardMemberProperty.MEMBER_UNIQUE_NAME
								.name(),
						Property.StandardMemberProperty.MEMBER_NAME.name(),
						Property.StandardMemberProperty.PARENT_UNIQUE_NAME
								.name(),
						Property.StandardMemberProperty.MEMBER_TYPE.name(),
						Property.StandardMemberProperty.MEMBER_CAPTION.name(),
						Property.StandardMemberProperty.CHILDREN_CARDINALITY
								.name(), Property.StandardMemberProperty.DEPTH
								.name()));

		/**
		 * Cached value returned by the {@link Member.Type#values} method, which
		 * calls {@link Class#getEnumConstants()} and unfortunately clones an
		 * array every time.
		 */
		private static final Member.Type[] MEMBER_TYPE_VALUES = Member.Type
				.values();

		private void addUserDefinedDimensionProperties(
				org.semanticweb.yars.nx.Node[] row,
				org.semanticweb.yars.nx.Node[] firstRow, LdOlap4jLevel level,
				Map<Property, Object> map) {

			// TODO: Started to implement but not finished
			// //NodeList nodes = row.getChildNodes();
			//
			// for (int i = 0; i < row.length; i++) {
			// org.semanticweb.yars.nx.Node node = row[i];
			// // Here, only
			// if (EXCLUDED_PROPERTY_NAMES.contains(firstRow[i].toString())) {
			// continue;
			// }
			// for (Property property : level.getProperties()) {
			// if (property instanceof XmlaOlap4jProperty
			// && property.getName().equalsIgnoreCase(
			// node.getLocalName())) {
			// map.put(property, node.getTextContent());
			// }
			// }
			// }
		}

		@Override
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jMember> list) throws OlapException {

			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>FoodMart</CATALOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME> <CUBE_NAME>Sales</CUBE_NAME>
			 * <DIMENSION_UNIQUE_NAME>[Gender]</DIMENSION_UNIQUE_NAME>
			 * <HIERARCHY_UNIQUE_NAME>[Gender]</HIERARCHY_UNIQUE_NAME>
			 * <LEVEL_UNIQUE_NAME>[Gender].[Gender]</LEVEL_UNIQUE_NAME>
			 * <LEVEL_NUMBER>1</LEVEL_NUMBER> <MEMBER_ORDINAL>1</MEMBER_ORDINAL>
			 * <MEMBER_NAME>F</MEMBER_NAME>
			 * <MEMBER_UNIQUE_NAME>[Gender].[F]</MEMBER_UNIQUE_NAME>
			 * <MEMBER_TYPE>1</MEMBER_TYPE> <MEMBER_CAPTION>F</MEMBER_CAPTION>
			 * <CHILDREN_CARDINALITY>0</CHILDREN_CARDINALITY>
			 * <PARENT_LEVEL>0</PARENT_LEVEL> <PARENT_UNIQUE_NAME>[Gender].[All
			 * Gender]</PARENT_UNIQUE_NAME> <PARENT_COUNT>1</PARENT_COUNT>
			 * <DEPTH>1</DEPTH> <!-- mondrian-specific --> </row>
			 */
			if (false) {
				int levelNumber = new Integer(
						row[mapFields
								.get("?LEVEL_NUMBER")].toString());
			}
			// TODO: FOr now, we only have member ordinal = 0
			int memberOrdinal = 0;
			String memberUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?MEMBER_UNIQUE_NAME")]);
			String memberName = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?MEMBER_NAME")]);
			// Parent unique name should be null if no parent is available.
			// TODO: Null handling is still not thought through since LDEngine
			// returns <null>
			String parentUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?PARENT_UNIQUE_NAME")]);

			// We see, an Integer is used, which means we need to return the
			// type in terms of the number
			Member.Type memberType = MEMBER_TYPE_VALUES[new Integer(
					row[mapFields
							.get("?MEMBER_TYPE")].toString())];

			String memberCaption = LdOlap4jUtil.makeCaption(LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?MEMBER_CAPTION")]));
			// TODO: For now, we only have cardinality 0
			int childrenCardinality = 0;

			// Gather member property values into a temporary map, so we can
			// create the member with all properties known. XmlaOlap4jMember
			// uses an ArrayMap for property values and it is not efficient to
			// add entries to the map one at a time.
			final LdOlap4jLevel level = context.getLevel(row, mapFields);

			// TODO: For now, I will not have any additional properties
			final Map<Property, Object> map = new HashMap<Property, Object>();
			// addUserDefinedDimensionProperties(row, level, map);

			// Usually members have the same depth as their level. (Ragged and
			// parent-child hierarchies are an exception.) Only store depth for
			// the unusual ones.
			// final Integer depth = integerElement(row,
			// Property.StandardMemberProperty.DEPTH.name());
			// if (depth != null && depth.intValue() != level.getDepth()) {
			// map.put(Property.StandardMemberProperty.DEPTH, depth);
			// }

			// If this member is a measure, we want to return an object that
			// implements the Measure interface to all API calls. But we also
			// need to retrieve the properties that occur in MDSCHEMA_MEMBERS
			// that are not available in MDSCHEMA_MEASURES, so we create a
			// member for internal use.
			// TODO: Expressions are not supported, yet, for members.
			LdOlap4jMember member = new LdOlap4jMember(level, memberUniqueName,
					memberName, memberCaption, "", parentUniqueName,
					memberType, childrenCardinality, memberOrdinal, map, null);
			list.add(member);
		}
	}

	static class NamedSetHandler extends HandlerImpl<XmlaOlap4jNamedSet> {
		public void handle(Element row, Context context,
				List<XmlaOlap4jNamedSet> list) {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>FoodMart</CATALOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME>
			 * <CUBE_NAME>Warehouse</CUBE_NAME> <SET_NAME>[Top
			 * Sellers]</SET_NAME> <SCOPE>1</SCOPE> </row>
			 */
			final String setName = stringElement(row, "SET_NAME");
			list.add(new XmlaOlap4jNamedSet(context.getCube(row), setName));
		}

		@Override
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<XmlaOlap4jNamedSet> list) throws OlapException {
			// TODO Auto-generated method stub

		}
	}

	static class SchemaHandler extends HandlerImpl<LdOlap4jSchema> {
		public void handle(Element row, Context context,
				List<LdOlap4jSchema> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>LOCALDB</CATLAOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME>
			 * <SCHEMA_OWNER>dbo</SCHEMA_OWNER> </row>
			 */
			String schemaName = stringElement(row, "TABLE_SCHEM");
			list.add(new LdOlap4jSchema(context.getCatalog(row),
					(schemaName == null) ? "" : schemaName));
		}

		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jSchema> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>LOCALDB</CATLAOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME>
			 * <SCHEMA_OWNER>dbo</SCHEMA_OWNER> </row>
			 */
			String schemaName = LdOlap4jUtil.convertNodeToMDX(row[mapFields
					.get("?SCHEMA_NAME")]);
			list.add(new LdOlap4jSchema(context.getCatalog(row, mapFields),
					(schemaName == null) ? "" : schemaName));
		}
	}

	static class CatalogSchemaHandler extends HandlerImpl<LdOlap4jSchema> {

		private String catalogName;

		public CatalogSchemaHandler(String catalogName) {
			super();
			if (catalogName == null) {
				throw new RuntimeException(
						"The CatalogSchemaHandler handler requires a catalog "
								+ "name.");
			}
			this.catalogName = catalogName;
		}

		public void handle(Element row, Context context,
				List<LdOlap4jSchema> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>CatalogName</CATLAOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME>
			 * <SCHEMA_OWNER>dbo</SCHEMA_OWNER> </row>
			 */

			// We are looking for a schema name from the cubes query restricted
			// on the catalog name. Some servers don't support nor include the
			// SCHEMA_NAME column in its response. If it's null, we convert it
			// to an empty string as to not cause problems later on.
			final String schemaName = stringElement(row, "TABLE_SCHEM");
			final String catalogName = stringElement(row, "TABLE_CATALOG");
			final String schemaName2 = (schemaName == null) ? "" : schemaName;
			if (this.catalogName.equals(catalogName)
					&& ((NamedList<LdOlap4jSchema>) list).get(schemaName2) == null) {
				list.add(new LdOlap4jSchema(context.getCatalog(row),
						schemaName2));
			}
		}

		/**
		 * We do not need this, as we create schemata normally.
		 */
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<LdOlap4jSchema> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>CatalogName</CATLAOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME>
			 * <SCHEMA_OWNER>dbo</SCHEMA_OWNER> </row>
			 */

			// We are looking for a schema name from the cubes query restricted
			// on the catalog name. Some servers don't support nor include the
			// SCHEMA_NAME column in its response. If it's null, we convert it
			// to an empty string as to not cause problems later on.
			final String schemaName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?TABLE_SCHEM")]);
			final String catalogName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?TABLE_CAT")]);
			final String schemaName2 = (schemaName == null) ? "" : schemaName;
			if (this.catalogName.equals(catalogName)
					&& ((NamedList<LdOlap4jSchema>) list).get(schemaName2) == null) {
				list.add(new LdOlap4jSchema(context.getCatalog(row, mapFields),
						schemaName2));
			}
		}
	}

	static class PropertyHandler extends HandlerImpl<XmlaOlap4jProperty> {

		public void handle(Element row, Context context,
				List<XmlaOlap4jProperty> list) throws OlapException {
			/*
			 * Example:
			 * 
			 * <row> <CATALOG_NAME>FoodMart</CATALOG_NAME>
			 * <SCHEMA_NAME>FoodMart</SCHEMA_NAME> <CUBE_NAME>HR</CUBE_NAME>
			 * <DIMENSION_UNIQUE_NAME>[Store]</DIMENSION_UNIQUE_NAME>
			 * <HIERARCHY_UNIQUE_NAME>[Store]</HIERARCHY_UNIQUE_NAME>
			 * <LEVEL_UNIQUE_NAME>[Store].[Store Name]</LEVEL_UNIQUE_NAME>
			 * <PROPERTY_NAME>Store Manager</PROPERTY_NAME>
			 * <PROPERTY_CAPTION>Store Manager</PROPERTY_CAPTION>
			 * <PROPERTY_TYPE>1</PROPERTY_TYPE> <DATA_TYPE>130</DATA_TYPE>
			 * <PROPERTY_CONTENT_TYPE>0</PROPERTY_CONTENT_TYPE> <DESCRIPTION>HR
			 * Cube - Store Hierarchy - Store Name Level - Store Manager
			 * Property</DESCRIPTION> </row>
			 */
			String description = stringElement(row, "DESCRIPTION");
			String uniqueName = stringElement(row, "DESCRIPTION");
			String caption = stringElement(row, "PROPERTY_CAPTION");
			String name = stringElement(row, "PROPERTY_NAME");
			Datatype datatype;

			Datatype ordinalDatatype = Datatype.getDictionary().forName(
					stringElement(row, "DATA_TYPE"));
			if (ordinalDatatype == null) {
				datatype = Datatype.getDictionary().forOrdinal(
						integerElement(row, "DATA_TYPE"));
			} else {
				datatype = ordinalDatatype;
			}

			final Integer contentTypeOrdinal = integerElement(row,
					"PROPERTY_CONTENT_TYPE");
			Property.ContentType contentType = contentTypeOrdinal == null ? null
					: Property.ContentType.getDictionary().forOrdinal(
							contentTypeOrdinal);
			int propertyType = integerElement(row, "PROPERTY_TYPE");
			Set<Property.TypeFlag> type = Property.TypeFlag.getDictionary()
					.forMask(propertyType);
			list.add(new XmlaOlap4jProperty(uniqueName, name, caption,
					description, datatype, type, contentType));
		}

		@Override
		public void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context,
				List<XmlaOlap4jProperty> list) throws OlapException {
			// TODO Auto-generated method stub

		}
	}

	/**
	 * Callback for converting LD results into metadata elements.
	 */
	interface Handler<T extends Named> {
		/**
		 * Converts an NX element from an LD result set into a metadata element
		 * and appends it to a list of metadata elements.
		 * 
		 * @param row
		 *            NX element
		 * 
		 * @param context
		 *            Context (schema, cube, dimension, etc.) that the request
		 *            was executed in and that the element will belong to
		 * 
		 * @param list
		 *            List of metadata elements to append new metadata element
		 * 
		 * @throws OlapException
		 *             on error
		 */
		void handle(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields, Context context, List<T> list)
				throws OlapException;

		/**
		 * Sorts a list of metadata elements.
		 * 
		 * <p>
		 * For most element types, the order returned by XMLA is correct, and
		 * this method will no-op.
		 * 
		 * @param list
		 *            List of metadata elements
		 */
		void sortList(List<T> list);
	}

	static abstract class HandlerImpl<T extends Named> implements Handler<T> {
		public void sortList(List<T> list) {
			// do nothing - assume XMLA returned list in correct order
		}
	}
	
	static class Restrictions {
		// Restrictions
		public String catalog = null;
		public String schemaPattern = null;
		public String cubeNamePattern = null;
		public String dimensionUniqueName = null;
		public String hierarchyUniqueName = null;
		public String levelUniqueName = null;
		public String memberUniqueName = null;
		public Integer tree = null;
		public Set<Member.TreeOp> treeOps = null;
		
		private Restrictions(Object[] restrictions) {

			for (int i = 0; i < restrictions.length; i = i + 2) {
				if ("CATALOG_NAME".equals((String) restrictions[i])) {
					catalog = (String) restrictions[i + 1];
					// we do not consider catalogs for now.
					continue;
				}
				if ("SCHEMA_NAME".equals((String) restrictions[i])) {
					schemaPattern = (String) restrictions[i + 1];
					// we do not consider schema for now
					continue;
				}
				if ("CUBE_NAME".equals((String) restrictions[i])) {
					cubeNamePattern = (String) restrictions[i + 1];
					continue;
				}
				if ("DIMENSION_UNIQUE_NAME".equals((String) restrictions[i])) {
					dimensionUniqueName = (String) restrictions[i + 1];
					continue;
				}
				if ("HIERARCHY_UNIQUE_NAME".equals((String) restrictions[i])) {
					hierarchyUniqueName = (String) restrictions[i + 1];
					continue;
				}
				if ("LEVEL_UNIQUE_NAME".equals((String) restrictions[i])) {
					levelUniqueName = (String) restrictions[i + 1];
					continue;
				}
				if ("MEMBER_UNIQUE_NAME".equals((String) restrictions[i])) {
					memberUniqueName = (String) restrictions[i + 1];
					continue;
				}
				if ("TREE_OP".equals((String) restrictions[i])) {
					tree = new Integer((String) restrictions[i + 1]);
					// treeOps erstellen wie in OpenVirtuoso
					continue;
				}
			}
		}

		private void resetRestrictions() {
			// Restrictions
			catalog = null;
			schemaPattern = null;
			cubeNamePattern = null;
			dimensionUniqueName = null;
			hierarchyUniqueName = null;
			levelUniqueName = null;
			memberUniqueName = null;
			tree = null;
			treeOps = null;
		}
	}

	static class Context {
		final LdOlap4jConnection olap4jConnection;
		final LdOlap4jDatabaseMetaData olap4jDatabaseMetaData;
		final LdOlap4jCatalog olap4jCatalog;
		final LdOlap4jSchema olap4jSchema;
		final LdOlap4jCube olap4jCube;
		final LdOlap4jDimension olap4jDimension;
		final LdOlap4jHierarchy olap4jHierarchy;
		final LdOlap4jLevel olap4jLevel;

		/**
		 * Creates a Context.
		 * 
		 * The context is re-written in order to deal with NXNodes.
		 * 
		 * @param olap4jConnection
		 *            Connection (must not be null)
		 * @param olap4jDatabaseMetaData
		 *            DatabaseMetaData (may be null)
		 * @param olap4jCatalog
		 *            Catalog (may be null if DatabaseMetaData is null)
		 * @param olap4jSchema
		 *            Schema (may be null if Catalog is null)
		 * @param olap4jCube
		 *            Cube (may be null if Schema is null)
		 * @param olap4jDimension
		 *            Dimension (may be null if Cube is null)
		 * @param olap4jHierarchy
		 *            Hierarchy (may be null if Dimension is null)
		 * @param olap4jLevel
		 *            Level (may be null if Hierarchy is null)
		 */
		Context(LdOlap4jConnection olap4jConnection,
				LdOlap4jDatabaseMetaData olap4jDatabaseMetaData,
				LdOlap4jCatalog olap4jCatalog, LdOlap4jSchema olap4jSchema,
				LdOlap4jCube olap4jCube, LdOlap4jDimension olap4jDimension,
				LdOlap4jHierarchy olap4jHierarchy, LdOlap4jLevel olap4jLevel) {
			this.olap4jConnection = olap4jConnection;
			this.olap4jDatabaseMetaData = olap4jDatabaseMetaData;
			this.olap4jCatalog = olap4jCatalog;
			this.olap4jSchema = olap4jSchema;
			this.olap4jCube = olap4jCube;
			this.olap4jDimension = olap4jDimension;
			this.olap4jHierarchy = olap4jHierarchy;
			this.olap4jLevel = olap4jLevel;
			assert (olap4jDatabaseMetaData != null || olap4jCatalog == null)
					&& (olap4jCatalog != null || olap4jSchema == null)
					&& (olap4jSchema != null || olap4jCube == null)
					&& (olap4jCube != null || olap4jDimension == null)
					&& (olap4jDimension != null || olap4jHierarchy == null)
					&& (olap4jHierarchy != null || olap4jLevel == null);
		}

		/**
		 * Shorthand way to create a Context at Cube level or finer.
		 * 
		 * @param olap4jCube
		 *            Cube (must not be null)
		 * @param olap4jDimension
		 *            Dimension (may be null)
		 * @param olap4jHierarchy
		 *            Hierarchy (may be null if Dimension is null)
		 * @param olap4jLevel
		 *            Level (may be null if Hierarchy is null)
		 */
		Context(LdOlap4jCube olap4jCube, LdOlap4jDimension olap4jDimension,
				LdOlap4jHierarchy olap4jHierarchy, LdOlap4jLevel olap4jLevel) {
			this(
					olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection,
					olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData,
					olap4jCube.olap4jSchema.olap4jCatalog,
					olap4jCube.olap4jSchema, olap4jCube, olap4jDimension,
					olap4jHierarchy, olap4jLevel);
		}

		/**
		 * Shorthand way to create a Context at Level level.
		 * 
		 * @param olap4jLevel
		 *            Level (must not be null)
		 */
		Context(LdOlap4jLevel olap4jLevel) {
			this(olap4jLevel.olap4jHierarchy.olap4jDimension.olap4jCube,
					olap4jLevel.olap4jHierarchy.olap4jDimension,
					olap4jLevel.olap4jHierarchy, olap4jLevel);
		}

		LdOlap4jHierarchy getHierarchy(Element row) {
			if (olap4jHierarchy != null) {
				return olap4jHierarchy;
			}
			final String hierarchyUniqueName = stringElement(row,
					"HIERARCHY_UNIQUE_NAME");
			LdOlap4jHierarchy hierarchy = getCube(row).hierarchiesByUname
					.get(hierarchyUniqueName);
			if (hierarchy == null) {
				// Apparently, the code has requested a member that is
				// not queried for yet. We must force the initialization
				// of the dimension tree first.
				final String dimensionUniqueName = stringElement(row,
						"DIMENSION_UNIQUE_NAME");
				String dimensionName = Olap4jUtil.parseUniqueName(
						dimensionUniqueName).get(0);
				LdOlap4jDimension dimension = getCube(row).dimensions
						.get(dimensionName);
				dimension.getHierarchies().size();
				// Now we attempt to resolve again
				hierarchy = getCube(row).hierarchiesByUname
						.get(hierarchyUniqueName);
			}
			return hierarchy;
		}

		LdOlap4jHierarchy getHierarchy(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields) {
			if (olap4jHierarchy != null) {
				return olap4jHierarchy;
			}
			final String hierarchyUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields
							.get("?HIERARCHY_UNIQUE_NAME")]);

			LdOlap4jHierarchy hierarchy = getCube(row, mapFields).hierarchiesByUname
					.get(hierarchyUniqueName);
			if (hierarchy == null) {
				// Apparently, the code has requested a member that is
				// not queried for yet. We must force the initialization
				// of the dimension tree first.
				final String dimensionUniqueName = LdOlap4jUtil
						.convertNodeToMDX(row[mapFields
								.get("?DIMENSION_UNIQUE_NAME")]);
				// TODO: For a specific cube, we need the local name of the
				// dimension, which in our case however always is the unique
				// name, so far.
				// String dimensionName = Olap4jUtil.parseUniqueName(
				// dimensionUniqueName).get(0);
				String dimensionName = dimensionUniqueName;
				LdOlap4jDimension dimension = getCube(row, mapFields).dimensions
						.get(dimensionName);
				dimension.getHierarchies().size();
				// Now we attempt to resolve again
				hierarchy = getCube(row, mapFields).hierarchiesByUname
						.get(hierarchyUniqueName);
			}
			return hierarchy;
		}

		LdOlap4jCube getCube(Element row) {
			if (olap4jCube != null) {
				return olap4jCube;
			}
			throw new UnsupportedOperationException(); // todo:
		}

		LdOlap4jCube getCube(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields) {
			if (olap4jCube != null) {
				return olap4jCube;
			}
			throw new UnsupportedOperationException(); // todo:
		}

		LdOlap4jDimension getDimension(Element row) {
			if (olap4jDimension != null) {
				return olap4jDimension;
			}
			final String dimensionUniqueName = stringElement(row,
					"DIMENSION_UNIQUE_NAME");
			LdOlap4jDimension dimension = getCube(row).dimensionsByUname
					.get(dimensionUniqueName);
			// Apparently, the code has requested a member that is
			// not queried for yet.
			if (dimension == null) {
				final String dimensionName = stringElement(row,
						"DIMENSION_NAME");
				return getCube(row).dimensions.get(dimensionName);
			}
			return dimension;
		}

		LdOlap4jDimension getDimension(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields) {
			if (olap4jDimension != null) {
				return olap4jDimension;
			}
			final String dimensionUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields
							.get("?DIMENSION_UNIQUE_NAME")]);
			LdOlap4jDimension dimension = getCube(row, mapFields).dimensionsByUname
					.get(dimensionUniqueName);
			// Apparently, the code has requested a member that is
			// not queried for yet.
			if (dimension == null) {
				final String dimensionName = LdOlap4jUtil
						.convertNodeToMDX(row[mapFields.get("?DIMENSION_NAME")]);
				return getCube(row, mapFields).dimensions.get(dimensionName);
			}
			return dimension;
		}

		public LdOlap4jLevel getLevel(Element row) {
			if (olap4jLevel != null) {
				return olap4jLevel;
			}
			final String levelUniqueName = stringElement(row,
					"LEVEL_UNIQUE_NAME");
			LdOlap4jLevel level = getCube(row).levelsByUname
					.get(levelUniqueName);
			if (level == null) {
				// Apparently, the code has requested a member that is
				// not queried for yet. We must force the initialization
				// of the dimension tree first.
				final String dimensionUniqueName = stringElement(row,
						"DIMENSION_UNIQUE_NAME");
				String dimensionName = Olap4jUtil.parseUniqueName(
						dimensionUniqueName).get(0);
				LdOlap4jDimension dimension = getCube(row).dimensions
						.get(dimensionName);
				for (Hierarchy hierarchyInit : dimension.getHierarchies()) {
					hierarchyInit.getLevels().size();
				}
				// Now we attempt to resolve again
				level = getCube(row).levelsByUname.get(levelUniqueName);
			}
			return level;
		}

		public LdOlap4jLevel getLevel(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields) {
			if (olap4jLevel != null) {
				return olap4jLevel;
			}
			final String levelUniqueName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?LEVEL_UNIQUE_NAME")]);
			LdOlap4jLevel level = getCube(row, mapFields).levelsByUname
					.get(levelUniqueName);
			if (level == null) {
				// Apparently, the code has requested a member that is
				// not queried for yet. We must force the initialization
				// of the dimension tree first.
				final String dimensionUniqueName = LdOlap4jUtil
						.convertNodeToMDX(row[mapFields
								.get("?DIMENSION_UNIQUE_NAME")]);
				// TODO: For a specific cube, we need the local name of the
				// dimension, which in our case however always is the unique
				// name, so far.
				// String dimensionName = Olap4jUtil.parseUniqueName(
				// dimensionUniqueName).get(0);
				String dimensionName = dimensionUniqueName;
				LdOlap4jCube cube = getCube(row, mapFields);
				LdOlap4jDimension dimension = cube.dimensions
						.get(dimensionName);
				for (Hierarchy hierarchyInit : dimension.getHierarchies()) {
					hierarchyInit.getLevels().size();
				}
				// Now we attempt to resolve again
				level = getCube(row, mapFields).levelsByUname
						.get(levelUniqueName);
			}
			return level;
		}

		public LdOlap4jCatalog getCatalog(Element row) throws OlapException {
			if (olap4jCatalog != null) {
				return olap4jCatalog;
			}
			final String catalogName = stringElement(row, "CATALOG_NAME");
			return (LdOlap4jCatalog) olap4jConnection.getOlapCatalogs().get(
					catalogName);
		}

		public LdOlap4jCatalog getCatalog(org.semanticweb.yars.nx.Node[] row,
				Map<String, Integer> mapFields) throws OlapException {
			if (olap4jCatalog != null) {
				return olap4jCatalog;
			}
			final String catalogName = LdOlap4jUtil
					.convertNodeToMDX(row[mapFields.get("?CATALOG_NAME")]);
			return (LdOlap4jCatalog) olap4jConnection.getOlapCatalogs().get(
					catalogName);
		}
	}

	/**
	 * Here, the different methods, XMLA provides are defined.
	 * 
	 * @author b-kaempgen
	 * 
	 */
	enum MetadataRequest {
		DISCOVER_DATASOURCES(new MetadataColumn("DataSourceName"),
				new MetadataColumn("DataSourceDescription"),
				new MetadataColumn("URL"),
				new MetadataColumn("DataSourceInfo"), new MetadataColumn(
						"ProviderName"), new MetadataColumn("ProviderType"),
				new MetadataColumn("AuthenticationMode")), DISCOVER_SCHEMA_ROWSETS(
				new MetadataColumn("SchemaName"), new MetadataColumn(
						"SchemaGuid"), new MetadataColumn("Restrictions"),
				new MetadataColumn("Description")), DISCOVER_ENUMERATORS(
				new MetadataColumn("EnumName"), new MetadataColumn(
						"EnumDescription"), new MetadataColumn("EnumType"),
				new MetadataColumn("ElementName"), new MetadataColumn(
						"ElementDescription"), new MetadataColumn(
						"ElementValue")), DISCOVER_PROPERTIES(
				new MetadataColumn("PropertyName"), new MetadataColumn(
						"PropertyDescription"), new MetadataColumn(
						"PropertyType"), new MetadataColumn(
						"PropertyAccessType"),
				new MetadataColumn("IsRequired"), new MetadataColumn("Value")), DISCOVER_KEYWORDS(
				new MetadataColumn("Keyword")), DISCOVER_LITERALS(
				new MetadataColumn("LiteralName"), new MetadataColumn(
						"LiteralValue"), new MetadataColumn(
						"LiteralInvalidChars"), new MetadataColumn(
						"LiteralInvalidStartingChars"), new MetadataColumn(
						"LiteralMaxLength")), DBSCHEMA_CATALOGS(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"DESCRIPTION"), new MetadataColumn("ROLES"),
				new MetadataColumn("DATE_MODIFIED")), DBSCHEMA_COLUMNS(
				new MetadataColumn("TABLE_CATALOG"), new MetadataColumn(
						"TABLE_SCHEMA"), new MetadataColumn("TABLE_NAME"),
				new MetadataColumn("COLUMN_NAME"), new MetadataColumn(
						"ORDINAL_POSITION"), new MetadataColumn(
						"COLUMN_HAS_DEFAULT"), new MetadataColumn(
						"COLUMN_FLAGS"), new MetadataColumn("IS_NULLABLE"),
				new MetadataColumn("DATA_TYPE"), new MetadataColumn(
						"CHARACTER_MAXIMUM_LENGTH"), new MetadataColumn(
						"CHARACTER_OCTET_LENGTH"), new MetadataColumn(
						"NUMERIC_PRECISION"), new MetadataColumn(
						"NUMERIC_SCALE")), DBSCHEMA_PROVIDER_TYPES(
				new MetadataColumn("TYPE_NAME"),
				new MetadataColumn("DATA_TYPE"), new MetadataColumn(
						"COLUMN_SIZE"), new MetadataColumn("LITERAL_PREFIX"),
				new MetadataColumn("LITERAL_SUFFIX"), new MetadataColumn(
						"IS_NULLABLE"), new MetadataColumn("CASE_SENSITIVE"),
				new MetadataColumn("SEARCHABLE"), new MetadataColumn(
						"UNSIGNED_ATTRIBUTE"), new MetadataColumn(
						"FIXED_PREC_SCALE"), new MetadataColumn(
						"AUTO_UNIQUE_VALUE"), new MetadataColumn("IS_LONG"),
				new MetadataColumn("BEST_MATCH")), DBSCHEMA_TABLES(
				new MetadataColumn("TABLE_CATALOG"), new MetadataColumn(
						"TABLE_SCHEMA"), new MetadataColumn("TABLE_NAME"),
				new MetadataColumn("TABLE_TYPE"), new MetadataColumn(
						"TABLE_GUID"), new MetadataColumn("DESCRIPTION"),
				new MetadataColumn("TABLE_PROPID"), new MetadataColumn(
						"DATE_CREATED"), new MetadataColumn("DATE_MODIFIED")), DBSCHEMA_TABLES_INFO(
				new MetadataColumn("TABLE_CATALOG"), new MetadataColumn(
						"TABLE_SCHEMA"), new MetadataColumn("TABLE_NAME"),
				new MetadataColumn("TABLE_TYPE"), new MetadataColumn(
						"TABLE_GUID"), new MetadataColumn("BOOKMARKS"),
				new MetadataColumn("BOOKMARK_TYPE"), new MetadataColumn(
						"BOOKMARK_DATATYPE"), new MetadataColumn(
						"BOOKMARK_MAXIMUM_LENGTH"), new MetadataColumn(
						"BOOKMARK_INFORMATION"), new MetadataColumn(
						"TABLE_VERSION"), new MetadataColumn("CARDINALITY"),
				new MetadataColumn("DESCRIPTION"), new MetadataColumn(
						"TABLE_PROPID")), DBSCHEMA_SCHEMATA(new MetadataColumn(
				"CATALOG_NAME"), new MetadataColumn("SCHEMA_NAME"),
				new MetadataColumn("SCHEMA_OWNER")), MDSCHEMA_ACTIONS(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("ACTION_NAME"), new MetadataColumn(
						"COORDINATE"), new MetadataColumn("COORDINATE_TYPE")), MDSCHEMA_CUBES(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("CUBE_TYPE"),
				new MetadataColumn("CUBE_GUID"), new MetadataColumn(
						"CREATED_ON"),
				new MetadataColumn("LAST_SCHEMA_UPDATE"), new MetadataColumn(
						"SCHEMA_UPDATED_BY"), new MetadataColumn(
						"LAST_DATA_UPDATE"), new MetadataColumn(
						"DATA_UPDATED_BY"), new MetadataColumn(
						"IS_DRILLTHROUGH_ENABLED"), new MetadataColumn(
						"IS_WRITE_ENABLED"), new MetadataColumn("IS_LINKABLE"),
				new MetadataColumn("IS_SQL_ENABLED"), new MetadataColumn(
						"DESCRIPTION"), new MetadataColumn("CUBE_CAPTION"),
				new MetadataColumn("BASE_CUBE_NAME")), MDSCHEMA_DIMENSIONS(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("DIMENSION_NAME"), new MetadataColumn(
						"DIMENSION_UNIQUE_NAME"), new MetadataColumn(
						"DIMENSION_GUID"), new MetadataColumn(
						"DIMENSION_CAPTION"), new MetadataColumn(
						"DIMENSION_ORDINAL"), new MetadataColumn(
						"DIMENSION_TYPE"), new MetadataColumn(
						"DIMENSION_CARDINALITY"), new MetadataColumn(
						"DEFAULT_HIERARCHY"),
				new MetadataColumn("DESCRIPTION"), new MetadataColumn(
						"IS_VIRTUAL"), new MetadataColumn("IS_READWRITE"),
				new MetadataColumn("DIMENSION_UNIQUE_SETTINGS"),
				new MetadataColumn("DIMENSION_MASTER_UNIQUE_NAME"),
				new MetadataColumn("DIMENSION_IS_VISIBLE")), MDSCHEMA_FUNCTIONS(
				new MetadataColumn("FUNCTION_NAME"), new MetadataColumn(
						"DESCRIPTION"), new MetadataColumn("PARAMETER_LIST"),
				new MetadataColumn("RETURN_TYPE"),
				new MetadataColumn("ORIGIN"), new MetadataColumn(
						"INTERFACE_NAME"), new MetadataColumn("LIBRARY_NAME"),
				new MetadataColumn("CAPTION")), MDSCHEMA_HIERARCHIES(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("DIMENSION_UNIQUE_NAME"),
				new MetadataColumn("HIERARCHY_NAME"), new MetadataColumn(
						"HIERARCHY_UNIQUE_NAME"), new MetadataColumn(
						"HIERARCHY_GUID"), new MetadataColumn(
						"HIERARCHY_CAPTION"), new MetadataColumn(
						"DIMENSION_TYPE"), new MetadataColumn(
						"HIERARCHY_CARDINALITY"), new MetadataColumn(
						"DEFAULT_MEMBER"), new MetadataColumn("ALL_MEMBER"),
				new MetadataColumn("DESCRIPTION"), new MetadataColumn(
						"STRUCTURE"), new MetadataColumn("IS_VIRTUAL"),
				new MetadataColumn("IS_READWRITE"), new MetadataColumn(
						"DIMENSION_UNIQUE_SETTINGS"), new MetadataColumn(
						"DIMENSION_IS_VISIBLE"), new MetadataColumn(
						"HIERARCHY_IS_VISIBLE"), new MetadataColumn(
						"HIERARCHY_ORDINAL"), new MetadataColumn(
						"DIMENSION_IS_SHARED"), new MetadataColumn(
						"PARENT_CHILD")), MDSCHEMA_LEVELS(new MetadataColumn(
				"CATALOG_NAME"), new MetadataColumn("SCHEMA_NAME"),
				new MetadataColumn("CUBE_NAME"), new MetadataColumn(
						"DIMENSION_UNIQUE_NAME"), new MetadataColumn(
						"HIERARCHY_UNIQUE_NAME"), new MetadataColumn(
						"LEVEL_NAME"), new MetadataColumn("LEVEL_UNIQUE_NAME"),
				new MetadataColumn("LEVEL_GUID"), new MetadataColumn(
						"LEVEL_CAPTION"), new MetadataColumn("LEVEL_NUMBER"),
				new MetadataColumn("LEVEL_CARDINALITY"), new MetadataColumn(
						"LEVEL_TYPE"), new MetadataColumn(
						"CUSTOM_ROLLUP_SETTINGS"), new MetadataColumn(
						"LEVEL_UNIQUE_SETTINGS"), new MetadataColumn(
						"LEVEL_IS_VISIBLE"), new MetadataColumn("DESCRIPTION")), MDSCHEMA_MEASURES(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("MEASURE_NAME"), new MetadataColumn(
						"MEASURE_UNIQUE_NAME"), new MetadataColumn(
						"MEASURE_CAPTION"), new MetadataColumn("MEASURE_GUID"),
				new MetadataColumn("MEASURE_AGGREGATOR"), new MetadataColumn(
						"DATA_TYPE"), new MetadataColumn("MEASURE_IS_VISIBLE"),
				new MetadataColumn("LEVELS_LIST"), new MetadataColumn(
						"DESCRIPTION")), MDSCHEMA_MEMBERS(new MetadataColumn(
				"CATALOG_NAME"), new MetadataColumn("SCHEMA_NAME"),
				new MetadataColumn("CUBE_NAME"), new MetadataColumn(
						"DIMENSION_UNIQUE_NAME"), new MetadataColumn(
						"HIERARCHY_UNIQUE_NAME"), new MetadataColumn(
						"LEVEL_UNIQUE_NAME"),
				new MetadataColumn("LEVEL_NUMBER"), new MetadataColumn(
						"MEMBER_ORDINAL"), new MetadataColumn("MEMBER_NAME"),
				new MetadataColumn("MEMBER_UNIQUE_NAME"), new MetadataColumn(
						"MEMBER_TYPE"), new MetadataColumn("MEMBER_GUID"),
				new MetadataColumn("MEMBER_CAPTION"), new MetadataColumn(
						"CHILDREN_CARDINALITY"), new MetadataColumn(
						"PARENT_LEVEL"), new MetadataColumn(
						"PARENT_UNIQUE_NAME"), new MetadataColumn(
						"PARENT_COUNT"), new MetadataColumn("TREE_OP"),
				new MetadataColumn("DEPTH")), MDSCHEMA_PROPERTIES(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("DIMENSION_UNIQUE_NAME"),
				new MetadataColumn("HIERARCHY_UNIQUE_NAME"),
				new MetadataColumn("LEVEL_UNIQUE_NAME"), new MetadataColumn(
						"MEMBER_UNIQUE_NAME"), new MetadataColumn(
						"PROPERTY_NAME"),
				new MetadataColumn("PROPERTY_CAPTION"), new MetadataColumn(
						"PROPERTY_TYPE"), new MetadataColumn("DATA_TYPE"),
				new MetadataColumn("PROPERTY_CONTENT_TYPE"),
				new MetadataColumn("DESCRIPTION")), MDSCHEMA_SETS(
				new MetadataColumn("CATALOG_NAME"), new MetadataColumn(
						"SCHEMA_NAME"), new MetadataColumn("CUBE_NAME"),
				new MetadataColumn("SET_NAME"), new MetadataColumn("SCOPE"));

		final List<MetadataColumn> columns;
		final Map<String, MetadataColumn> columnsByName;

		/**
		 * Creates a MetadataRequest.
		 * 
		 * Note: DBSCHEMA_CATALOGS and DBSCHEMA_SCHEMATA are special w.r.t. the
		 * columns.
		 * 
		 * @param columns
		 *            Columns
		 */
		MetadataRequest(MetadataColumn... columns) {
			if (name().equals("DBSCHEMA_CATALOGS")) {
				// DatabaseMetaData.getCatalogs() is defined by JDBC not XMLA,
				// so has just one column. Ignore the 4 columns from XMLA.
				columns = new MetadataColumn[] { new MetadataColumn(
						"CATALOG_NAME", "TABLE_CAT") };
			} else if (name().equals("DBSCHEMA_SCHEMATA")) {
				// DatabaseMetaData.getCatalogs() is defined by JDBC not XMLA,
				// so has just one column. Ignore the 4 columns from XMLA.
				columns = new MetadataColumn[] {
						new MetadataColumn("SCHEMA_NAME", "TABLE_SCHEM"),
						new MetadataColumn("CATALOG_NAME", "TABLE_CAT") };
			}
			this.columns = UnmodifiableArrayList.asCopyOf(columns);
			final Map<String, MetadataColumn> map = new HashMap<String, MetadataColumn>();
			for (MetadataColumn column : columns) {
				map.put(column.name, column);
			}
			this.columnsByName = Collections.unmodifiableMap(map);
		}

		/**
		 * Returns whether this request requires a
		 * {@code &lt;DatasourceName&gt;} element.
		 * 
		 * @return whether this request requires a DatasourceName element
		 */
		public boolean requiresDatasourceName() {
			return this != DISCOVER_DATASOURCES;
		}

		/**
		 * Returns whether this request requires a {@code &lt;CatalogName&gt;}
		 * element.
		 * 
		 * @return whether this request requires a CatalogName element
		 */
		public boolean requiresCatalogName() {
			// If we don't specifiy CatalogName in the properties of an
			// MDSCHEMA_FUNCTIONS request, Mondrian's XMLA provider will give
			// us the whole set of functions multiplied by the number of
			// catalogs. JDBC (and Mondrian) assumes that functions belong to a
			// catalog whereas XMLA (and SSAS) assume that functions belong to
			// the database. Always specifying a catalog is the easiest way to
			// reconcile them.
			return this == MDSCHEMA_FUNCTIONS;
		}

		/**
		 * Returns whether this request allows a {@code &lt;CatalogName&gt;}
		 * element in the properties section of the request. Even for requests
		 * that allow it, it is usually optional.
		 * 
		 * @return whether this request allows a CatalogName element
		 */
		public boolean allowsCatalogName() {
			return true;
		}

		/**
		 * Returns the column with a given name, or null if there is no such
		 * column.
		 * 
		 * @param name
		 *            Column name
		 * @return Column, or null if not found
		 */
		public MetadataColumn getColumn(String name) {
			return columnsByName.get(name);
		}

		public boolean allowsLocale() {
			return name().startsWith("MDSCHEMA");
		}
	}

	private static final Pattern LOWERCASE_PATTERN = Pattern
			.compile(".*[a-z].*");

	static class MetadataColumn {
		final String name;
		final String xmlaName;

		MetadataColumn(String xmlaName, String name) {
			this.xmlaName = xmlaName;
			this.name = name;
		}

		MetadataColumn(String xmlaName) {
			this.xmlaName = xmlaName;
			String name = xmlaName;
			if (LOWERCASE_PATTERN.matcher(name).matches()) {
				name = Olap4jUtil.camelToUpper(name);
			}
			// VALUE is a SQL reserved word
			if (name.equals("VALUE")) {
				name = "PROPERTY_VALUE";
			}
			this.name = name;
		}
	}

	private static class XmlaOlap4jMdxValidator implements MdxValidator {
		private final OlapConnection connection;

		XmlaOlap4jMdxValidator(OlapConnection connection) {
			this.connection = connection;
		}

		public SelectNode validateSelect(SelectNode selectNode)
				throws OlapException {
			StringWriter sw = new StringWriter();
			selectNode.unparse(new ParseTreeWriter(sw));
			String mdx = sw.toString();
			final LdOlap4jConnection olap4jConnection = (LdOlap4jConnection) connection;
			return selectNode;
		}
	}
}

// End XmlaOlap4jConnection.java
