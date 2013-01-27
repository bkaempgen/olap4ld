/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.ld;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.olap4j.CellSetListener;
import org.olap4j.OlapConnection;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Member.TreeOp;
import org.semanticweb.yars.nx.Node;

/**
 * Implementation of {@link org.olap4j.OlapDatabaseMetaData} for XML/A
 * providers.
 * 
 * <p>
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs; it is
 * instantiated using {@link Factory#newDatabaseMetaData}.
 * </p>
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jDatabaseMetaData.java 455 2011-05-24 10:01:26Z jhyde
 *          $
 * @since May 23, 2007
 */
abstract class LdOlap4jDatabaseMetaData implements OlapDatabaseMetaData {
	final LdOlap4jConnection olap4jConnection;

	/**
	 * Creates an XmlaOlap4jDatabaseMetaData.
	 * 
	 * <p>
	 * Note that this constructor should make zero non-trivial calls, which
	 * could cause deadlocks due to java.sql.DriverManager synchronization
	 * issues.
	 * 
	 * @param olap4jConnection
	 *            Connection
	 */
	LdOlap4jDatabaseMetaData(LdOlap4jConnection olap4jConnection) {
		this.olap4jConnection = olap4jConnection;
	}

	/**
	 * Own class for returning metadata (without overrides).
	 * 
	 * @param metadataRequest
	 * @param patternValues
	 * @return
	 * @throws OlapException
	 */
	private ResultSet getMetadataLd(
			LdOlap4jConnection.MetadataRequest metadataRequest,
			Object... patternValues) throws OlapException {

		// create empty overrides.
		final Map<LdOlap4jConnection.MetadataColumn, String> overrides = Collections
				.emptyMap();
		// return getMetadata(metadataRequest, overrides, patternValues);
		return getMetadataLd(metadataRequest, overrides, patternValues);
	}

	/**
	 * Executes a metadata query and returns the result as a JDBC
	 * {@link ResultSet}.
	 * 
	 * Here, three functionalities are implemented:
	 * 
	 * - executeMetadataRequestOnLd works only on filter values and context
	 * (which is empty) - after executeMetadataRequestOnLd, we filter for wild
	 * cards - we override values - and we set values after transforming them
	 * into MDX compliant format
	 * 
	 * @param metadataRequest
	 *            Name of the metadata request. Corresponds to the XMLA method
	 *            name, e.g. "MDSCHEMA_CUBES"
	 * 
	 * @param overrides
	 *            Map of metadata columns to forced values. Used to override the
	 *            value returned by the server for a list of columns.
	 * 
	 * @param patternValues
	 *            Array of alternating parameter name and value pairs. If the
	 *            parameter value is null, it is ignored.
	 * 
	 * @return Result set of metadata
	 * 
	 * @throws org.olap4j.OlapException
	 *             on error
	 */
	private ResultSet getMetadataLd(
			LdOlap4jConnection.MetadataRequest metadataRequest,
			Map<LdOlap4jConnection.MetadataColumn, String> overrides,
			Object... patternValues) throws OlapException {
		// Since pattern values oscillate between name and value, it needs to be
		// modulo 2 = 0
		assert patternValues.length % 2 == 0;

		// Context is simply the connection
		final LdOlap4jConnection.Context context = new LdOlap4jConnection.Context(
				olap4jConnection, null, null, null, null, null, null, null);

		/*
		 * Here, all patterns are gone through and (possibly sql) restrictions
		 * are created
		 * 
		 * In LD, we don't have such sql, therefore difficult.
		 */
		// For filter values
		List<String> patternValueList = new ArrayList<String>();
		// For wildcard values
		Map<String, Matcher> predicateList = new HashMap<String, Matcher>();
		for (int i = 0; i < patternValues.length; i += 2) {
			String name = (String) patternValues[i];
			assert metadataRequest.getColumn(name) != null : "Request '"
					+ metadataRequest + "' does not support column '" + name
					+ "'";
			Object value = patternValues[i + 1];
			// Now, any result should match this value
			if (value == null) {
				// ignore, no restriction
			} else if (value instanceof Wildcard) {
				final Wildcard wildcard = (Wildcard) value;

				// For now, I switch off wildcards.
				if (true || (wildcard.pattern.indexOf('%') < 0 && wildcard.pattern
						.indexOf('_') < 0)) {
					// If no wildcard characters are used, simply use pattern
					patternValueList.add(name);
					patternValueList.add(wildcard.pattern);
				} else {
					/*
					 * If wildcard is used, create java regex. Here, not XMLA is
					 * doing the filtering, but we are doing it hereafter.
					 */
					String regexp = Olap4jUtil.wildcardToRegexp(Collections
							.singletonList(wildcard.pattern));
					final Matcher matcher = Pattern.compile(regexp).matcher("");
					predicateList.put(name, matcher);
				}
			} else {
				patternValueList.add(name);
				patternValueList.add((String) value);
			}
		}

		// Request was created before, here, it is generated and then executed.
		List<Node[]> root = ((LdOlap4jConnection) olap4jConnection)
				.executeMetadataRequestOnLd(context, metadataRequest,
						patternValueList.toArray(new String[patternValueList
								.size()]));

		// String request = olap4jConnection.generateRequest(context,
		// metadataRequest,
		// patternValueList.toArray(new String[patternValueList.size()]));
		//
		// // Get XML
		// final Element root =
		// olap4jConnection.executeMetadataRequest(request);

		// Now, the results are worked with
		List<List<Object>> rowList = new ArrayList<List<Object>>();
		boolean isFirst = true;
		Map<String, Integer> mapFields = new HashMap<String, Integer>();
		rowLoop: for (Node[] row : root) {
			// We do not need to ignore any of the rows apart from the first
			// The first Node gives the fields
			if (isFirst) {
				mapFields = LdOlap4jUtil.getNodeResultFields(row);
				isFirst = false;
				continue;
			}
			final ArrayList<Object> valueList = new ArrayList<Object>();

			// Here, we simply check on the wildcard patterns
			for (Map.Entry<String, Matcher> entry : predicateList.entrySet()) {
				// For each pattern for columns
				final String column = entry.getKey();
				// final String value = XmlaOlap4jUtil.stringElement(row,
				// column);
				/*
				 * MetadataRequest uses certain column names which are also
				 * returned by our LD. If not returned, we simply put in an
				 * empty value.
				 * 
				 * Existing values we convert into MDX format.
				 */
				String columName = "?" + column;
				final String value;
				if (mapFields.containsKey(columName)) {
					int index = mapFields.get(columName);

					value = LdOlap4jUtil.convertNodeToMDX(row[index]);
				} else {
					value = "";
				}
				final Matcher matcher = entry.getValue();
				// If any matching does not succeed (if pattern does not match),
				// we
				// jump further processing.
				if (!matcher.reset(value).matches()) {
					continue rowLoop;
				} else {
					// in this case we continue with this row
				}
			}

			// For each column that is required, either override it, set it or
			// set it to "".
			for (LdOlap4jConnection.MetadataColumn column : metadataRequest.columns) {
				// override possible values
				if (overrides.containsKey(column)) {
					valueList.add(overrides.get(column));
				} else {

					// Get the value as used by xmla
					// final String value = XmlaOlap4jUtil.stringElement(row,
					// column.xmlaName);
					// MetadataRequest uses certain column names which are also
					// returned by our LD
					String columName = "?" + column.name;
					final String value;
					if (mapFields.containsKey(columName)) {
						int index = mapFields.get(columName);

						value = LdOlap4jUtil.convertNodeToMDX(row[index]);
					} else {
						value = "";
					}
					valueList.add(value);
				}
			}
			rowList.add(valueList);
		}

		// Create headers for the result row
		List<String> headerList = new ArrayList<String>();
		for (LdOlap4jConnection.MetadataColumn column : metadataRequest.columns) {
			headerList.add(column.name);
		}

		return olap4jConnection.factory.newFixedResultSet(olap4jConnection,
				headerList, rowList);
	}

	/**
	 * Converts a string to a wildcard object.
	 * 
	 * @param pattern
	 *            String pattern
	 * @return wildcard object, or null if pattern was null
	 */
	private Wildcard wildcard(String pattern) {
		return pattern == null ? null : new Wildcard(pattern);
	}

	// implement DatabaseMetaData

	public boolean allProceduresAreCallable() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean allTablesAreSelectable() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getURL() throws SQLException {
		return olap4jConnection.getURL();
	}

	public String getUserName() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isReadOnly() throws SQLException {
		// olap4j does not currently support writeback
		return true;
	}

	public boolean nullsAreSortedHigh() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean nullsAreSortedLow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean nullsAreSortedAtStart() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean nullsAreSortedAtEnd() throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Uses getDatabaseProperties.
	 * 
	 * Just take the first of our LinkedDataEngine.
	 * 
	 */
	public String getDatabaseProductName() throws SQLException {
		return LdOlap4jUtil.convertNodeToMDX(olap4jConnection.myLinkedData
				.getDatabases().get(0)[0]);
		// final ResultSet rs = this.getDatabaseProperties(null, null);
		// try {
		// while (rs.next()) {
		// if (rs.getString(XmlaConstants.Literal.PROPERTY_NAME.name())
		// .equals("ProviderName")) {
		// return rs.getString("PROPERTY_VALUE");
		// }
		// }
		// return "";
		// } finally {
		// rs.close();
		// }
	}

	public String getDatabaseProductVersion() throws SQLException {
		return LdOlap4jUtil.convertNodeToMDX(olap4jConnection.myLinkedData
				.getDatabases().get(0)[3]);
		// final ResultSet rs = this.getDatabaseProperties(null, null);
		// try {
		// while (rs.next()) {
		// if (rs.getString(XmlaConstants.Literal.PROPERTY_NAME.name())
		// .equals("ProviderVersion")) {
		// return rs.getString("PROPERTY_VALUE");
		// }
		// }
		// return "";
		// } finally {
		// rs.close();
		// }
	}

	public String getDriverName() throws SQLException {
		return olap4jConnection.driver.getName();
	}

	public String getDriverVersion() throws SQLException {
		return olap4jConnection.driver.getVersion();
	}

	public int getDriverMajorVersion() {
		return olap4jConnection.driver.getMajorVersion();
	}

	public int getDriverMinorVersion() {
		return olap4jConnection.driver.getMinorVersion();
	}

	public boolean usesLocalFiles() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean usesLocalFilePerTable() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean storesUpperCaseIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean storesLowerCaseIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean storesMixedCaseIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getIdentifierQuoteString() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getSQLKeywords() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getNumericFunctions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getStringFunctions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getSystemFunctions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getTimeDateFunctions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getSearchStringEscape() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getExtraNameCharacters() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsColumnAliasing() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean nullPlusNonNullIsNull() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsConvert() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsTableCorrelationNames() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsExpressionsInOrderBy() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsOrderByUnrelated() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsGroupBy() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsGroupByUnrelated() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsGroupByBeyondSelect() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsLikeEscapeClause() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsMultipleResultSets() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsMultipleTransactions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsNonNullableColumns() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsMinimumSQLGrammar() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCoreSQLGrammar() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsExtendedSQLGrammar() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsANSI92FullSQL() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsOuterJoins() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsFullOuterJoins() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsLimitedOuterJoins() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getSchemaTerm() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getProcedureTerm() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getCatalogTerm() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isCatalogAtStart() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getCatalogSeparator() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSchemasInDataManipulation() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsPositionedDelete() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsPositionedUpdate() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSelectForUpdate() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsStoredProcedures() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSubqueriesInComparisons() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSubqueriesInExists() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSubqueriesInIns() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsCorrelatedSubqueries() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsUnion() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsUnionAll() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxBinaryLiteralLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxCharLiteralLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxColumnNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxColumnsInGroupBy() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxColumnsInIndex() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxColumnsInOrderBy() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxColumnsInSelect() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxColumnsInTable() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxConnections() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxCursorNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxIndexLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxSchemaNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxProcedureNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxCatalogNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxRowSize() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxStatementLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxStatements() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxTableNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxTablesInSelect() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxUserNameLength() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getDefaultTransactionIsolation() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsTransactions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String types[]) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getSchemas() throws OlapException {
		LdOlap4jUtil._log.info("MetaData getSchemas()...");

		return getMetadataLd(LdOlap4jConnection.MetadataRequest.DBSCHEMA_SCHEMATA);
	}

	public ResultSet getCatalogs() throws OlapException {

		LdOlap4jUtil._log.info("MetaData getCatalogs()...");
		return getMetadataLd(LdOlap4jConnection.MetadataRequest.DBSCHEMA_CATALOGS);
	}

	public ResultSet getTableTypes() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getTypeInfo() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsResultSetType(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean othersDeletesAreVisible(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean updatesAreDetected(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean deletesAreDetected(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean insertsAreDetected(int type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsBatchUpdates() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public OlapConnection getConnection() {
		return olap4jConnection;
	}

	public boolean supportsSavepoints() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsNamedParameters() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsMultipleOpenResults() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsGetGeneratedKeys() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getResultSetHoldability() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getDatabaseMajorVersion() throws SQLException {
		throw Olap4jUtil.needToImplement(this);
	}

	public int getDatabaseMinorVersion() throws SQLException {
		throw Olap4jUtil.needToImplement(this);
	}

	public int getJDBCMajorVersion() throws SQLException {
		// this driver supports jdbc 3.0 and jdbc 4.0
		// FIXME: should return 3 if the current connection is jdbc 3.0
		return 4;
	}

	public int getJDBCMinorVersion() throws SQLException {
		// this driver supports jdbc 3.0 and jdbc 4.0
		return 0;
	}

	public int getSQLStateType() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean locatorsUpdateCopy() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean supportsStatementPooling() throws SQLException {
		throw new UnsupportedOperationException();
	}

	// implement java.sql.Wrapper

	// straightforward implementation of unwrap and isWrapperFor, since this
	// class already implements the interface they most likely require:
	// DatabaseMetaData and OlapDatabaseMetaData

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this)) {
			return iface.cast(this);
		}
		throw getHelper().createException("does not implement '" + iface + "'");
	}

	/**
	 * Returns the error-handler.
	 * 
	 * @return Error handler
	 */
	private LdHelper getHelper() {
		return olap4jConnection.helper;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	// implement OlapDatabaseMetaData
	/*
	 * Here, we always use the mondrian olap server. What we could do: - rather
	 * than return the result sets from the mondrian, create them myself -
	 * incrementally query the triple store and collect the information for
	 * creating the xml and fill the database - before a real question is
	 * issued: create the xml and fill the database
	 */

	public Set<CellSetListener.Granularity> getSupportedCellSetListenerGranularities()
			throws OlapException {

		// Set<CellSetListener.Granularity> betweenResult =
		// this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData()
		// .getSupportedCellSetListenerGranularities();
		// return betweenResult;

		return Collections.emptySet();
	}

	public ResultSet getActions(String catalog, String schemaPattern,
			String cubeNamePattern, String actionNamePattern)
			throws OlapException {
		LdOlap4jUtil._log.info("MetaData getActions()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear
		return olap4jConnection.factory.newEmptyResultSet(olap4jConnection);

		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getActions(catalog, schemaPattern, cubeNamePattern,
		// actionNamePattern);
		// return betweenResult;

		// return getMetadata(
		// XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_ACTIONS,
		// "CATALOG_NAME", catalog, "SCHEMA_NAME",
		// wildcard(schemaPattern), "CUBE_NAME",
		// wildcard(cubeNamePattern), "ACTION_NAME",
		// wildcard(actionNamePattern));
	}

	public ResultSet getDatabases() throws OlapException {
		// We can really query LD
		LdOlap4jUtil._log.info("MetaData getDatabases()...");
		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData().getDatabases();
		// return betweenResult;
		return getMetadataLd(LdOlap4jConnection.MetadataRequest.DISCOVER_DATASOURCES);
	}

	public ResultSet getLiterals() throws OlapException {
		LdOlap4jUtil._log.info("MetaData getLiterals()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear
		return olap4jConnection.factory.newEmptyResultSet(olap4jConnection);

		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData().getLiterals();
		// return betweenResult;

		// return getMetadata(
		// XmlaOlap4jConnection.MetadataRequest.DISCOVER_LITERALS);
	}

	public ResultSet getDatabaseProperties(String dataSourceName,
			String propertyNamePattern) throws OlapException {
		LdOlap4jUtil._log.info("MetaData getDatabaseProperties()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear: It returns the specific features of the
		// database.
		return olap4jConnection.factory.newEmptyResultSet(olap4jConnection);

		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData()
		// .getDatabaseProperties(dataSourceName, propertyNamePattern);
		// return betweenResult;

		// return
		// getMetadata(XmlaOlap4jConnection.MetadataRequest.DISCOVER_PROPERTIES);
	}

	public ResultSet getProperties(String catalog, String schemaPattern,
			String cubeNamePattern, String dimensionUniqueName,
			String hierarchyUniqueName, String levelUniqueName,
			String memberUniqueName, String propertyNamePattern)
			throws OlapException {
		LdOlap4jUtil._log.info("MetaData getProperties()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear
		return olap4jConnection.factory.newEmptyResultSet(olap4jConnection);

		//
		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getProperties(catalog, schemaPattern, cubeNamePattern,
		// dimensionUniqueName, hierarchyUniqueName,
		// levelUniqueName, memberUniqueName, propertyNamePattern);
		// return betweenResult;

		// return getMetadata(
		// XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_PROPERTIES,
		// "CATALOG_NAME", catalog, "SCHEMA_NAME",
		// wildcard(schemaPattern), "CUBE_NAME",
		// wildcard(cubeNamePattern), "DIMENSION_UNIQUE_NAME",
		// dimensionUniqueName, "HIERARCHY_UNIQUE_NAME",
		// hierarchyUniqueName, "LEVEL_UNIQUE_NAME", levelUniqueName,
		// "MEMBER_UNIQUE_NAME", memberUniqueName, "PROPERTY_NAME",
		// wildcard(propertyNamePattern));
	}

	public String getMdxKeywords() throws OlapException {
		LdOlap4jUtil._log.info("MetaData getMdxKeywords()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear
		return "";
		//
		// final XmlaOlap4jConnection.MetadataRequest metadataRequest =
		// XmlaOlap4jConnection.MetadataRequest.DISCOVER_KEYWORDS;
		// final XmlaOlap4jConnection.Context context = new
		// XmlaOlap4jConnection.Context(
		// olap4jConnection, null, null, null, null, null, null, null);
		// String betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData().getMdxKeywords();
		// return betweenResult;

		// String request = olap4jConnection.generateRequest(context,
		// metadataRequest, new Object[0]);
		// final Element root =
		// olap4jConnection.executeMetadataRequest(request);
		// StringBuilder buf = new StringBuilder();
		// for (Element row : XmlaOlap4jUtil.childElements(root)) {
		// if (buf.length() > 0) {
		// buf.append(',');
		// }
		// final String keyword = XmlaOlap4jUtil.stringElement(row, "Keyword");
		// buf.append(keyword);
		// }
		// return buf.toString();
	}

	public ResultSet getCubes(String catalog, String schemaPattern,
			String cubeNamePattern) throws OlapException {

		// Test what is returned by mondrian
		// e.g., sql enabled false
		// ResultSet cubes =
		// olap4jConnection.myOlapServer.getOlapConnection().getMetaData().getCubes(null,
		// schemaPattern, cubeNamePattern);

		// We do not need to query OLAP server
		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData()
		// .getCubes(catalog, schemaPattern, cubeNamePattern);
		// return betweenResult;
		// XMLA doesn't support drillthrough so override
		// whatever the server returns.
		LdOlap4jUtil._log.info("MetaData getCubes()...");

		final Map<LdOlap4jConnection.MetadataColumn, String> overrides = new HashMap<LdOlap4jConnection.MetadataColumn, String>();

		overrides.put(LdOlap4jConnection.MetadataRequest.MDSCHEMA_CUBES
				.getColumn("IS_DRILLTHROUGH_ENABLED"), "false");

		return getMetadataLd(LdOlap4jConnection.MetadataRequest.MDSCHEMA_CUBES,
				overrides, "CATALOG_NAME", catalog, "SCHEMA_NAME",
				wildcard(schemaPattern), "CUBE_NAME", wildcard(cubeNamePattern));
	}

	public ResultSet getDimensions(String catalog, String schemaPattern,
			String cubeNamePattern, String dimensionNamePattern)
			throws OlapException {

		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getDimensions(catalog, schemaPattern, cubeNamePattern,
		// dimensionNamePattern);
		// return betweenResult;
		LdOlap4jUtil._log.info("MetaData getDimensions()...");

		return getMetadataLd(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_DIMENSIONS,
				"CATALOG_NAME", catalog, "SCHEMA_NAME",
				wildcard(schemaPattern), "CUBE_NAME",
				wildcard(cubeNamePattern), "DIMENSION_NAME",
				wildcard(dimensionNamePattern));
	}

	public ResultSet getOlapFunctions(String functionNamePattern)
			throws OlapException {
		LdOlap4jUtil._log.info("MetaData getOLapFunctions()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear
		return olap4jConnection.factory.newEmptyResultSet(olap4jConnection);

		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection().getMetaData()
		// .getOlapFunctions(functionNamePattern);
		// return betweenResult;

		// return getMetadata(
		// XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_FUNCTIONS,
		// "FUNCTION_NAME", wildcard(functionNamePattern));
	}

	public ResultSet getHierarchies(String catalog, String schemaPattern,
			String cubeNamePattern, String dimensionUniqueName,
			String hierarchyNamePattern) throws OlapException {
		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getHierarchies(catalog, schemaPattern, cubeNamePattern,
		// dimensionUniqueName, hierarchyNamePattern);
		// return betweenResult;
		LdOlap4jUtil._log.info("MetaData getHierarchies()...");

		return getMetadataLd(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_HIERARCHIES,
				"CATALOG_NAME", catalog, "SCHEMA_NAME",
				wildcard(schemaPattern), "CUBE_NAME",
				wildcard(cubeNamePattern), "DIMENSION_UNIQUE_NAME",
				dimensionUniqueName, "HIERARCHY_NAME",
				wildcard(hierarchyNamePattern));
	}

	public ResultSet getMeasures(String catalog, String schemaPattern,
			String cubeNamePattern, String measureNamePattern,
			String measureUniqueName) throws OlapException {
		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getMeasures(catalog, schemaPattern, cubeNamePattern,
		// measureNamePattern, measureUniqueName);
		// return betweenResult;
		LdOlap4jUtil._log.info("MetaData getMeasures()...");

		return getMetadataLd(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEASURES,
				"CATALOG_NAME", catalog, "SCHEMA_NAME",
				wildcard(schemaPattern), "CUBE_NAME",
				wildcard(cubeNamePattern), "MEASURE_NAME",
				wildcard(measureNamePattern), "MEASURE_UNIQUE_NAME",
				measureUniqueName);
	}

	public ResultSet getMembers(String catalog, String schemaPattern,
			String cubeNamePattern, String dimensionUniqueName,
			String hierarchyUniqueName, String levelUniqueName,
			String memberUniqueName, Set<Member.TreeOp> treeOps)
			throws OlapException {

		String treeOpRestriction;
		// What exactly is queried?
		if (treeOps == null) {
			treeOpRestriction = "";
		} else {
			treeOpRestriction = "";
			for (TreeOp treeOp : treeOps) {
				treeOpRestriction += treeOp.getDescription();
			}
		}
		LdOlap4jUtil._log.info("MetaData getMembers(" + catalog + ","
				+ schemaPattern + "," + cubeNamePattern + ","
				+ dimensionUniqueName + "," + hierarchyUniqueName + ","
				+ levelUniqueName + "," + memberUniqueName + ","
				+ treeOpRestriction + ")...");

		String treeOpString;
		if (treeOps != null) {
			int op = 0;
			for (Member.TreeOp treeOp : treeOps) {
				op |= treeOp.xmlaOrdinal();
			}
			treeOpString = String.valueOf(op);
		} else {
			treeOpString = null;
		}
		return getMetadataLd(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEMBERS,
				"CATALOG_NAME", catalog, "SCHEMA_NAME",
				wildcard(schemaPattern), "CUBE_NAME",
				wildcard(cubeNamePattern), "DIMENSION_UNIQUE_NAME",
				dimensionUniqueName, "HIERARCHY_UNIQUE_NAME",
				hierarchyUniqueName, "LEVEL_UNIQUE_NAME", levelUniqueName,
				"MEMBER_UNIQUE_NAME", memberUniqueName, "TREE_OP", treeOpString);
	}

	public ResultSet getLevels(String catalog, String schemaPattern,
			String cubeNamePattern, String dimensionUniqueName,
			String hierarchyUniqueName, String levelNamePattern)
			throws OlapException {
		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getLevels(catalog, schemaPattern, cubeNamePattern,
		// dimensionUniqueName, hierarchyUniqueName,
		// levelNamePattern);
		// return betweenResult;
		LdOlap4jUtil._log.info("MetaData getLevels()...");

		return getMetadataLd(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_LEVELS,
				"CATALOG_NAME", catalog, "SCHEMA_NAME",
				wildcard(schemaPattern), "CUBE_NAME",
				wildcard(cubeNamePattern), "DIMENSION_UNIQUE_NAME",
				dimensionUniqueName, "HIERARCHY_UNIQUE_NAME",
				hierarchyUniqueName, "LEVEL_NAME", wildcard(levelNamePattern));
	}

	public ResultSet getSets(String catalog, String schemaPattern,
			String cubeNamePattern, String setNamePattern) throws OlapException {
		LdOlap4jUtil._log.info("MetaData getSets()...");
		// We simply return empty result set, since the relevance of those
		// methods is not clear
		return olap4jConnection.factory.newEmptyResultSet(olap4jConnection);

		// ResultSet betweenResult = this.olap4jConnection.myOlapServer
		// .getOlapConnection()
		// .getMetaData()
		// .getSets(catalog, schemaPattern, cubeNamePattern,
		// setNamePattern);
		// return betweenResult;
		// return

		// getMetadata(XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_SETS,
		// "CATALOG_NAME", catalog, "SCHEMA_NAME",
		// wildcard(schemaPattern), "CUBE_NAME",
		// wildcard(cubeNamePattern), "SET_NAME", wildcard(setNamePattern));
	}

	/**
	 * Wrapper which indicates that a restriction is to be treated as a
	 * SQL-style wildcard match.
	 */
	static class Wildcard {
		final String pattern;

		/**
		 * Creates a Wildcard.
		 * 
		 * @param pattern
		 *            Pattern
		 */
		Wildcard(String pattern) {
			assert pattern != null;
			this.pattern = pattern;
		}
	}
}

// End XmlaOlap4jDatabaseMetaData.java

