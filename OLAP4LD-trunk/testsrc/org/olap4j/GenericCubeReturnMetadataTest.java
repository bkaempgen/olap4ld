/*
//$Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
//
//Licensed to Julian Hyde under one or more contributor license
//agreements. See the NOTICE file distributed with this work for
//additional information regarding copyright ownership.
//
//Julian Hyde licenses this file to you under the Apache License,
//Version 2.0 (the "License"); you may not use this file except in
//compliance with the License. You may obtain a copy of the License at:
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
 */
package org.olap4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;

import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.metadata.Member;
import org.olap4j.test.TestContext;

/**
 * Unit test for olap4j metadata methods.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class GenericCubeReturnMetadataTest extends TestCase {
	private static final String NL = System.getProperty("line.separator");

	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private String catalogName;
	private OlapConnection olapConnection;
	private OlapDatabaseMetaData olapDatabaseMetaData;
	private final String propertyNamePattern = null;
	private final String dataSourceName = "xx";

	private ResultsDecoratorHTML resultDecorator;

	private String cubeNamePattern;

	private static final List<String> CUBE_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME", "CUBE_TYPE",
			"CUBE_GUID", "CREATED_ON", "LAST_SCHEMA_UPDATE",
			"SCHEMA_UPDATED_BY", "LAST_DATA_UPDATE", "DATA_UPDATED_BY",
			"IS_DRILLTHROUGH_ENABLED", "IS_WRITE_ENABLED", "IS_LINKABLE",
			"IS_SQL_ENABLED", "DESCRIPTION");
	private static final List<String> LITERALS_COLUMN_NAMES = Arrays.asList(
			"LITERAL_NAME", "LITERAL_VALUE", "LITERAL_INVALID_CHARS",
			"LITERAL_INVALID_STARTING_CHARS", "LITERAL_MAX_LENGTH");
	private static final List<String> SETS_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME", "SET_NAME", "SCOPE");
	private static final List<String> PROPERTIES_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
			"DIMENSION_UNIQUE_NAME", "HIERARCHY_UNIQUE_NAME",
			"LEVEL_UNIQUE_NAME", "MEMBER_UNIQUE_NAME", "PROPERTY_NAME",
			"PROPERTY_CAPTION", "PROPERTY_TYPE", "DATA_TYPE",
			"PROPERTY_CONTENT_TYPE", "DESCRIPTION");
	private static final List<String> MEMBERS_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
			"DIMENSION_UNIQUE_NAME", "HIERARCHY_UNIQUE_NAME",
			"LEVEL_UNIQUE_NAME", "LEVEL_NUMBER", "MEMBER_ORDINAL",
			"MEMBER_NAME", "MEMBER_UNIQUE_NAME", "MEMBER_TYPE", "MEMBER_GUID",
			"MEMBER_CAPTION", "CHILDREN_CARDINALITY", "PARENT_LEVEL",
			"PARENT_UNIQUE_NAME", "PARENT_COUNT", "TREE_OP", "DEPTH");
	private static final List<String> MEASURES_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME", "MEASURE_NAME",
			"MEASURE_UNIQUE_NAME", "MEASURE_CAPTION", "MEASURE_GUID",
			"MEASURE_AGGREGATOR", "DATA_TYPE", "MEASURE_IS_VISIBLE",
			"LEVELS_LIST", "DESCRIPTION");
	private static final List<String> LEVELS_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
			"DIMENSION_UNIQUE_NAME", "HIERARCHY_UNIQUE_NAME", "LEVEL_NAME",
			"LEVEL_UNIQUE_NAME", "LEVEL_GUID", "LEVEL_CAPTION", "LEVEL_NUMBER",
			"LEVEL_CARDINALITY", "LEVEL_TYPE", "CUSTOM_ROLLUP_SETTINGS",
			"LEVEL_UNIQUE_SETTINGS", "LEVEL_IS_VISIBLE", "DESCRIPTION");
	private static final List<String> HIERARCHIES_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
			"DIMENSION_UNIQUE_NAME", "HIERARCHY_NAME", "HIERARCHY_UNIQUE_NAME",
			"HIERARCHY_GUID", "HIERARCHY_CAPTION", "DIMENSION_TYPE",
			"HIERARCHY_CARDINALITY", "DEFAULT_MEMBER", "ALL_MEMBER",
			"DESCRIPTION", "STRUCTURE", "IS_VIRTUAL", "IS_READWRITE",
			"DIMENSION_UNIQUE_SETTINGS", "DIMENSION_IS_VISIBLE",
			"HIERARCHY_IS_VISIBLE", "HIERARCHY_ORDINAL", "DIMENSION_IS_SHARED",
			"PARENT_CHILD");
	private static final List<String> FUNCTIONS_COLUMN_NAMES = Arrays.asList(
			"FUNCTION_NAME", "DESCRIPTION", "PARAMETER_LIST", "RETURN_TYPE",
			"ORIGIN", "INTERFACE_NAME", "LIBRARY_NAME", "CAPTION");
	private static final List<String> DIMENSIONS_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME", "DIMENSION_NAME",
			"DIMENSION_UNIQUE_NAME", "DIMENSION_GUID", "DIMENSION_CAPTION",
			"DIMENSION_ORDINAL", "DIMENSION_TYPE", "DIMENSION_CARDINALITY",
			"DEFAULT_HIERARCHY", "DESCRIPTION", "IS_VIRTUAL", "IS_READWRITE",
			"DIMENSION_UNIQUE_SETTINGS", "DIMENSION_MASTER_UNIQUE_NAME",
			"DIMENSION_IS_VISIBLE");
	private static final List<String> DATABASE_PROPERTIES_COLUMN_NAMES = Arrays
			.asList("PROPERTY_NAME", "PROPERTY_DESCRIPTION", "PROPERTY_TYPE",
					"PROPERTY_ACCESS_TYPE", "IS_REQUIRED", "PROPERTY_VALUE");
	private static final List<String> DATASOURCES_COLUMN_NAMES = Arrays.asList(
			"DATA_SOURCE_NAME", "DATA_SOURCE_DESCRIPTION", "URL",
			"DATA_SOURCE_INFO", "PROVIDER_NAME", "PROVIDER_TYPE",
			"AUTHENTICATION_MODE");
	private static final List<String> CATALOGS_COLUMN_NAMES = Arrays
			.asList("TABLE_CAT");
	private static final List<String> SCHEMAS_COLUMN_NAMES = Arrays.asList(
			"TABLE_SCHEM", "TABLE_CAT");
	private static final List<String> ACTIONS_COLUMN_NAMES = Arrays.asList(
			"CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME", "ACTION_NAME",
			"COORDINATE", "COORDINATE_TYPE");

	/**
	 * Most important part is the input of a cube name.
	 * 
	 * @throws SQLException
	 */
	public GenericCubeReturnMetadataTest() throws SQLException {

		//SEC Example
		this.cubeNamePattern = "httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FarchiveXXX2F1141391XXX2F0001193125-09-222058XXX23:dsd";
		
		//Yahoo Finance Example
		//this.cubeNamePattern = "httpXXX3AXXX2FXXX2Fyahoofinancewrap.appspot.comXXX2FarchiveXXX2FMAXXX2F2006-06-01XXX23:dsd";
	}

	protected void setUp() throws SQLException {
		connection = tester.createConnection();
		catalogName = connection.getCatalog();
		olapConnection = tester.getWrapper().unwrap(connection,
				OlapConnection.class);
		olapDatabaseMetaData = olapConnection.getMetaData();

		this.resultDecorator = new ResultsDecoratorHTML(null);
	}

	protected void tearDown() throws Exception {
		if (connection != null && !connection.isClosed()) {
			connection.close();
			connection = null;
		}
	}

	// ~ Helper methods ----------

	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
	}

	private void assertContainsLine(String partial, String seek, String s) {
		if (partial == null) {
			partial = seek;
		}
		int i = s.indexOf(partial);
		if (i < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
		int start = i;
		while (start > 0 && s.charAt(start - 1) != 0x0D
				&& s.charAt(start - 1) != 0x0A) {
			--start;
		}
		int end = i;
		while (end < s.length() && s.charAt(end) != 0x0D
				&& s.charAt(end) != 0x0A) {
			++end;
		}
		String line = s.substring(start, end);
		assertEquals(seek, line);
	}

	private void assertNotContains(String seek, String s) {
		if (s.indexOf(seek) >= 0) {
			fail("expected not to find '" + seek + "' in '" + s + "'");
		}
	}

	private int linecount(String s) {
		int i = 0;
		int count = 0;
		while (i < s.length()) {
			int nl = s.indexOf('\n', i);
			if (nl < 0) {
				break;
			}
			i = nl + 1;
			++count;
		}
		return count;
	}

	// ~ Tests follow -------------

	/*
	 * No tests for
	 * 
	 * databases catalogs schemas actions
	 */

	public void testDatabaseMetaDataGetDatasources() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getDatabases(),
				DATASOURCES_COLUMN_NAMES);

		System.out.println("getDatabases(): " + s);
	}

	public void testDatabaseMetaDataGetCatalogs() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getCatalogs(),
				CATALOGS_COLUMN_NAMES);
		System.out.println("getCatalogs(): " + s);
	}

	public void testDatabaseMetaDataGetSchemas() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getSchemas(),
				SCHEMAS_COLUMN_NAMES);

		System.out.println("getSchemas(): " + s);
	}

	public void testDatabaseMetaDataGetLiterals() throws SQLException {
		// String s = checkResultSet(olapDatabaseMetaData.getLiterals(),
		// LITERALS_COLUMN_NAMES);
		//
		// System.out.println("getLiterals(): " + s);
	}

	public void testDatabaseMetaDataGetDatabaseProperties() throws SQLException {
//		String s = checkResultSet(olapDatabaseMetaData.getDatabaseProperties(
//				dataSourceName, propertyNamePattern),
//				DATABASE_PROPERTIES_COLUMN_NAMES);
//		System.out.println("getDatabaseProperties(): " + s);
	}

	public void testDatabaseMetaDataGetProperties() throws SQLException {
//		String s = checkResultSet(olapDatabaseMetaData.getProperties(
//				catalogName, null, null, null, null, null, null, null),
//				PROPERTIES_COLUMN_NAMES);
//		System.out.println("getProperties(): " + s);
	}

	public void testDatabaseMetaDataGetMdxKeywords() throws SQLException {
		String keywords = olapDatabaseMetaData.getMdxKeywords();

		System.out.println("getMdxKeywords(): " + keywords);
	}

	/**
	 * We ask for a specific cube and return its properties.
	 * @throws SQLException
	 */
	public void testDatabaseMetaDataGetCubes() throws SQLException {
		String s = checkResultSet(
				olapDatabaseMetaData.getCubes(catalogName, null, cubeNamePattern),
				CUBE_COLUMN_NAMES);

		System.out.println("getCubes(): " + s);
	}

	/**
	 * We ask for all dimensions of the cube.
	 * @throws SQLException
	 */
	public void testDatabaseMetaDataGetDimensions() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getDimensions(
				catalogName, null, cubeNamePattern, null), DIMENSIONS_COLUMN_NAMES);
		
		System.out.println("getDimensions(): " + s);
	}

	public void testDatabaseMetaDataGetFunctions() throws SQLException {
//		String s = checkResultSet(olapDatabaseMetaData.getOlapFunctions(null),
//				FUNCTIONS_COLUMN_NAMES);
//		System.out.println("getFunctions(): " + s);
	}

	/** 
	 * For each of the dimensions, we return the hierarchy.
	 * @throws SQLException
	 */
	public void testDatabaseMetaDataGetHierarchies() throws SQLException {
		
		String s = checkResultSet(olapDatabaseMetaData.getHierarchies(
				catalogName, null, cubeNamePattern, null, null), HIERARCHIES_COLUMN_NAMES);
		System.out.println("getHierarchies(): " + s);
	}

	public void testDatabaseMetaDataGetLevels() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getLevels(catalogName,
				null, cubeNamePattern, null, null, null), LEVELS_COLUMN_NAMES);
		
		System.out.println("getLevels(): " + s);
	}

	public void testDatabaseMetaDataGetLiterals2() throws SQLException {
//		String s = checkResultSet(olapDatabaseMetaData.getLiterals(),
//				LITERALS_COLUMN_NAMES);
//		System.out.println("getLiterals2(): " + s);
	}

	public void testDatabaseMetaDataGetMeasures() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getMeasures(catalogName,
				null, cubeNamePattern, null, null), MEASURES_COLUMN_NAMES);
		
		System.out.println("getMeasures(): " + s);
	}
	
	/**
	 * For each measure, find the measure member.
	 * @throws SQLException
	 */
	public void testDatabaseMetaDataGetMeasureMembers() throws SQLException {
		
		ResultSet measures = olapDatabaseMetaData.getMeasures(catalogName,null, cubeNamePattern, null, null);
		
		while (measures.next()) {
			String measure = measures.getString(4);
		
			String s = checkResultSet(olapDatabaseMetaData.getMembers(catalogName,
					null, cubeNamePattern, null, null, null, measure, Olap4jUtil.enumSetOf(Member.TreeOp.SELF)), MEMBERS_COLUMN_NAMES);
			
			System.out.println("getMembers("+measure+"): " + s);
			
		}
	}

	/**
	 * For each level, find the members
	 * @throws SQLException
	 */
	public void testDatabaseMetaDataGetMembers() throws SQLException {
		ResultSet levels = olapDatabaseMetaData.getLevels(catalogName,null, cubeNamePattern, null, null, null);
		
		while (levels.next()) {
			String level = levels.getString(6);
			
			String s = checkResultSet(olapDatabaseMetaData.getMembers(catalogName,
					null, cubeNamePattern, null, null, level, null, null), MEMBERS_COLUMN_NAMES);

			System.out.println("getMembers(Level:"+level+"): " + s);
		}
		
		// Get single member
		String s = checkResultSet(olapDatabaseMetaData.getMembers(catalogName,
				null, cubeNamePattern, null, null, null, "httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FcikXXX2F1141391XXX23:id", Olap4jUtil.enumSetOf(Member.TreeOp.SELF)), MEMBERS_COLUMN_NAMES);
		assertContains("httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FcikXXX2F1141391XXX23:id",s);
		System.out.println("getMembers(Level:): " + s);
	}
	
	

	public void testDatabaseMetaDataGetSets() throws SQLException {
		// String s = checkResultSet(
		// olapDatabaseMetaData.getSets(catalogName, null, null, null),
		// SETS_COLUMN_NAMES);
		// System.out.println("getSets(): " + s);
	}

	// todo: More tests required for other methods on DatabaseMetaData

	private String checkResultSet(ResultSet resultSet, List<String> columnNames)
			throws SQLException {
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		final int columnCount = resultSetMetaData.getColumnCount();
		List<String> rsColumnNames = new ArrayList<String>();
		if (columnNames != null) {
			for (int k = 0; k < columnCount; k++) {
				rsColumnNames.add(resultSetMetaData.getColumnName(k + 1));
			}

			final HashSet<String> set = new HashSet<String>(columnNames);
			set.removeAll(rsColumnNames);
			assertTrue("Expected columns not found: " + set, set.isEmpty());
		}
		assertNotNull(resultSet);
		int k = 0;
		StringBuilder buf = new StringBuilder();
		while (resultSet.next()) {
			++k;
			for (int i = 0; i < columnCount; i++) {
				if (i > 0) {
					buf.append(", ");
				}
				String s = resultSet.getString(i + 1);
				buf.append(resultSetMetaData.getColumnName(i + 1)).append('=')
						.append(s);
			}
			buf.append(NL);
		}
		assertTrue(k >= 0);
		assertTrue(resultSet.isAfterLast());

		return buf.toString();
	}

}

// End MetadataTest.java
