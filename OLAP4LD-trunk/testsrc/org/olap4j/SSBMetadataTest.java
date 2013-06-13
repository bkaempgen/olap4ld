/*
// $Id: MetadataTest.java 544 2012-08-02 17:39:40Z lucboudreau $
//
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
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
package org.olap4j;

import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;
import org.olap4j.test.TestContext;

import junit.framework.TestCase;

import java.sql.*;
import java.util.*;

/**
 * Unit test for olap4j metadata methods on Star Schema.
 * 
 * @version v1
 */
public class SSBMetadataTest extends TestCase {
	private TestContext testContext = TestContext.instance();
	private TestContext.Tester tester = testContext.getTester();
	private static final String NL = System.getProperty("line.separator");

	private Connection connection;
	private String catalogName;
	private OlapConnection olapConnection;
	private OlapDatabaseMetaData olapDatabaseMetaData;
	private final String propertyNamePattern = null;
	private final String dataSourceName = "xx";

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

	public SSBMetadataTest() throws SQLException {
	}

	protected void setUp() throws SQLException {
		connection = tester.createConnection();
		catalogName = connection.getCatalog();
		olapConnection = tester.getWrapper().unwrap(connection,
				OlapConnection.class);
		olapDatabaseMetaData = olapConnection.getMetaData();
	}

	protected void tearDown() throws Exception {
		if (connection != null && !connection.isClosed()) {
			connection.close();
			connection = null;
		}
		connection = null;
		olapConnection = null;
		olapDatabaseMetaData = null;
		testContext = null;
		tester = null;
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

	public void testDatabaseMetaData() throws SQLException {
		assertEquals("" + catalogName + "", catalogName);

		DatabaseMetaData databaseMetaData = connection.getMetaData();
		switch (tester.getFlavor()) {
		case XMLA:
		case REMOTE_XMLA:
			// FIXME: implement getDatabaseXxxVersion in XMLA driver
			break;
		default:
			assertTrue(databaseMetaData.getDatabaseMajorVersion() > 0);
			assertTrue(databaseMetaData.getDatabaseMinorVersion() >= 0);
			// assertTrue(databaseMetaData.getDatabaseProductName() != null);
			assertTrue(databaseMetaData.getDatabaseProductVersion() != null);
			break;
		}
		assertTrue(databaseMetaData.getDriverName() != null);
		assertTrue(databaseMetaData.getDriverVersion() != null);

		// mondrian-specific
		switch (tester.getFlavor()) {
		case MONDRIAN:
			assertTrue(databaseMetaData.isReadOnly());
			assertNull(databaseMetaData.getUserName());
			assertNotNull(databaseMetaData.getURL());
			break;
		default:
			break;
		}

		// unwrap connection; may or may not be the same object as connection;
		// check extended methods

		// also unwrap metadata from regular connection
		assertTrue(((OlapWrapper) databaseMetaData)
				.isWrapperFor(OlapDatabaseMetaData.class));
		assertFalse(((OlapWrapper) databaseMetaData)
				.isWrapperFor(OlapStatement.class));
		OlapDatabaseMetaData olapDatabaseMetaData1 = ((OlapWrapper) databaseMetaData)
				.unwrap(OlapDatabaseMetaData.class);
		assertTrue(olapDatabaseMetaData1.getDriverName().equals(
				olapDatabaseMetaData.getDriverName()));
		switch (tester.getFlavor()) {
		case XMLA:
		case REMOTE_XMLA:
			// FIXME: implement getDatabaseXxxVersion in XMLA driver
			break;
		default:
			assertTrue(olapDatabaseMetaData1.getDatabaseProductVersion()
					.equals(olapDatabaseMetaData.getDatabaseProductVersion()));
		}
	}

	public void testDatabases() throws SQLException {
		final Database database = olapConnection.getOlapDatabase();
		assertEquals("Failed to auto detect the catalog.", database.getName(),
				"OPENVIRTUOSO");

		// Setting a correct database should work.
		olapConnection.setDatabase(database.getName());
		// setting a null database should fail
		try {
			olapConnection.setDatabase(null);
			fail();
		} catch (OlapException e) {
			// no op.
		}
		// setting a non existent database should fail
		try {
			olapConnection.setDatabase("Chunky Bacon");
			fail();
		} catch (OlapException e) {
			// no op.
		}
	}

	public void testSchemas() throws OlapException {
		// check schema auto detection
		final Schema schema1 = olapConnection.getOlapSchema();
		assertEquals("Failed to auto detect the schema.", "[LdSchema]",
				schema1.getName());
		// Setting a correct schema should work.
		olapConnection.setSchema(schema1.getName());
		// setting a null schema should fail
		try {
			olapConnection.setSchema(null);
			fail();
		} catch (OlapException e) {
			// no op.
		}
		// setting a non existent schema should fail
		try {
			olapConnection.setSchema("Chunky Bacon");
			fail();
		} catch (OlapException e) {
			// no op.
		}
	}

	public void testCatalogs() throws SQLException {
		final Catalog catalog = olapConnection.getOlapCatalog();
		assertEquals("Failed to auto detect the catalog.", catalog.getName(),
				"[LdCatalog]");

		// Setting a correct catalog should work.
		olapConnection.setCatalog(catalogName);
		// setting a null catalog should fail
		try {
			olapConnection.setCatalog(null);
			fail();
		} catch (OlapException e) {
			// no op.
		}
		// setting a non existent catalog should fail
		try {
			olapConnection.setCatalog("Chunky Bacon");
			fail();
		} catch (OlapException e) {
			// no op.
		}
	}

	public void testDatabaseMetaDataGetActions() throws SQLException {
		// olap4ld currently does not support actions
		// String s = checkResultSet(
		// olapDatabaseMetaData.getActions(catalogName, null, null, null),
		// ACTIONS_COLUMN_NAMES);
		// assertEquals("", s); // mondrian has no actions
	}

	public void testDatabaseMetaDataGetDatasources() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getDatabases(),
				DATASOURCES_COLUMN_NAMES);

		TestContext
				.assertEqualsVerbose(
						"DATA_SOURCE_NAME=[OPENVIRTUOSO], DATA_SOURCE_DESCRIPTION=[OLAP+data+from+the+statistical+Linked+Data+cloudYYY], URL=[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8890XXX2Fsparql], DATA_SOURCE_INFO=[Data+following+the+Linked+Data+principlesYYY], PROVIDER_NAME=[The+communityYYY], PROVIDER_TYPE=, AUTHENTICATION_MODE=\n",
						s);

	}

	public void testDatabaseMetaDataGetCatalogs() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getCatalogs(),
				CATALOGS_COLUMN_NAMES);
		final String expected;
		expected = "TABLE_CAT=[LdCatalog]\n";

		TestContext.assertEqualsVerbose(expected, s);
	}

	public void testDatabaseMetaDataGetSchemas() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getSchemas(),
				SCHEMAS_COLUMN_NAMES);

		final String expected;

		expected = "TABLE_SCHEM=[LdSchema], TABLE_CAT=[LdCatalog]\n";
		TestContext.assertEqualsVerbose(expected, s);
	}

	public void testDatabaseMetaDataGetLiterals() throws SQLException {
		// not supported
		// String s = checkResultSet(olapDatabaseMetaData.getLiterals(),
		// LITERALS_COLUMN_NAMES);
		// assertContains("LITERAL_NAME=DBLITERAL_QUOTE, LITERAL_VALUE=[, ", s);
	}

	public void testDatabaseMetaDataGetDatabaseProperties() throws SQLException {
		// not supported
		// String s = checkResultSet(olapDatabaseMetaData.getDatabaseProperties(
		// dataSourceName, propertyNamePattern),
		// DATABASE_PROPERTIES_COLUMN_NAMES);
		// assertContains("PROPERTY_NAME=ProviderName, ", s);
	}

	public void testDatabaseMetaDataGetProperties() throws SQLException {
		// not supported
		// String s = checkResultSet(olapDatabaseMetaData.getProperties(
		// catalogName, null, null, null, null, null, null, null),
		// PROPERTIES_COLUMN_NAMES);
		// assertContains(
		// "CATALOG_NAME="
		// + catalogName
		// +
		// ", SCHEMA_NAME=FoodMart, CUBE_NAME=Warehouse and Sales, DIMENSION_UNIQUE_NAME=[Store], HIERARCHY_UNIQUE_NAME=[Store].[Store], LEVEL_UNIQUE_NAME=[Store].[Store].[Store Name], MEMBER_UNIQUE_NAME=null, PROPERTY_NAME=Frozen Sqft, PROPERTY_CAPTION=Frozen Sqft, PROPERTY_TYPE=1, DATA_TYPE=5, PROPERTY_CONTENT_TYPE=0, DESCRIPTION=Warehouse and Sales Cube - Store Hierarchy - Store Name Level - Frozen Sqft Property",
		// s);
		// assertEquals(s, 66, linecount(s));
		//
		// s = checkResultSet(olapDatabaseMetaData.getProperties(catalogName,
		// "FoodMart", "Sales", null, null,
		// "[Store].[Store].[Store Name]", null, null),
		// PROPERTIES_COLUMN_NAMES);
		// assertContains(
		// "CATALOG_NAME="
		// + catalogName
		// +
		// ", SCHEMA_NAME=FoodMart, CUBE_NAME=Sales, DIMENSION_UNIQUE_NAME=[Store], HIERARCHY_UNIQUE_NAME=[Store].[Store], LEVEL_UNIQUE_NAME=[Store].[Store].[Store Name], MEMBER_UNIQUE_NAME=null, PROPERTY_NAME=Has coffee bar, PROPERTY_CAPTION=Has coffee bar, PROPERTY_TYPE=1, DATA_TYPE=130, PROPERTY_CONTENT_TYPE=0, DESCRIPTION=Sales Cube - Store Hierarchy - Store Name Level - Has coffee bar Property",
		// s);
		// assertNotContains("CATALOG_NAME=" + catalogName
		// + ", SCHEMA_NAME=FoodMart, CUBE_NAME=Warehouse and Sales, ", s);
		// assertEquals(8, linecount(s));
	}

	public void testDatabaseMetaDataGetMdxKeywords() throws SQLException {
		// not supported
		// String keywords = olapDatabaseMetaData.getMdxKeywords();
		// assertNotNull(keywords);
		// assertContains(",From,", keywords);
	}

	public void testDatabaseMetaDataGetCubes() throws SQLException {
		String s = checkResultSet(
				olapDatabaseMetaData.getCubes(catalogName, null, null),
				CUBE_COLUMN_NAMES);
		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], CUBE_TYPE=[CUBE], CUBE_GUID=, CREATED_ON=, LAST_SCHEMA_UPDATE=, SCHEMA_UPDATED_BY=, LAST_DATA_UPDATE=, DATA_UPDATED_BY=, IS_DRILLTHROUGH_ENABLED=false, IS_WRITE_ENABLED=, IS_LINKABLE=, IS_SQL_ENABLED=, DESCRIPTION=null, CUBE_CAPTION=null, BASE_CUBE_NAME=\n",
				s);
		final int lineCount = linecount(s);

		// Null catalog specified in metadata request, but connection has a
		// catalog. Should return all cubes, ignoring the connection's catalog.
		olapDatabaseMetaData.getConnection().setCatalog(catalogName);
		s = checkResultSet(olapDatabaseMetaData.getCubes(null, null, null),
				CUBE_COLUMN_NAMES);
		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], CUBE_TYPE=[CUBE], CUBE_GUID=, CREATED_ON=, LAST_SCHEMA_UPDATE=, SCHEMA_UPDATED_BY=, LAST_DATA_UPDATE=, DATA_UPDATED_BY=, IS_DRILLTHROUGH_ENABLED=false, IS_WRITE_ENABLED=, IS_LINKABLE=, IS_SQL_ENABLED=, DESCRIPTION=null, CUBE_CAPTION=null, BASE_CUBE_NAME=\n",
				s);
		final int lineCount3 = linecount(s);
		if (tester.getFlavor() == TestContext.Tester.Flavor.XMLA) {
			assertEquals(lineCount * 2, lineCount3);
		}
		s = checkResultSet(olapDatabaseMetaData.getCubes("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]"), CUBE_COLUMN_NAMES);

		assertTrue(s.contains(", IS_DRILLTHROUGH_ENABLED=false"));
		assertFalse(s.contains(", IS_DRILLTHROUGH_ENABLED=true"));

		// If we ask for 'rdfhZZZinstXXX3Adsd' cube we should get it, but
		// nothing else.
		s = checkResultSet(olapDatabaseMetaData.getCubes(catalogName, null,
				"[rdfhZZZinstXXX3Adsd]"), CUBE_COLUMN_NAMES);
		assertContains(", CUBE_NAME=[rdfhZZZinstXXX3Adsd],", s);
		assertNotContains(", CUBE_NAME=[rdfhZZZinst,", s);

		// Patterns currently not supported.
		// If we ask for a pattern, should get multiple hits.
		// s = checkResultSet(
		// olapDatabaseMetaData.getCubes(catalogName, null, "rdfh-inst:%"),
		// CUBE_COLUMN_NAMES);
		// assertTrue(s.contains(", CUBE_NAME=[rdfhZZZinstXXX3Adsd]"));
		// assertTrue(s.contains(", CUBE_NAME=Warehouse"));
		// assertFalse(s.contains(", CUBE_NAME=Sales"));
	}

	public void testDatabaseMetaDataGetDimensions() throws SQLException {

		// All possible dimensions
		String s = checkResultSet(olapDatabaseMetaData.getDimensions(
				catalogName, null, null, null), DIMENSIONS_COLUMN_NAMES);

		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_commitdate], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_commitdate], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_custkey], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_discount], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_discount], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_linenumber], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_linenumber], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_orderdate], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_orderkey], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderkey], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_orderpriority], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderpriority], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_ordtotalprice], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_ordtotalprice], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_partkey], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_partkey], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_quantity], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_quantity], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_shipmode], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_shipmode], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_shippriority], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_shippriority], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_suppkey], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_suppkey], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_supplycost], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_supplycost], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=[rdfhXXX3Alo_tax], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_tax], DIMENSION_GUID=, DIMENSION_CAPTION=null, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[0], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=null, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_NAME=Measures, DIMENSION_UNIQUE_NAME=Measures, DIMENSION_GUID=, DIMENSION_CAPTION=Measures, DIMENSION_ORDINAL=[0], DIMENSION_TYPE=[2], DIMENSION_CARDINALITY=, DEFAULT_HIERARCHY=, DESCRIPTION=Measures, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_MASTER_UNIQUE_NAME=, DIMENSION_IS_VISIBLE=\n",
				s);

		// There should be 16 dimensions (including Measures)
		assertEquals(16, linecount(s));
	}

	public void testDatabaseMetaDataGetFunctions() throws SQLException {
		// not supported
		// String s =
		// checkResultSet(olapDatabaseMetaData.getOlapFunctions(null),
		// FUNCTIONS_COLUMN_NAMES);
		// assertContains(
		// "FUNCTION_NAME=Name, DESCRIPTION=Returns the name of a member., PARAMETER_LIST=Member, RETURN_TYPE=8, ORIGIN=1, INTERFACE_NAME=, LIBRARY_NAME=null, CAPTION=Name",
		// s);
		// // Mondrian has 361 functions (as of 2008/1/23)
		// final int functionCount = linecount(s);
		// assertTrue(functionCount + " functions", functionCount > 360);
		//
		// // Mondrian has 13 variants of the Ascendants and Descendants
		// functions
		// s =
		// checkResultSet(olapDatabaseMetaData.getOlapFunctions("%scendants"),
		// FUNCTIONS_COLUMN_NAMES);
		// assertEquals(s, 13, linecount(s));
	}

	public void testDatabaseMetaDataGetHierarchies() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getHierarchies(
				catalogName, null, null, null, null), HIERARCHIES_COLUMN_NAMES);
		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_commitdate], HIERARCHY_NAME=[rdfhXXX3Alo_commitdateCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_commitdateCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_NAME=[rdfhXXX3Alo_custkeyCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_linenumber], HIERARCHY_NAME=[rdfhXXX3Alo_linenumberCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_linenumberCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_NAME=[rdfhXXX3Alo_orderdateCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderkey], HIERARCHY_NAME=[rdfhXXX3Alo_orderkeyCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderkeyCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderpriority], HIERARCHY_NAME=[rdfhXXX3Alo_orderpriorityCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderpriorityCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_ordtotalprice], HIERARCHY_NAME=[rdfhXXX3Alo_ordtotalpriceCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_ordtotalpriceCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_partkey], HIERARCHY_NAME=[rdfhXXX3Alo_partkeyCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_partkeyCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_shipmode], HIERARCHY_NAME=[rdfhXXX3Alo_shipmodeCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_shipmodeCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_shippriority], HIERARCHY_NAME=[rdfhXXX3Alo_shippriorityCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_shippriorityCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_suppkey], HIERARCHY_NAME=[rdfhXXX3Alo_suppkeyCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_supplycost], HIERARCHY_NAME=[rdfhXXX3Alo_supplycostCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_supplycostCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_tax], HIERARCHY_NAME=[rdfhXXX3Alo_taxCodeList], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_taxCodeList], HIERARCHY_GUID=, HIERARCHY_CAPTION=null, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=null, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=Measures, HIERARCHY_NAME=Measures, HIERARCHY_UNIQUE_NAME=Measures, HIERARCHY_GUID=, HIERARCHY_CAPTION=Measures, DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=Measures, STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_discount], HIERARCHY_NAME=[rdfhXXX3Alo_discount], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_discount], HIERARCHY_GUID=, HIERARCHY_CAPTION=[rdfhXXX3Alo_discount], DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=[rdfhXXX3Alo_discount], STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_quantity], HIERARCHY_NAME=[rdfhXXX3Alo_quantity], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_quantity], HIERARCHY_GUID=, HIERARCHY_CAPTION=[rdfhXXX3Alo_quantity], DIMENSION_TYPE=, HIERARCHY_CARDINALITY=, DEFAULT_MEMBER=, ALL_MEMBER=, DESCRIPTION=[rdfhXXX3Alo_quantity], STRUCTURE=, IS_VIRTUAL=, IS_READWRITE=, DIMENSION_UNIQUE_SETTINGS=, DIMENSION_IS_VISIBLE=, HIERARCHY_IS_VISIBLE=, HIERARCHY_ORDINAL=, DIMENSION_IS_SHARED=, PARENT_CHILD=\n",
				s);

	}

	public void testDatabaseMetaDataGetLevels() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getLevels(catalogName,
				null, null, null, null, null), LEVELS_COLUMN_NAMES);

		// Get normal level
		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_commitdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_commitdateCodeList], LEVEL_NAME=[rdfhXXX3Alo_commitdateCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_commitdateCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_commitdateCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_linenumber], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_linenumberCodeList], LEVEL_NAME=[rdfhXXX3Alo_linenumberCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_linenumberCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_linenumberCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderkeyCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_orderkeyCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderpriority], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderpriorityCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderpriorityCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderpriorityCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_orderpriorityCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_ordtotalprice], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_ordtotalpriceCodeList], LEVEL_NAME=[rdfhXXX3Alo_ordtotalpriceCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_ordtotalpriceCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_ordtotalpriceCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_shipmode], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_shipmodeCodeList], LEVEL_NAME=[rdfhXXX3Alo_shipmodeCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_shipmodeCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_shipmodeCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_shippriority], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_shippriorityCodeList], LEVEL_NAME=[rdfhXXX3Alo_shippriorityCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_shippriorityCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_shippriorityCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_supplycost], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_supplycostCodeList], LEVEL_NAME=[rdfhXXX3Alo_supplycostCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_supplycostCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_supplycostCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_tax], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_taxCodeList], LEVEL_NAME=[rdfhXXX3Alo_taxCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_taxCodeList], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_taxCodeList], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[2], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_custkeyCityLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCityLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[3], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_custkeyCustomerLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCustomerLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[4], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderdateYearLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderdateYearLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderdateYearMonthLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderdateYearMonthLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[2], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderdateYearMonthNumLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderdateYearMonthNumLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[3], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderdateDateLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderdateDateLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[4], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_orderdate], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearCodeList], LEVEL_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearDateLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_orderdateWeeknuminyearDateLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[2], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_partkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_partkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_partkeyMfgrLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_partkeyMfgrLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_partkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_partkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_partkeyCategoryLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_partkeyCategoryLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[2], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_partkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_partkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_partkeyBrand1Level], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_partkeyBrand1Level], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[3], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_partkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_partkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_partkeyPartLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_partkeyPartLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[4], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_suppkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_suppkeyRegionLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyRegionLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_suppkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_suppkeyNationLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyNationLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[2], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_suppkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_suppkeyCityLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyCityLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[3], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_suppkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_suppkeyCodeList], LEVEL_NAME=[rdfhXXX3Alo_suppkeySupplierLevel], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_suppkeySupplierLevel], LEVEL_GUID=, LEVEL_CAPTION=null, LEVEL_NUMBER=[4], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=null\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=Measures, HIERARCHY_UNIQUE_NAME=Measures, LEVEL_NAME=Measures, LEVEL_UNIQUE_NAME=Measures, LEVEL_GUID=, LEVEL_CAPTION=Measures, LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=Measures\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_discount], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_discount], LEVEL_NAME=[rdfhXXX3Alo_discount], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_discount], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_discount], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_quantity], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_quantity], LEVEL_NAME=[rdfhXXX3Alo_quantity], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_quantity], LEVEL_GUID=, LEVEL_CAPTION=[rdfhXXX3Alo_quantity], LEVEL_NUMBER=[1], LEVEL_CARDINALITY=[0], LEVEL_TYPE=[0x0000], CUSTOM_ROLLUP_SETTINGS=, LEVEL_UNIQUE_SETTINGS=, LEVEL_IS_VISIBLE=, DESCRIPTION=[NXXX2FA]\n",
				s);

	}

	public void testDatabaseMetaDataGetLiterals2() throws SQLException {
		// not supported
		// String s = checkResultSet(olapDatabaseMetaData.getLiterals(),
		// LITERALS_COLUMN_NAMES);
		// assertContains(
		// "LITERAL_NAME=DBLITERAL_QUOTE, LITERAL_VALUE=[, LITERAL_INVALID_CHARS=null, LITERAL_INVALID_STARTING_CHARS=null, LITERAL_MAX_LENGTH=-1",
		// s);
		// assertEquals(17, linecount(s));
	}

	public void testDatabaseMetaDataGetMeasures() throws SQLException {
		String s = checkResultSet(olapDatabaseMetaData.getMeasures(catalogName,
				null, null, null, null), MEASURES_COLUMN_NAMES);

		// There should be the measure for rdfh:lo_discount with the correct
		// aggregation function AVG (which is also a dimension)
		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Alo_discount], MEASURE_UNIQUE_NAME=[rdfhXXX3Alo_discount], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[AVG], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Alo_extendedprice], MEASURE_UNIQUE_NAME=[rdfhXXX3Alo_extendedprice], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[SUM], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Alo_quantity], MEASURE_UNIQUE_NAME=[rdfhXXX3Alo_quantity], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[SUM], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Alo_revenue], MEASURE_UNIQUE_NAME=[rdfhXXX3Alo_revenue], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[SUM], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Alo_supplycost], MEASURE_UNIQUE_NAME=[rdfhXXX3Alo_supplycost], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[SUM], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Asum_profit], MEASURE_UNIQUE_NAME=[rdfhXXX3Asum_profit], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[CALCULATED], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], MEASURE_NAME=[rdfhXXX3Asum_revenue], MEASURE_UNIQUE_NAME=[rdfhXXX3Asum_revenue], MEASURE_CAPTION=null, MEASURE_GUID=, MEASURE_AGGREGATOR=[CALCULATED], DATA_TYPE=[5], MEASURE_IS_VISIBLE=[true], LEVELS_LIST=, DESCRIPTION=\n",
				s);

	}

	public void testDatabaseMetaDataGetMeasureMember() throws SQLException {
		ResultSet resultset;
		String s;

		// with treeop ask for specific measure rdfh:lo_discount (that also has
		// is providing a degenerated dimension)
		resultset = olapDatabaseMetaData.getMembers("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]", null, null, null,
				"[rdfhXXX3Alo_discount]",
				Olap4jUtil.enumSetOf(Member.TreeOp.SELF));
		s = checkResultSet(resultset, MEMBERS_COLUMN_NAMES);

		// TestContext
		// .assertEqualsVerbose
		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=Measures, HIERARCHY_UNIQUE_NAME=Measures, LEVEL_UNIQUE_NAME=Measures, LEVEL_NUMBER=[0], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_discount], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_discount], MEMBER_TYPE=[3], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=[0], PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n",
				s);

		// Can we query all measure members?
		resultset = olapDatabaseMetaData.getMembers("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]", "Measures", null, null,
				null, null);
		s = checkResultSet(resultset, MEMBERS_COLUMN_NAMES);

		assertEquals(s, 7, linecount(s));

	}

	public void testDatabaseMetaDataGetMembers() throws SQLException {
		ResultSet resultset;
		String s;
		resultset = olapDatabaseMetaData.getMembers("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]", null, null,
				"[rdfhXXX3Alo_custkeyRegionLevel]", null, null);
		s = checkResultSet(resultset, MEMBERS_COLUMN_NAMES);

		// suppkey
		assertEquals(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_NUMBER=[1], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_NUMBER=[1], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyRegionAMERICA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAMERICA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_NUMBER=[1], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyRegionASIA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionASIA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_NUMBER=[1], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyRegionEUROPE], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionEUROPE], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_NUMBER=[1], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyRegionMIDDLEZZZEAST], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionMIDDLEZZZEAST], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n",
				s);

		// by member unique name
		resultset = olapDatabaseMetaData.getMembers("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]", "[rdfhXXX3Alo_custkey]",
				null, null, "[rdfhXXX3Alo_custkeyRegionAFRICA]", null);
		s = checkResultSet(resultset, MEMBERS_COLUMN_NAMES);
		assertEquals(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionLevel], LEVEL_NUMBER=[1], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n",
				s);

		// with treeop: children
		resultset = olapDatabaseMetaData.getMembers("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]", "[rdfhXXX3Alo_custkey]",
				null, null, "[rdfhXXX3Alo_custkeyRegionAFRICA]",
				Olap4jUtil.enumSetOf(Member.TreeOp.CHILDREN));
		s = checkResultSet(resultset, MEMBERS_COLUMN_NAMES);

		assertEquals(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_NUMBER=[2], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyNationALGERIA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationALGERIA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=[1], PARENT_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_NUMBER=[2], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyNationETHIOPIA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationETHIOPIA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=[1], PARENT_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_NUMBER=[2], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyNationKENYA], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationKENYA], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=[1], PARENT_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_NUMBER=[2], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyNationMOROCCO], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationMOROCCO], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=[1], PARENT_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], PARENT_COUNT=, TREE_OP=, DEPTH=\n"
						+ "CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_custkey], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_custkeyCodeList], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationLevel], LEVEL_NUMBER=[2], MEMBER_ORDINAL=, MEMBER_NAME=[rdfhXXX3Alo_custkeyNationMOZAMBIQUE], MEMBER_UNIQUE_NAME=[rdfhXXX3Alo_custkeyNationMOZAMBIQUE], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=[1], PARENT_UNIQUE_NAME=[rdfhXXX3Alo_custkeyRegionAFRICA], PARENT_COUNT=, TREE_OP=, DEPTH=\n",
				s);

		// Can we query for all members of the degenerated dimension
		// rdfh:lo_quantity?
		resultset = olapDatabaseMetaData.getMembers("[LdCatalog]",
				"[LdSchema]", "[rdfhZZZinstXXX3Adsd]",
				"[rdfhXXX3Alo_quantity]", null, null, null, null);
		s = checkResultSet(resultset, MEMBERS_COLUMN_NAMES);

		assertContains(
				"CATALOG_NAME=[LdCatalog], SCHEMA_NAME=[LdSchema], CUBE_NAME=[rdfhZZZinstXXX3Adsd], DIMENSION_UNIQUE_NAME=[rdfhXXX3Alo_quantity], HIERARCHY_UNIQUE_NAME=[rdfhXXX3Alo_quantity], LEVEL_UNIQUE_NAME=[rdfhXXX3Alo_quantity], LEVEL_NUMBER=[0], MEMBER_ORDINAL=, MEMBER_NAME=[1], MEMBER_UNIQUE_NAME=[1], MEMBER_TYPE=[1], MEMBER_GUID=, MEMBER_CAPTION=null, CHILDREN_CARDINALITY=, PARENT_LEVEL=null, PARENT_UNIQUE_NAME=null, PARENT_COUNT=, TREE_OP=, DEPTH=\n",
				s);

		assertEquals(s, 50, linecount(s));

	}

	public void testDatabaseMetaDataGetSets() throws SQLException {
		// not supported
		// String s = checkResultSet(
		// olapDatabaseMetaData.getSets(catalogName, null, null, null),
		// SETS_COLUMN_NAMES);
		// TestContext
		// .assertEqualsVerbose(
		// "CATALOG_NAME="
		// + catalogName
		// +
		// ", SCHEMA_NAME=FoodMart, CUBE_NAME=Warehouse, SET_NAME=[Top Sellers], SCOPE=1\n"
		// + "CATALOG_NAME="
		// + catalogName
		// +
		// ", SCHEMA_NAME=FoodMart, CUBE_NAME=Warehouse and Sales, SET_NAME=[Top Sellers], SCOPE=1\n",
		// s);
		//
		// s = checkResultSet(olapDatabaseMetaData.getSets(catalogName, null,
		// null, "non existent set"), SETS_COLUMN_NAMES);
		// TestContext.assertEqualsVerbose("", s);
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
