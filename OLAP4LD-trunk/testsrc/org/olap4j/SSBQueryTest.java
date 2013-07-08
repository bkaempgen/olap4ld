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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.catalina.tribes.membership.Membership;
import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.test.TestContext;

/**
 * Unit test for olap4j metadata methods.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class SSBQueryTest extends TestCase {
	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private OlapConnection olapConnection;
	private OlapStatement stmt;
	private MdxParserFactory parserFactory;
	private MdxParser parser;

	public SSBQueryTest() throws SQLException {
	}

	protected void setUp() throws SQLException {
		connection = tester.createConnection();
		connection.getCatalog();
		olapConnection = tester.getWrapper().unwrap(connection,
				OlapConnection.class);
		olapConnection.getMetaData();

		// Create a statement based upon the object model.
		// One can simply keep open the statement and issue new queries.
		OlapConnection olapconnection = (OlapConnection) connection;
		this.stmt = null;
		try {
			stmt = olapconnection.createStatement();
		} catch (OlapException e) {
			System.out.println("Validation failed: " + e);
			return;
		}

		this.parserFactory = olapconnection.getParserFactory();
		this.parser = parserFactory.createMdxParser(olapconnection);
	}

	protected void tearDown() throws Exception {
		if (connection != null && !connection.isClosed()) {
			connection.close();
			connection = null;
		}
	}
	
	/**
	 * Generic Query
	 */
	public void testSsb001Example() {
		
		try {
			
			//String name = "httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23ds";
			String name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.ttl#ds";
			name = URLEncoder.encode(name, "UTF-8");
			name = name.replace("%", "XXX");
			name = name.replace(".", "YYY");
			name = name.replace("-", "ZZZ");
			// xmla4js is attaching square brackets automatically
			Cube cube = olapConnection.getOlapDatabases().get(0).getCatalogs().get(0).getSchemas().get(0).getCubes().get("["+name+"]");
			// Currently, we have to first query for dimensions.
			List<Dimension> dimensions = cube.getDimensions();
			assertEquals(5, dimensions.size());
			for (Dimension dimension : dimensions) {
				List<Member> members = dimension.getHierarchies().get(0).getLevels().get(0).getMembers();
				assertEquals(true, members.size() >= 1);
			}
			List<Measure> measures = cube.getMeasures();
			assertEquals(5, measures.size());
			
			
			String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_supplycost]} ON COLUMNS,CrossJoin({Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_custkeyCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_orderdateCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_partkeyCodeList])}, {Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_suppkeyCodeList])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23ds]");
			assertContains("| Customer  1 | Date 19940101 | Part  1 | Supplier  1 | 7116579.0 |      4.0 |     7413105.0 |     51.0 |   261639.0 |"
					, result);
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		
	}
	
	/**
	 * Generic Query
	 */
	public void testEurostatEmploymentRateExample() {
		
		try {
			
			//String name = "httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23ds";
			String name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tsdec420_ds.rdf#ds";
			name = URLEncoder.encode(name, "UTF-8");
			name = name.replace("%", "XXX");
			name = name.replace(".", "YYY");
			name = name.replace("-", "ZZZ");
			// xmla4js is attaching square brackets automatically
			Cube cube = olapConnection.getOlapDatabases().get(0).getCatalogs().get(0).getSchemas().get(0).getCubes().get("["+name+"]");
			// Currently, we have to first query for dimensions.
			List<Dimension> dimensions = cube.getDimensions();
			assertEquals(5, dimensions.size());
			for (Dimension dimension : dimensions) {
				List<Member> members = dimension.getHierarchies().get(0).getLevels().get(0).getMembers();
				assertEquals(true, members.size() >= 1);
			}
			List<Measure> measures = cube.getMeasures();
			assertEquals(5, measures.size());
			
			
			String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_supplycost]} ON COLUMNS,CrossJoin({Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_custkeyCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_orderdateCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_partkeyCodeList])}, {Members([httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23lo_suppkeyCodeList])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2FlocalhostXXX2Ffios_xmla4jsXXX2FexampleYYYttlXXX23ds]");
			assertContains("| Customer  1 | Date 19940101 | Part  1 | Supplier  1 | 7116579.0 |      4.0 |     7413105.0 |     51.0 |   261639.0 |"
					, result);
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		
	}

//	/**
//	 * Generic Query
//	 */
//	public void testGESIS() {
//		String result = executeStatement("SELECT {[Measures].[Measures].[Measures].[http://purlYYYorg/linked-data/sdmx/2009/measure#obsValue]} ON COLUMNS,	"
//				+ "{[http://lodYYYgesisYYYorg/lodpilot/ALLBUS/vocabYYYrdf#geo].[http://lodYYYgesisYYYorg/lodpilot/ALLBUS/geoYYYrdf#list].[http://lodYYYgesisYYYorg/lodpilot/ALLBUS/geoYYYrdf#list].[http://lodYYYgesisYYYorg/lodpilot/ALLBUS/geoYYYrdf#00]} ON ROWS "
//				+ "FROM [http://lodYYYgesisYYYorg/lodpilot/ALLBUS/ZA4570v590YYYrdf#ds]");
//
//		assertContains("1.436037326E9", result);
//	}
// 
//	/**
//	 * Generic Query
//	 */
//	public void testSSB001() {
//		String result = executeStatement("SELECT {Measures.Measures.Measures.[rdfhXXX3Alo_revenue]} ON COLUMNS, "
//				+ "{[rdfhXXX3Alo_custkey].[rdfhXXX3Alo_custkeyCodeList].[rdfhXXX3Alo_custkeyCustomerLevel].[rdfhZZZinstXXX3Acustomer_277]} ON ROWS "
//				+ "FROM [http://olap4ldYYYgooglecodeYYYcom/git/OLAP4LD-trunk/tests/ssb001/ttl/lineorder_qbYYYttl#ds]");
//
//		assertContains("1.436037326E9", result);
//	}
//
//	/**
//	 * Generic Query
//	 */
//	public void testGenericQuery() {
//		String result = executeStatement("SELECT {Measures.Measures.Measures.[rdfhXXX3Alo_revenue]} ON COLUMNS, "
//				+ "{[rdfhXXX3Alo_custkey].[rdfhXXX3Alo_custkeyCodeList].[rdfhXXX3Alo_custkeyCustomerLevel].[rdfhZZZinstXXX3Acustomer_178]} ON ROWS "
//				+ "FROM [rdfhZZZinstXXX3Adsd]");
//
//		assertContains("1.436037326E9", result);
//	}
//
//	/**
//	 * Query 1.1
//	 */
//	public void testSSB_Q_1_1() {
//		String result = executeStatement("WITH MEMBER [Revenue] as 'Measures.Measures.Measures.[rdfhXXX3Alo_extendedprice] * Measures.Measures.Measures.[rdfhXXX3Alo_discount]' "
//				+ "SELECT {[Revenue]} ON COLUMNS, "
//				+ "{[rdfhXXX3Alo_orderdate].[rdfhXXX3Alo_orderdateCodeList].[rdfhXXX3Alo_orderdateYearLevel].[rdfhXXX3Alo_orderdateYear1993]} ON ROWS "
//				+ "FROM [rdfhZZZinstXXX3Adsd] "
//				+ "WHERE CrossJoin(Filter(Members([rdfhXXX3Alo_quantity]), "
//				+ "Cast(Name(CurrentMember([rdfhXXX3Alo_quantity])) as NUMERIC) < 25), "
//				+ "Filter(Members([rdfhXXX3Alo_discount]), "
//				+ "Cast(Name(CurrentMember([rdfhXXX3Alo_discount])) as NUMERIC) >= 1 "
//				+ "and Cast(Name(CurrentMember([rdfhXXX3Alo_discount])) as NUMERIC) <= 3))");
//
//		assertContains("4182760987", result);
//	}
//
//	/**
//	 * Query 1.2
//	 */
//	public void testSSB_Q_1_2() {
//		String result = executeStatement("WITH MEMBER Measures.[Revenue] as 'Measures.Measures.Measures.[rdfh:lo_extendedprice] * Measures.Measures.Measures.[rdfh:lo_discount]' "
//				+ "SELECT {Measures.[Revenue]} ON COLUMNS, "
//				+ "{[rdfh:lo_orderdate].[rdfh:lo_orderdateCodeList].[rdfh:lo_orderdateYearMonthNumLevel].[rdfh:lo_orderdateYearMonthNum199401]} ON ROWS "
//				+ "FROM [rdfh-inst:dsd] "
//				+ "WHERE CrossJoin(Filter(Members([rdfh:lo_quantity]), "
//				+ "Cast(Name(CurrentMember([rdfh:lo_quantity])) as NUMERIC) >= 26 and Cast(Name(CurrentMember([rdfh:lo_quantity])) as NUMERIC) <= 35), "
//				+ "Filter(Members([rdfh:lo_discount]), "
//				+ "Cast(Name(CurrentMember([rdfh:lo_discount])) as NUMERIC) >= 4 "
//				+ "and Cast(Name(CurrentMember([rdfh:lo_discount])) as NUMERIC) <= 6)) ");
//
//		assertContains("1036391395", result);
//	}
//
//	/**
//	 * Query 1.3
//	 */
//	public void testSSB_Q_1_3() {
//		String result = executeStatement("WITH MEMBER Measures.[Revenue] as 'Measures.Measures.Measures.[rdfh:lo_extendedprice] * Measures.Measures.Measures.[rdfh:lo_discount]' "
//				+ "SELECT {Measures.[Revenue]} ON COLUMNS, "
//				+ "{[rdfh:lo_orderdate].[rdfh:lo_orderdateWeeknuminyearCodeList].[rdfh:lo_orderdateWeeknuminyearLevel].[rdfh:lo_orderdateWeeknuminyear19946]} ON ROWS "
//				+ "FROM [rdfh-inst:dsd] "
//				+ "WHERE CrossJoin(Filter(Members([rdfh:lo_quantity]), "
//				+ "Cast(Name(CurrentMember([rdfh:lo_quantity])) as NUMERIC) >= 26 and Cast(Name(CurrentMember([rdfh:lo_quantity])) as NUMERIC) <= 35), "
//				+ "Filter(Members([rdfh:lo_discount]), "
//				+ "Cast(Name(CurrentMember([rdfh:lo_discount])) as NUMERIC) >= 5 "
//				+ "and Cast(Name(CurrentMember([rdfh:lo_discount])) as NUMERIC) <= 7))");
//
//		assertContains("303927274", result);
//	}
//
//	/**
//	 * Query 2.1
//	 * 
//	 * XXX: Does not fit, yet, since Children is not implemented, yet.
//	 */
//	public void notestSSB_Q_2_1() {
//		executeStatement("SELECT {Measures.Measures.Measures.[rdfh:lo_revenue]} ON COLUMNS, "
//				+ "CrossJoin(Members([rdfh:lo_orderdate].[rdfh:lo_orderdateCodeList].[rdfh:lo_orderdateYearLevel]), "
//				+ "{[rdfh:lo_partkey].[rdfh:lo_partkeyCodeList].[rdfh:lo_partkeyCategoryLevel].[rdfh:lo_partkeyCategoryMFGR-12]}) ON ROWS "
//				+ "FROM [rdfh-inst:dsd] "
//				+ "WHERE {[rdfh:lo_suppkey].[rdfh:lo_suppkeyCodeList].[rdfh:lo_suppkeyRegionLevel].[rdfh:lo_suppkeyRegionAMERICA]}");
//
//	}
//
//	/**
//	 * Query 2.2
//	 */
//	public void notestSSB_Q_2_2() {
//		executeStatement("SELECT {Measures.Measures.Measures.[rdfh:lo_revenue]} ON COLUMNS, "
//				+ "CrossJoin(Members([rdfh:lo_orderdate].[rdfh:lo_orderdateCodeList].[rdfh:lo_orderdateYearLevel]), "
//				+ "{[rdfh:lo_partkey].[rdfh:lo_partkeyCodeList].[rdfh:lo_partkeyCategoryLevel].[rdfh:lo_partkeyCategoryMFGR-12]}) ON ROWS "
//				+ "FROM [rdfh-inst:dsd] "
//				+ "WHERE {[rdfh:lo_suppkey].[rdfh:lo_suppkeyCodeList].[rdfh:lo_suppkeyRegionLevel].[rdfh:lo_suppkeyRegionAMERICA]}");
//	}

	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
	}

	private String executeStatement(String mdxString) {
		// Execute the statement.
		String resultString = "";
		CellSet cset;
		try {

			SelectNode select = parser.parseSelect(mdxString);
			MdxValidator validator = parserFactory
					.createMdxValidator(olapConnection);
			select = validator.validateSelect(select);
			cset = stmt.executeOlapQuery(select);

			// String s = TestContext.toString(cset);
			resultString = toString(cset, Format.RECTANGULAR);

			System.out.println("Output:");
			System.out.println(resultString);

		} catch (OlapException e) {
			System.out.println("Execution failed: " + e);
		}
		return resultString;
	}

	/**
	 * Converts a {@link CellSet} to text.
	 * 
	 * @param cellSet
	 *            Query result
	 * @param format
	 *            Format
	 * @return Result as text
	 */
	static String toString(CellSet cellSet, Format format) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		switch (format) {
		case TRADITIONAL:
			new TraditionalCellSetFormatter().format(cellSet, pw);
			break;
		case COMPACT_RECTANGULAR:
		case RECTANGULAR:
			new RectangularCellSetFormatter(
					format == Format.COMPACT_RECTANGULAR).format(cellSet, pw);
			break;
		}
		pw.flush();
		return sw.toString();
	}
}

// End MetadataTest.java
