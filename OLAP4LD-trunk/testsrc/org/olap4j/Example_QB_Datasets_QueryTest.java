/*
//
//Licensed to Benedikt Kämpgen under one or more contributor license
//agreements. See the NOTICE file distributed with this work for
//additional information regarding copyright ownership.
//
//Benedikt Kämpgen licenses this file to you under the Apache License,
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
import java.util.logging.Level;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.test.TestContext;

/**
 * Unit test for OLAP4LD Queries on external example data.
 * 
 * For most Example QB Datasets given at [1], we want to add a unit test.
 * 
 * [1] http://www.linked-data-cubes.org/index.php/Example_QB_Datasets
 * 
 * Remember to set test.properties:
 * org.olap4j.test.helperClassName=org.olap4j.LdRemoteOlap4jTester
 * org.olap4j.RemoteXmlaTester
 * .JdbcUrl=jdbc:ld://olap4ld;Catalog=LdCatalog;JdbcDrivers
 * =com.mysql.jdbc.Driver
 * ;Server=http://;Database=EMBEDDEDSESAME;Datastructuredefinitions=;Datasets=;
 * 
 * @version 0.1
 * @author bkaempgen
 */
public class Example_QB_Datasets_QueryTest extends TestCase {
	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private OlapConnection olapConnection;
	private OlapStatement stmt;
	private MdxParserFactory parserFactory;
	private MdxParser parser;

	public Example_QB_Datasets_QueryTest() throws SQLException {
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
		if (olapConnection != null && !olapConnection.isClosed()) {
			olapConnection.close();
			olapConnection = null;
		}

		if (connection != null && !connection.isClosed()) {
			connection.close();
			connection = null;
		}
	}

	private void metadataTest(String dsUri, int numberOfDimensions,
			int numberOfMeasures) {
		try {

			// We have to use MDX encoded name
			String name = URLEncoder.encode(dsUri, "UTF-8");
			name = name.replace("%", "XXX");
			name = name.replace(".", "YYY");
			name = name.replace("-", "ZZZ");
			// xmla4js is attaching square brackets automatically
			// xmla-server is using a set of values for a restriction.
			Cube cube = olapConnection.getOlapDatabases().get(0).getCatalogs()
					.get(0).getSchemas().get(0).getCubes()
					.get("[" + name + "]");

			// Currently, we have to first query for dimensions.
			List<Dimension> dimensions = cube.getDimensions();

			// Number of dimensions
			assertEquals(numberOfDimensions, dimensions.size());
			for (Dimension dimension : dimensions) {
				List<Member> members = dimension.getHierarchies().get(0)
						.getLevels().get(0).getMembers();

				// Each dimension should have some members
				assertEquals(true, members.size() >= 1);
			}

			List<Measure> measures = cube.getMeasures();

			// Number of measures
			assertEquals(numberOfMeasures, measures.size());
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Eventually, we want to allow to set the database for an olap4ld
	 * connection.
	 */
	public void testSettingDatabase() {
		// String dsUri =
		// "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.ttl#ds";
		//
		// try {
		// olapConnection.setDatabase("http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.ttl#ds");
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	/**
	 * We test Example SSB 001 dataset that we created manually.
	 * 
	 * Properties: Example SSB dataset uses hasTopConcept for codelists. No
	 * degenerated dimensions. And explicitly defines aggregation functions.
	 */
	public void testExampleSsb001Metadata() {
		String dsUri = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.ttl#ds";
		metadataTest(dsUri, 5, 5);
	}

	/**
	 * Do crossjoin over all dimensions on rows and show all measures on
	 * columns.
	 */
	public void testExampleSsb001OlapFullCrossJoin() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_supplycost]} ON COLUMNS,CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_orderdateCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_partkeyCodeList])}, {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_suppkeyCodeList])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]");

		assertContains(
				"| revenue   | discount | extendedprice | quantity | supplycost |",
				result);
		assertContains(
				"|  | Customer  1 |  | Date 19940101 |  | Part  1 |  | Supplier  1 | 2372193.0 |      4.0 |     2471035.0 |     17.0 |    87213.0 |",
				result);
		assertContains(
				"|  | Customer  2 |  | Date 19940101 |  | Part  1 |  | Supplier  1 | 2372193.0 |      4.0 |     2471035.0 |     17.0 |    87213.0 |",
				result);
	}

	/**
	 * Do crossjoin over all dimensions on rows and show all measures on
	 * columns, but this time with NON EMPTY clause for rows.
	 */
	public void testExampleSsb001OlapFullCrossJoinNotEmpty() {

		String result = executeStatement("SELECT NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_supplycost]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_orderdateCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_partkeyCodeList])}, {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_suppkeyCodeList])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]");

		assertContains(
				"| revenue   | discount | extendedprice | quantity | supplycost |",
				result);
		assertContains(
				"|  | Customer  1 |  | Date 19940101 |  | Part  1 |  | Supplier  1 | 2372193.0 |      4.0 |     2471035.0 |     17.0 |    87213.0 |",
				result);

		// Empty rows are not shown
		assertEquals(
				false,
				result.contains("|  |             |  | Date 19940601 |  | Part  1 |  | Supplier  1 |           |          |               |          |            |"));
	}

	/**
	 * We query for customers on rows and parts on columns and explicitly select
	 * lo_extendedprice as measure per WHERE clause.
	 */
	public void testExampleSsb001OlapDiceMeasure() {

		String result = executeStatement("SELECT {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_partkeyCodeList])} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds] WHERE {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_extendedprice]}");

		assertContains("| Part  1   | Part  2   |", result);
		assertContains("| Customer  1 | 2471035.0 | 2471035.0 |", result);
	}

	/**
	 * Test Example cik/90983#id COSTCO WHOLESALE CORP /NEW (example form)
	 * Properties: Here, aggregation function is not explicitly given such that
	 * AVG and COUNT are automatically added. DS and DSD are located in the same
	 * file. Dimensions ranges are given in linked files (e.g., for
	 * http://edgarwrap.ontologycentral.com/vocab/edgar#issuer). All ranges are
	 * non-skos-concepts, thus, there is not code list.
	 */
	public void testExampleEdgarCOSTCOMetadata() {
		String name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/edgarwrap/0001193125-10-230379.rdf#ds";
		metadataTest(name, 6, 2);
	}

	/**
	 * Do typical first query: Select certain measure (avg) and first dimension.
	 */
	public void testExampleEdgarCOSTCOOlapTypicalCrossjoin1() {
		String result;
		result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]");
		assertContains("| 1997-08-01 |        943000.0 |", result);
	}

	/**
	 * This time, do crossjoin on measure, dtstart, dtend on columns, subject on
	 * rows.
	 * 
	 */
	public void testExampleEdgarCOSTCOOlapLargeDateCrossjoinColumns() {
		String result;
		result = executeStatement("	SELECT CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]}, CrossJoin({Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart])}, {Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtend])})) ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]");
		assertContains(
				"|  | Assets                                                                                                                                                              |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                 |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |   1.0341E10 |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |      1.09895E10 |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |      1.19075E10 |",
				result);
	}

	/**
	 * Same as above but with NON EMPTY on columns.
	 * 
	 * XXX: With Non-empty, this does not work, yet
	 */
	public void testExampleEdgarCOSTCOOlapLargeDateCrossjoinWithNonEmptyColumns() {
		// String result;
		// result =
		// executeStatement("	SELECT NON EMPTY CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]}, CrossJoin({Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart])}, {Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtend])})) ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]");
		// assertContains(
		// "|  | Assets                                                                                                                                                              |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                 |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |   1.0341E10 |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |      1.09895E10 |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |                |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |             |      1.19075E10 |",
		// result);
		// TEST?
	}

	/**
	 * Test cik/90983#id COSTCO WHOLESALE CORP /NEW (example form), this time
	 * not the example.
	 */
	public void testOriginalEdgarCOSTCOMetadata() {
		String name = "http://edgarwrap.ontologycentral.com/archive/909832/0001193125-10-230379#ds";
		// name =
		// "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/edgarwrap/0001193125-10-230379.rdf#ds";
		// name =
		// "http://localhost:8888/archive/909832/0001193125-10-230379#ds";

		metadataTest(name, 6, 2);
	}

	/**
	 * Test cik/90983#id COSTCO WHOLESALE CORP /NEW (example form): All subjects
	 * in rows.
	 */
	public void testOriginalEdgarCOSTCOOlapBothMeasuresPlusSubject() {
		String result;
		result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])} ON ROWS FROM [httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FarchiveXXX2F909832XXX2F0001193125ZZZ10ZZZ230379XXX23ds]");
		assertContains(
				"| Fair value measurement with unobservable inputs reconciliation recurring basis asset value                                                                          |        9777777.78 |         9.0 |",
				result);
	}

	public void testOriginalEurostatGDPpercapitainPPSMetadata() {
		String name = "http://estatwrap.ontologycentral.com/id/tec00114#ds";
		// name = "http://estatwrap.ontologycentral.com/id/tec00114";
		// name = "http://localhost:8888/id/tec00114#ds";
		name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tec00114_ds.rdf#ds";
		metadataTest(name, 5, 2);
	}
	
	
		
	
	public void testEurostatEmploymentRateExampleMetadata() {
		String name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tsdec420_ds.rdf#ds";
		// name = "http://estatwrap.ontologycentral.com/id/tec00114";
		metadataTest(name, 6, 1);
	}

	/**
	 * Generic Query
	 */
	public void testEurostatEmploymentRateExampleOlap() {
		String result;
		// Query asking for date on rows, sex on columns.
		result = executeStatement("SELECT {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsdYYYrdfXXX23cl_sex])} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]");
		assertContains("|   F   |   M   |   T   |", result);
		assertContains("| 2005 | 62.42 | 77.73 | 70.04 |", result);
		assertContains("| 2012 | 62.09 | 74.12 |  68.1 |", result);

		// Query asking for date and geo on rows, sex on columns.
		result = executeStatement("SELECT {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsdYYYrdfXXX23cl_sex])} ON COLUMNS,CrossJoin({Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])}, {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])}) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]");
		assertContains("|   F  |   M  |   T  |", result);
		assertContains("|  | 2005 |  |   AT   | 64.9 | 78.5 | 71.7 |", result);
		assertContains("|  | 2012 |  |   AT   | 70.3 | 80.9 | 75.6 |", result);

		// Query asking for date on rows, sex and geo on columns.
		result = executeStatement("SELECT CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsdYYYrdfXXX23cl_sex])}, {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])}) ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]");
		assertContains(
				"|   AT |   BE |   BG |   CH |   CY |   CZ |   DE |   DK |   EE |   EL |   ES |  27 EU |   FI |   FR |   HR |   HU |",
				result);
		assertContains(
				"| 2005 | 64.9 | 58.6 | 57.1 | 72.7 | 63.8 | 61.3 | 63.1 | 73.7 | 69.0 | 49.6 | 54.4 |   60.0 | 70.8 | 63.7 | 52.8 | 55.6 | 62.4 | 81.2 | 48.4 | 61.7 | 66.6 | 58.4 | 65.7 |      | 35.1 | 67.6 | 74.6 | 51.7 | 66.0 | 56.9 | 75.5 | 66.2 | 56.7 |      | 68.5 | 68.1 | 78.5 | 74.3 | 66.8 | 87.1 | 85.5 | 80.1 | 75.6 | 82.3 | 75.4 | 79.8 | 79.9 |   76.0 | 75.1 | 75.3 | 67.5 | 69.2 | 82.8 | 89.6 | 74.8 | 86.1 | 74.9 | 79.4 | 75.4 |      | 80.6 | 82.4 | 81.6 | 65.1 | 78.7 | 70.4 | 80.7 | 75.8 | 72.5 |      | 82.0 | 81.7 | 71.7 | 66.5 | 61.9 | 79.9 | 74.4 | 70.7 | 69.4 | 78.0 | 72.0 | 64.6 | 67.2 |   68.0 | 73.0 | 69.4 | 60.0 | 62.2 | 72.6 | 85.5 | 61.6 | 73.9 | 70.6 | 69.0 | 70.3 |      | 57.9 | 75.1 | 78.2 | 58.3 | 72.3 | 63.6 | 78.1 | 71.1 | 64.5 |      | 75.2 | 74.8 |",
				result);
		assertContains(
				"| 2012 | 70.3 | 61.7 | 60.2 | 76.0 | 64.8 | 62.5 | 71.5 | 72.2 | 69.3 | 45.2 | 54.0 |   62.4 | 72.5 | 65.0 | 50.2 | 56.4 | 59.4 | 79.1 | 50.5 |      | 67.9 | 64.1 | 66.4 | 38.7 | 46.8 | 71.9 | 77.3 | 57.5 | 63.1 | 56.3 | 76.8 | 64.6 | 57.3 | 30.9 | 68.4 |      | 80.9 | 72.7 | 65.8 | 87.9 | 76.1 | 80.2 | 81.8 | 78.6 | 75.2 | 65.3 | 64.5 |   74.6 | 75.5 | 73.8 | 60.6 | 68.1 | 68.1 | 84.4 | 71.6 |      | 69.4 | 78.5 | 70.2 | 57.5 | 79.0 | 82.5 | 82.4 | 72.0 | 69.9 | 71.4 | 81.9 | 71.8 | 72.8 | 75.0 | 80.0 |      | 75.6 | 67.2 | 63.0 | 82.0 | 70.2 | 71.5 | 76.7 | 75.4 | 72.1 | 55.3 | 59.3 |   68.5 | 74.0 | 69.3 | 55.3 | 62.1 | 63.7 | 81.8 | 61.0 |      | 68.7 | 71.4 | 68.2 | 48.2 | 63.1 | 77.2 | 79.9 | 64.7 | 66.5 | 63.8 | 79.4 | 68.3 | 65.1 | 52.8 | 74.2 |      |",
				result);

		// Query asking for three dimensions at a time.
		result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23employment_rate]} ON COLUMNS,CrossJoin({Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsdYYYrdfXXX23cl_sex])}, {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])})) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]");
		assertContains("| 2005 |  |   F |  |   AT   |             64.9 |",
				result);
		assertContains("| 2012 |  |   F |  |   AT   |             70.3 |",
				result);

	}

	public void testEurostatRealGDPGrowthRateExampleMetadata() {

		String name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tsieb020_ds.rdf#ds";
		// name = "http://estatwrap.ontologycentral.com/id/tec00114";
		metadataTest(name, 3, 1);
	}

	/**
	 * Generic Query
	 */
	public void testEurostatRealGDPGrowthRateExampleOlap() {

		String result = executeStatement("SELECT {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23time])} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsieb020_dsdYYYrdfXXX23CL_geo])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsieb020_dsYYYrdfXXX23ds]");
		assertContains(
				"|  2005 |  2006 |  2007 |  2008 |  2009 |  2010 |  2011 |  2012 |",
				result);
		assertContains(
				"|   AT   |   2.5 |   3.6 |   3.7 |   2.2 |  -3.9 |   2.0 |   1.7 |   2.1 |",
				result);
		assertContains(
				"|   US   |   3.1 |   2.7 |   1.9 |   0.0 |  -2.6 |   2.8 |   2.1 |   2.5 |",
				result);

	}

	public void testSmartDbWrapExampleMetadata() {

		String name = "http://smartdbwrap.appspot.com/id/locationdataset/AD0514/Q";
		// name = "http://estatwrap.ontologycentral.com/id/tec00114";
		metadataTest(name, 7, 2);
	}

	/**
	 * Generic Query
	 */
	public void testSmartDbWrapExampleOlap() {

		String result = executeStatement("SELECT {Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Flocation])} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fyear])} ON ROWS FROM [httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FidXXX2FlocationdatasetXXX2FAD0514XXX2FQ]");
		assertContains("|  1974 |      2.6 |", result);
	}

	/**
	 * Generic Query for both measures with certain values
	 */
	public void testSmartDbWrapExampleOlapMeasure() {

		// XXX: Does not work currently, since currently measures are taken as
		// cubes:
		// "A cube should be a qb:DataSet and serve via qb:structure a qb:DataStructureDefinition, also this one http://smartdbwrap.appspot.com/obsValueAGGFUNCAVG!"

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FobsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FobsValueAGGFUNCCOUNT]} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fanalysis_Object])} ON ROWS FROM [httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FidXXX2FlocationdatasetXXX2FAD0514XXX2FQ]");
		assertContains(
				"|   Q |                  3.19 |                    24.0 |",
				result);
	}

	public void testYahooFinanceWrapExampleMetadata() {

		String name = "http://yahoofinancewrap.appspot.com/archive/BAC/2012-12-12#ds";
		// name = "http://estatwrap.ontologycentral.com/id/tec00114";
		metadataTest(name, 5, 2);
	}

	/**
	 * Generic Query
	 */
	public void testYahooFinanceWrapExampleOlap() {

		// Problem:
		String result = executeStatement("SELECT {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FvocabXXX2FyahooXXX23subject])} ON ROWS FROM [httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FarchiveXXX2FBACXXX2F2012ZZZ12ZZZ12XXX23ds]");
		assertContains("| Adjusted Closing Price |      10.59 |", result);

	}

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
