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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.olap4j.test.TestContext;

/**
 * Unit test for OLAP4LD Queries on external example data and Drill-Across operator.
 * 
 * To also help developers with Drill-Across queries, we provide the same as Example_QB_Datasets_QueryTest for Drill-Across queries.
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
public class Example_QB_Datasets_ConvertCube_QueryTest extends TestCase {
	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private OlapConnection olapConnection;
	private OlapStatement stmt;

	public Example_QB_Datasets_ConvertCube_QueryTest() throws SQLException {	
		
			Olap4ldUtil.prepareLogging();
			
			// Logging
			// For debugging purposes
			Olap4ldUtil._log.setLevel(Level.CONFIG);
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

	
	// Variations of the UNEMPLOY query
	
	/**
	 * Specific measures from two datasets. 
	 */
	public void executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_MoreDatasets_AutomaticConvert() {
		
		// Maybe ask for specific measure? 
		// Add another dataset? XXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Fnama_aux_gphXXX23ds
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValue]}, Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23indic_na])) ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  |  00      | 461.333333333333333333333333,1.1,140.3 |",
				result);
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

			cset = stmt.executeOlapQuery(mdxString);

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
