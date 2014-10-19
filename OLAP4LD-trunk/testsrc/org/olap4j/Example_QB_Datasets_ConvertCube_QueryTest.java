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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.linkeddata.BaseCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.ConvertCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.ReconciliationCorrespondence;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.olap4j.test.TestContext;

/**
 * Unit test for OLAP4LD Queries on external example data and Drill-Across and
 * Convert-Cube and Merge-Cubes operator.
 * 
 * To also help developers with Convert-Cube and Merge-Cubes queries, we provide
 * the same as Example_QB_Datasets_QueryTest for those queries.
 * 
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
	 * 
	 * Here we see that we get three values per cell because due to reasoning we
	 * have another dimension which we should add to column as well.
	 * 
	 */
	public void executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_AutomaticConvert() {

		// Maybe ask for specific measure?
		// Add another dataset?
		// XXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Fnama_aux_gphXXX23ds
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValue]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// With both COMP_YES and COMP_PERCNOS,
		// Should be correct: GDP Growth 1.1, Answers to survey, COMP_YES,
		// Unemployment Fear Metric from COMP_PERCNOS: 0.826; Dice: Germany,
		// 2008
		// Result as manually computed: Unemployment Fear Metric 0.826
		assertContains(
				"|  |      |  |  00      | 1144 160 80,1.1,240 240 240,0.826589595375722543352601 0.826589595375722543352601 0.826589595375722543352601 |",
				result);

		// Result as in ISEM paper: 0.9275 Dice: Germany, 1980
		assertContains(
				"|  |      |  |  00      |         1126 42 46,88 88 88,0.927512355848434925864909 0.927512355848434925864909 0.927512355848434925864909 |",
				result);
	}

	public void executeDrillAcrossGdpCap_AutomaticConvert() {

		// Maybe ask for specific measure?
		// Add another dataset?
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValue]}, Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23indic_na])) ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Fnama_aux_gphXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Fnama_gdp_cXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Fdemo_pjanXXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: | | | | 00 | 28100 104.1 4.1 27300 | 28000 28000
		// 103.8 3.8 |
		// We distinguish different NGDPH | RGDPH (with different units)

		assertContains(
				"|  |      |  |  00      | 461.333333333333333333333333,1.1,140.3 |",
				result);
	}

	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
	}

	/**
	 * 
	 * The ordering of Merge-Cubes is relevant but every Merge-Cube may only be
	 * used once in a query plan (no cycles).
	 * 
	 * Implementation for GlobalCube paper: noc(dp,ds,mc)
	 * 
	 * @param dp
	 *            Depth of query plan
	 * @param ds
	 *            Number of datasets (converted or not converted)
	 * @param mc
	 *            Number of merge-cubes
	 * @return Number of query plans
	 */
	public static int numberForMergeChildren(int dp, int ds, int mc) {

		if (dp == 0) {

			return ds;
		}

		int no = 0;

		// Both same depth
		no += mc * mc * numberForMergeChildren(dp - 1, ds, mc - 1)
				* numberForMergeChildren(dp - 1, ds, mc - 1);

		// Left
		for (int i = 0; i < dp - 1; i++) {
			no += mc * mc * numberForMergeChildren(dp - 1, ds, mc - 1)
					* numberForMergeChildren(i, ds, mc - 1);
		}

		// Right
		for (int i = 0; i < dp - 1; i++) {
			no += mc * mc * numberForMergeChildren(dp - 1, ds, mc - 1)
					* numberForMergeChildren(i, ds, mc - 1);
		}
		return no;
	}

	/**
	 * The ordering of Merge-Cubes is relevant but every Merge-Cube may only be
	 * used once in a query plan.
	 * 
	 * XXX: Is not up to date to numberForMergeChildren anymore.
	 * 
	 * @param depth
	 *            Depth of query plan
	 * @param datasets
	 *            Number of datasets
	 * @param correspondences
	 *            Merge
	 * @return
	 */
	public static List<LogicalOlapOp> generateMergeLqpsForDepth(int depth,
			String[] datasets,
			List<ReconciliationCorrespondence> correspondences) {

		List<LogicalOlapOp> newlqps = new ArrayList<LogicalOlapOp>();

		if (depth == 0) {

			for (int i = 0; i < datasets.length; i++) {
				newlqps.add(new BaseCubeOp(datasets[i]));
			}
			return newlqps;
		}

		// Both same depth.

		for (ReconciliationCorrespondence selectedCorrespondence : correspondences) {

			List<ReconciliationCorrespondence> reducedcorrespondences = new ArrayList<ReconciliationCorrespondence>();

			for (ReconciliationCorrespondence aCorrespondence : correspondences) {
				// Only if not correspondence
				if (!aCorrespondence.getname().equals(
						selectedCorrespondence.getname())) {
					reducedcorrespondences.add(aCorrespondence);
				}
			}

			// Here, the same correspondence is not allowed to be used.
			List<LogicalOlapOp> nextdepthlqps = generateMergeLqpsForDepth(
					depth - 1, datasets, reducedcorrespondences);

			for (LogicalOlapOp logicalOlapOp1 : nextdepthlqps) {
				for (LogicalOlapOp logicalOlapOp2 : nextdepthlqps) {
					newlqps.add(new ConvertCubeOp(logicalOlapOp1,
							logicalOlapOp2, selectedCorrespondence));
				}
			}

			// Recursively other depths of the other.
			for (int i = 0; i < depth - 1; i++) {
				List<LogicalOlapOp> lowerdepthlqps = generateMergeLqpsForDepth(
						i, datasets, reducedcorrespondences);
				for (LogicalOlapOp logicalOlapOp1 : lowerdepthlqps) {
					for (LogicalOlapOp logicalOlapOp2 : nextdepthlqps) {

						// Left / Right
						newlqps.add(new ConvertCubeOp(logicalOlapOp1,
								logicalOlapOp2, selectedCorrespondence));
						// Vice-versa
						newlqps.add(new ConvertCubeOp(logicalOlapOp2,
								logicalOlapOp1, selectedCorrespondence));
					}
				}
			}

		}

		return newlqps;
	}

	/**
	 * The ordering of ccs is relevant but every cc can only be used once in a
	 * query plan.
	 * 
	 * @param depth
	 * @param d
	 * @param cc
	 * @return
	 */
	public static int numberForConvertChildren(int depth, int d, int cc) {
		if (depth == 0) {
			return d;
		}

		return cc * numberForConvertChildren(depth - 1, d, cc - 1);
	}

	/**
	 * We are interested in whether we can estimate the upper bound of derived
	 * datasets.
	 * 
	 */
	public void testExample_QB_Datasets_ConvertCube_QueryTestnumberForMergeChildren() {

		System.out.println(Example_QB_Datasets_ConvertCube_QueryTest
				.numberForMergeChildren(0, 3, 3));
		System.out.println(Example_QB_Datasets_ConvertCube_QueryTest
				.numberForMergeChildren(1, 3, 3));
		System.out.println(Example_QB_Datasets_ConvertCube_QueryTest
				.numberForMergeChildren(2, 3, 3));
		System.out.println(Example_QB_Datasets_ConvertCube_QueryTest
				.numberForMergeChildren(3, 3, 3));
		System.out.println(Example_QB_Datasets_ConvertCube_QueryTest
				.numberForMergeChildren(4, 3, 3));

		int no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForMergeChildren(0, 3, 3);
		assertEquals(3, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(
				1, 3, 3);
		assertEquals(81, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(
				2, 3, 3);
		assertEquals(13608, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(
				3, 3, 3);
		assertEquals(3003480, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(
				4, 3, 3);
		assertEquals(0, no);

		// Before
		// int no =
		// Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(0,
		// 3, 2);
		// assertEquals(3, no);
		//
		// no =
		// Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(1,
		// 3, 2);
		// assertEquals(18, no);
		//
		// no =
		// Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(2,
		// 3, 2);
		// assertEquals(864, no);
		//
		// no =
		// Example_QB_Datasets_ConvertCube_QueryTest.numberForMergeChildren(3,
		// 3, 2);
		// assertEquals(1565568, no);
	}

	public void testExample_QB_Datasets_ConvertCube_QueryTestgenerateMergeLqpsForDepth() {

		List<ReconciliationCorrespondence> correspondences = EmbeddedSesameEngine
				.getReconciliationCorrespondences(true);
		String[] datasets = {
				"http://estatwrap.ontologycentral.com/id/tec00115#ds",
				"http://lod.gesis.org/lodpilot/ALLBUS/ZA4570v590.rdf#ds",
				"http://estatwrap.ontologycentral.com/id/nama_aux_gph#ds" };

		List<LogicalOlapOp> lqps = Example_QB_Datasets_ConvertCube_QueryTest
				.generateMergeLqpsForDepth(0, datasets, correspondences);
		assertEquals(3, lqps.size());

		lqps = Example_QB_Datasets_ConvertCube_QueryTest
				.generateMergeLqpsForDepth(1, datasets, correspondences);
		assertEquals(18, lqps.size());

		lqps = Example_QB_Datasets_ConvertCube_QueryTest
				.generateMergeLqpsForDepth(2, datasets, correspondences);
		assertEquals(270, lqps.size());

		lqps = Example_QB_Datasets_ConvertCube_QueryTest
				.generateMergeLqpsForDepth(3, datasets, correspondences);
		assertEquals(0, lqps.size());

		// Before
		// List<LogicalOlapOp> lqps =
		// Example_QB_Datasets_ConvertCube_QueryTest.generateMergeLqpsForDepth(0,
		// datasets, correspondences);
		// assertEquals(3, lqps.size());
		//
		// lqps =
		// Example_QB_Datasets_ConvertCube_QueryTest.generateMergeLqpsForDepth(1,
		// datasets, correspondences);
		// assertEquals(18, lqps.size());
		//
		// lqps =
		// Example_QB_Datasets_ConvertCube_QueryTest.generateMergeLqpsForDepth(2,
		// datasets, correspondences);
		// assertEquals(864, lqps.size());
		//
		// lqps =
		// Example_QB_Datasets_ConvertCube_QueryTest.generateMergeLqpsForDepth(3,
		// datasets, correspondences);
		// assertEquals(1565568, lqps.size());
	}

	public void testExample_QB_Datasets_ConvertCube_QueryTestnumberForConvertChildren() {

		// Case for one
		int no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(0, 3, 1);
		assertEquals(3, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(1, 3, 1);
		assertEquals(3, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(2, 3, 1);
		assertEquals(0, no);

		// Case for two
		no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(0, 3, 2);
		assertEquals(3, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(1, 3, 2);
		assertEquals(6, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(2, 3, 2);
		assertEquals(6, no);

		no = Example_QB_Datasets_ConvertCube_QueryTest
				.numberForConvertChildren(3, 3, 2);
		assertEquals(0, no);

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
