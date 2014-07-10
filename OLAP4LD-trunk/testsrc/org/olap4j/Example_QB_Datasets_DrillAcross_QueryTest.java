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
public class Example_QB_Datasets_DrillAcross_QueryTest extends TestCase {
	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private OlapConnection olapConnection;
	private OlapStatement stmt;

	public Example_QB_Datasets_DrillAcross_QueryTest() throws SQLException {	
		
			Olap4ldUtil.prepareLogging();
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
	public void executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  |  00      |      461.33 |         1.1 |",
				result);
	}
	
	/**
	 * Same as above but here we are querying additional datasets. The query does not serve more information, though.
	 * 
	 * One can see here that the order the datasets are given in the FROM clause determines the order measures
	 * are returned from the Drill-Across, since at Drill-Across, a mapping to the respective measure
	 * is not done, yet.
	 * 
	 * What I will be investigating is the difference in time between the one above and this one. 
	 * I have to take into account any possible dataset, probably.
	 */
	public void executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_moreDatasets() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  |  00      |                                                                    461.33 |                                                                    1.1 |",
				result);
	}
	
	/**
	 * Same as above but here we are querying one unspecific measure. The query will provide more information since it will take
	 * into account the additional datasets since the query (and the datasets) are not specific enough.
	 * 
	 */
	public void executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_moreDatasets_UnspecificMeasure() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  |  00      |      461.33 |         1.1 |",
				result);
	}
	
	// General queries
	
	/**
	 * Mixture of specific and unspecific measures (although not really). 
	 */
	public void testDrillAcrossEstatwrapGDPpercapitainPPS_EurostatEmploymentRate() {
		
		// Problem:
		// Make sure: , = XXX2C
		String result = executeStatement("SELECT /* $session: olap4ld_example_datasets_testExampleEstatwrapGDPpercapitainPPSOlapEsaAggregateBothMeasures */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValue],[httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23employment_rate]} ON COLUMNS,CrossJoin({Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])},{Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])}) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]");

		assertContains(
				"|  |        |  | 2011 |       148.0 |         1.0 |       148.0 |            70.43 |",
				result);
	}

	/**
	 * Query towards ISEM paper.
	 * 
	 * (date, {obsValue AVG, gesis SUM, estatwrap SUM})
	 * 
	 * Correctly returns:
	 * 
	 * 1) The shared measure obsValueAVG of the first (Estatwrap) dataset. 
	 * 
	 * 2) The sum of answers of the GESIS dataset. (automatically for Germany)
	 * 
	 * 3) The sum of Employment rate of
	 * Estatwrap dataset. (for all)
	 * 
	 */
	public void testDrillAcrossTowardsUnemploymentFearAndEmploymentRate() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCSUM], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23dsXXX2ChttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds]");

		// obsValue, gesis:sum, estwrap:sum (no dice, 2008)
		assertContains("|  | 2008 |       95.28 |      461.33 |      4097.0 |",
				result);
	}
	
	/**
	 * 
	 * (date, geo, {obsValue AVG, gesis SUM, estatwrap SUM})
	 * 
	 * Integration, but without dice. XXX: I should clean up the example queries one day.
	 */
	public void testDrillAcrossTowardsUnemploymentFearAndEmploymentRateExampleDatasetsGeoDimension() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCSUM], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds]");

		// obsValue, gesis:sum, estatwrap:sum (no dice, Germany)
		assertContains("|  |  00    |      117.62 |      485.03 |       941.0 |",
				result);
	}

	/**
	 * 
	 * More dice query
	 * 
	 * Should be correct, since I ask for GESIS average for Germany only, which
	 * is the same as for all.
	 * 
	 * Also, I ask for only "Nos" (
	 * httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvariableYYYrdfXXX23v590_1
	 * ), which should be less than 1384 (SUM)
	 * 
	 * It is correct that AVG and SUM are the same, since behind it there is per
	 * year only one value.
	 */
	public void testDiceGermanyNoUnemploymentFear() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds] WHERE CrossJoin({[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]},{[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvariableYYYrdfXXX23v590_1]})");

		assertContains("|  | 2008 |      1144.0 |      1144.0 |", result);
	}

	/**
	 * See one above
	 */
	public void testDiceGermanyEmploymentrate() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		assertContains("|  | 2008 |       116.0 |       116.0 |",	result);
	}


	/**
	 * Here, I explicitly want to integrate with OLAP4LD example estatwrap
	 * dataset (no quota).
	 * 
	 * Should be the same as for dices above but integrated.
	 * 
	 * This shall be the query closest to the one for ISEM.
	 * 
	 * Correctly shows:
	 * 
	 * 1) The average of Estatwrap obsValue
	 * 2) The sum of answers of the GESIS dataset.
	 * 
	 */
	public void testDrillAcrossUnemploymentFearAndEmploymentRateGermany() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCSUM], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23dsXXX2ChttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  | 2008 |       116.0 |      461.33 |       116.0 |",
				result);
	}
	
	/**
	 * Query closest to ISEM paper.
	 */
	public void testDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsAGGFUNCAVG]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  | 2008 |         1.1 |      461.33 |",
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
