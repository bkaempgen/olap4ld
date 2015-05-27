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
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
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
		
	}

	protected void setUp() throws SQLException {
		
		Olap4ldUtil.prepareLogging();
		
		Olap4ldUtil._isDebug = false;
		
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

	// From Performance evaluation EU 2020 indicators
	
	/**
	 * 

	% First four experiments:
	% Employment rate: http://estatwrap.ontologycentral.com/id/t2020_10 (1992-2013) - replaced by
	% Energy dependence: http://estatwrap.ontologycentral.com/id/tsdcc310 (2001-2012)
	% GDP on RnD: http://estatwrap.ontologycentral.com/id/t2020_20 (from 1990 to 2012) - replaced by
	% Energy productivity: http://estatwrap.ontologycentral.com/id/t2020_rd310 (2000-2012)
	% Energy intensity: http://estatwrap.ontologycentral.com/id/tsdec360 (2001-2012)
	% Greenhouse gas emissions, base\ldots: http://estatwrap.ontologycentral.com/id/t2020_30 (1990-2011) - replaced by
	% Greenhouse gas emissions per capita: http://estatwrap.ontologycentral.com/id/t2020_rd300 (2000-2011)
	% Next four experiments:
	%  
	% Share of renewable energy: http://estatwrap.ontologycentral.com/id/t2020_31 (2004-2012)
	% People at risk of poverty or social exclusion: http://estatwrap.ontologycentral.com/id/t2020_50 (2004-2012)
	People living in households with very low work intensity: http://estatwrap.ontologycentral.com/id/t2020_51 (2004 - 2012)
	People at risk of poverty after social transfers: http://estatwrap.ontologycentral.com/id/t2020_52 (2003-2012)
	Severely materially deprived people: http://estatwrap.ontologycentral.com/id/t2020_53 (2003-2012)
	 * 
	 * 
	 */
	public void testExecuteDrillAcrossEu2020indicators4() {

		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testEU2020-4 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsAGGFUNCAVG]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds]");
		
		assertContains("|  | 2008 |      260.64 |       10.62 |        5.86 |       43.83 |",
				result);
	}
	
	public void testExecuteDrillAcrossEu2020indicators8() {

		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testEU2020-8 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_31XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_50XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_51XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_52XXX23dsAGGFUNCAVG]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_31XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_50XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_51XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_52XXX23ds]");
		
		assertContains("|  | 2006 |     2169.55 |     6676.42 |       13.45 |       39.39 |        5.06 |       10.92 |      277.37 |     4218.92 |",
				result);
	}
	
	// Variations of the UNEMPLOY query from performance evaluation
	
	
	public void testExecuteDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermanyMetadata() {
		String dsUri = "httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23ds";
		// localhost
		// dsUri =
		// "http://localhost:8080/ldcx-trunk/ldcx/tests/ssb001/ttl/example.ttl#ds";
		metadataTest(dsUri, 5, 5);
	}
	
	/**
	 * Specific measures from two datasets. 
	 */
	public void testExecuteDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23ds] ");

		// Should be correct: obsValue (?), gesis:avg (461.33), estatwrap:avg (1.1) Dice: Germany, 2008
		assertContains("|  |  00      |         1.1 | 461.3333333333333 |",
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
	public void testExecuteDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_moreDatasets() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:avg (461.33), estatwrap:avg (1.1) Dice: Germany, 2008
		assertContains("|  |      |  |  00      |      461.33 |         1.1 |",
				result);
	}
	
	/**
	 * Same as above but here we are querying one unspecific measure. The query will provide more information since it will take
	 * into account the additional datasets since the query (and the datasets) are not specific enough.
	 * 
	 * We get many more values for Germany, e.g., for 2008: 461.333333333333333333333333,1.1,140.3 (checked manually, correct)
	 * 
	 */
	public void testExecuteDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_moreDatasets_UnspecificMeasure() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  |  00      | 461.333333333333333333333333 / 1.1 / 140.3 |",
				result);
	}
	
	/**
	 * Same as above but here we are querying one unspecific + general measure. The query will provide more information since it will take
	 * into account the additional datasets since the query (and the datasets) are not specific enough.
	 * 
	 * We get many more values for Germany, e.g., for 2008: 461.333333333333333333333333,1.1,140.3 (checked manually, correct)
	 * 
	 * Here, we can either enable or disable merging correspondences.
	 * 
	 */
	public void testExecuteDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany_GeneralMeasure() {
		
		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValue]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  |  00      | 1144 160 80 / 1.1 / 140.3 |",
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
		assertContains("|  | 2008 |       95.07 |      461.33 |      4088.0 |",
				result);
	}
	
	/**
	 * 
	 * (date, geo, {obsValue AVG, gesis SUM, estatwrap SUM})
	 * 
	 * Integration, but without dice. 
	 * 
	 * Here, we see that links are missing for these example datasets:
	 * 
	 * <http://olap4ld.googlecode.com/dic/geo#DE> <http://www.w3.org/2002/07/owl#sameAs> <http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#00>.
	 * 
	 */
	public void testDrillAcrossTowardsUnemploymentFearAndEmploymentRateExampleDatasetsGeoDimension() {

		/*
		 * Tests:
		 * 
		 * Is there integration done?
		 * 
		 * Specific measures: [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCSUM], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23dsAGGFUNCSUM]
		 * Implicit unspecific: httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG <= no integration it seems
		 * Concrete unspecific: httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValue <= works, it seems.
		 * 
		 */
		
		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCSUM], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds]");

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
	 * See one above.
	 * 
	 * Since we use gesis:Germany, we need to make sure that it is included as a dataset, also.
	 */
	public void testDiceGermanyEmploymentrate() {

		String result = executeStatement("SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23dsAGGFUNCSUM]} ON COLUMNS, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00114XXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		assertContains("|  | 2008 |      461.33 |       116.0 |",	result);
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
