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
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.olap4j.test.TestContext;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

/**
 * Performance evaluation for Drill-Across paper. 
 * 
 * GDP Growth vs. Employment Fear as in ISEM paper.
 * 
 * 
 * @version 0.1
 * @author bkaempgen
 */
public class LDCX_Performance_Evaluation_DrillAcrossTest extends TestCase {
	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private OlapConnection olapConnection;
	private OlapStatement stmt;

	public LDCX_Performance_Evaluation_DrillAcrossTest() throws SQLException {	
				
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
	
	public void executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany() {

		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testGdpEmployment */ {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsAGGFUNCAVG]} ON COLUMNS, CrossJoin(Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate]),Members([httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FvocabYYYrdfXXX23geo])) ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftec00115XXX23dsXXX2ChttpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FZA4570v590YYYrdfXXX23ds] WHERE {[httpXXX3AXXX2FXXX2FlodYYYgesisYYYorgXXX2FlodpilotXXX2FALLBUSXXX2FgeoYYYrdfXXX2300]}");

		// Should be correct: obsValue (?), gesis:sum (461.33), estatwrap:sum (116) Dice: Germany, 2008
		assertContains("|  |      |  | Germany (until 1990 former territory of the FRG)                                   |      461.33 |         1.1 |",
				result);
	}
	
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
	public void executeDrillAcrossEu2020indicators4() {

		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testEU2020-4 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsAGGFUNCAVG]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds]");
		
		assertContains("|  | 2008 |      260.64 |       10.54 |       41.85 |        5.25 |",
				result);
	}
	
	public void executeDrillAcrossEu2020indicators8() {

		String result = executeStatement("SELECT /* $session: ldcx_performance_evaluation_testEU2020-8 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_31XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_50XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_51XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_52XXX23dsAGGFUNCAVG]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_31XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_50XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_51XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_52XXX23ds]");
		
		assertContains("|  | 2006 |     2169.55 |     6676.42 |       13.45 |       39.39 |        5.06 |       10.92 |      277.37 |     4218.92 |",
				result);
	}
	
	public void test1() {
		
		// for now, we simply assume equivalence statements given
		EmbeddedSesameEngine.equivs.add(new Node[] {
				new Resource(
						"http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#geo"),
				new Resource(
						"http://ontologycentral.com/2009/01/eurostat/ns#geo") });

		// Hierarchy gesis-geo:list = estatwrap:geo
		EmbeddedSesameEngine.equivs.add(new Node[] {
				new Resource(
						"http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#list"),
				new Resource(
						"http://ontologycentral.com/2009/01/eurostat/ns#geo") });

		// Could also for the olap
		EmbeddedSesameEngine.equivs.add(new Node[] {
		new Resource("http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#00"),
		new Resource("http://estatwrap.ontologycentral.com/dic/geo#DE") });

		EmbeddedSesameEngine.equivs.add(new Node[] {
				new Resource("http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#00"),
				new Resource("http://olap4ld.googlecode.com/dic/geo#DE") });
		
		executeDrillAcrossUnemploymentFearAndRealGDPGrowthRateGermany();
		
	}
	
	public void test2() {
		
		executeDrillAcrossEu2020indicators4();
		
	}
	
	public void test3() {
		
		executeDrillAcrossEu2020indicators8();
		
	}
	
	public void test4() {
		
		executeDrillAcrossEu2020indicators8();
		
	}
	
	public void test5() {
		
		executeDrillAcrossEu2020indicators8();
		
	}
	
	public void test6() {
		
		executeDrillAcrossEu2020indicators8();
		
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
