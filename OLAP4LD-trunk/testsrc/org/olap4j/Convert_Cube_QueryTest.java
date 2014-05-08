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
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.driver.olap4ld.linkeddata.BaseCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.ReconciliationCorrespondence;
import org.olap4j.driver.olap4ld.linkeddata.ConvertCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LinkedDataCubesEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.PhysicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.Restrictions;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

/**
 * Tests on executing drill-across.
 * 
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class Convert_Cube_QueryTest extends TestCase {

	private LinkedDataCubesEngine lde;

	public Convert_Cube_QueryTest() throws SQLException {

		Olap4ldUtil.prepareLogging();
		// Logging
		// For debugging purposes
		// Olap4ldUtil._log.setLevel(Level.INFO);

		// For monitoring usage
		Olap4ldUtil._log.setLevel(Level.CONFIG);

		// For warnings (and errors) only
		// Olap4ldUtil._log.setLevel(Level.WARNING);

		Olap4ldUtil._isDebug = false;

		try {
			// Must have settings without influence on query processing
			URL serverUrlObject = new URL("http://example.de");
			List<String> datastructuredefinitions = new ArrayList<String>();
			List<String> datasets = new ArrayList<String>();
			lde = new EmbeddedSesameEngine(serverUrlObject,
					datastructuredefinitions, datasets, "EMBEDDEDSESAME");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// For now, we strictly separate Convert-Cube and Drill-Across with
		// equivalence classes.
		EmbeddedSesameEngine.equivs = new ArrayList<Node[]>();
	}

	protected void setUp() throws SQLException {

	}

	protected void tearDown() throws Exception {

	}

	public void test_GDP_Mioeur2eur() throws OlapException {

		String domainUri = "http://141.52.218.13:8080/QB-Slicer/rest/mioeur2eur?dsUri";

		// First: GDP and main components
		Node gdpdsuri = new Resource(
				"http://estatwrap.ontologycentral.com/id/nama_gdp_c#ds");
		Restrictions gdprestrictions = new Restrictions();
		gdprestrictions.cubeNamePattern = gdpdsuri;

		// Base-cube
		// XXX: We need to make sure that only the lowest members are queried
		// from each cube.

		// In order to fill the engine with data
		List<Node[]> gdpcube = lde.getCubes(gdprestrictions);
		assertEquals(2, gdpcube.size());
		Map<String, Integer> gdpcubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(gdpcube.get(0));
		System.out.println("CUBE_NAME: "
				+ gdpcube.get(1)[gdpcubemap.get("?CUBE_NAME")]);

		List<Node[]> gdpcubemeasures = lde.getMeasures(gdprestrictions);

		List<Node[]> gdpcubedimensions = lde.getDimensions(gdprestrictions);
		assertEquals(true, gdpcubedimensions.size() > 1);

		List<Node[]> gdpcubehierarchies = lde.getHierarchies(gdprestrictions);

		List<Node[]> gdpcubelevels = lde.getLevels(gdprestrictions);

		List<Node[]> gdpcubemembers = lde.getMembers(gdprestrictions);

		BaseCubeOp gdpbasecube = new BaseCubeOp(gdpcube, gdpcubemeasures,
				gdpcubedimensions, gdpcubehierarchies, gdpcubelevels,
				gdpcubemembers);

		// Projection
		// Since we do not remove any measure from the cubes, we do not need
		// projection.
		// XXX: We will see how the query processor handles it if no projection
		// is called.

		// Dice
		// For now, we do not do any dice since we slice over most dimensions
		// XXX: However, the results may be wrong, if we do not ensure the
		// constraint
		// that base cube only queries for those members that are on the lowest
		// level.

		// Slice
		// XXX: Since in MDX, we probably cannot distinguish between dimensions
		// for specific cubes
		// {indic_na, sex, age, esa95}

		// Roll-up
		// XXX: We do not need roll-up

		List<Node[]> mio_eur2eur_inputmembers = new ArrayList<Node[]>();
		mio_eur2eur_inputmembers
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#unit"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR") });

		// mio_eur2eur_inputmembers.add(new Node[] {new
		// Resource("http://purl.org/linked-data/sdmx/2009/measure#obsValue"),
		// new Variable("value1")});

		List<Node[]> mio_eur2eur_outputmembers = new ArrayList<Node[]>();
		mio_eur2eur_outputmembers.add(new Node[] {
				new Resource(
						"http://ontologycentral.com/2009/01/eurostat/ns#unit"),
				new Resource(
						"http://estatwrap.ontologycentral.com/dic/unit#EUR") });
		// mio_eur2eur_outputmembers.add(new Node[] {new Variable("outputcube"),
		// new
		// Resource("http://purl.org/linked-data/sdmx/2009/measure#obsValue"),
		// new Variable("value2")});

		String mio_eur2eur_function = "(1000000 * x)";

		ReconciliationCorrespondence mio_eur2eur_correspondence = new ReconciliationCorrespondence(
				"mio_eur2eur", mio_eur2eur_inputmembers, null,
				mio_eur2eur_outputmembers, mio_eur2eur_function);

		// Convert-context
		LogicalOlapOp convertgdp = new ConvertCubeOp(gdpbasecube,
				mio_eur2eur_correspondence, domainUri);

		LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(convertgdp);

		String result = executeStatement(myplan);

		assertContains(
				"http://estatwrap.ontologycentral.com/dic/geo#UK; http://estatwrap.ontologycentral.com/dic/indic_na#P7; http://estatwrap.ontologycentral.com/dic/unit#EUR; 2010; 5.59686E11; ",
				result);

	}

	/**
	 * First try of a pipeline. Using Estatwrap data. Computes GDP per capita.
	 * 
	 * @throws OlapException
	 */
	public void test_GDP_per_Capita_Calculation() throws OlapException {

		String domainUri = "http://141.52.218.13:8080/QB-Slicer/rest/mioeur2eur?dsUri=";

		// First: GDP and main components - Current prices dataset
		Node gdpdsuri = new Resource(
				"http://estatwrap.ontologycentral.com/id/nama_gdp_c#ds");
		Restrictions gdprestrictions = new Restrictions();
		gdprestrictions.cubeNamePattern = gdpdsuri;

		// Base-cube
		// XXX: We need to make sure that only the lowest members are queried
		// from each cube.

		// In order to fill the engine with data
		List<Node[]> gdpcube = lde.getCubes(gdprestrictions);
		assertEquals(2, gdpcube.size());
		Map<String, Integer> gdpcubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(gdpcube.get(0));
		System.out.println("CUBE_NAME: "
				+ gdpcube.get(1)[gdpcubemap.get("?CUBE_NAME")]);

		List<Node[]> gdpcubemeasures = lde.getMeasures(gdprestrictions);

		List<Node[]> gdpcubedimensions = lde.getDimensions(gdprestrictions);
		assertEquals(true, gdpcubedimensions.size() > 1);

		List<Node[]> gdpcubehierarchies = lde.getHierarchies(gdprestrictions);

		List<Node[]> gdpcubelevels = lde.getLevels(gdprestrictions);

		List<Node[]> gdpcubemembers = lde.getMembers(gdprestrictions);

		// Base-cube
		BaseCubeOp gdpbasecube = new BaseCubeOp(gdpcube, gdpcubemeasures,
				gdpcubedimensions, gdpcubehierarchies, gdpcubelevels,
				gdpcubemembers);

		// Projection
		// Since we do not remove any measure from the cubes, we do not need
		// projection.
		// XXX: We will see how the query processor handles it if no projection
		// is called.

		// Dice
		// For now, we do not do any dice since we slice over most dimensions
		// XXX: However, the results may be wrong, if we do not ensure the
		// constraint
		// that base cube only queries for those members that are on the lowest
		// level.

		// Slice
		// XXX: Since in MDX, we probably cannot distinguish between dimensions
		// for specific cubes
		// {indic_na, sex, age, esa95}

		// Roll-up
		// XXX: We do not need roll-up

		List<Node[]> mio_eur2eur_inputmembers = new ArrayList<Node[]>();
		mio_eur2eur_inputmembers
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#unit"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR") });

		// mio_eur2eur_inputmembers.add(new Node[] {new
		// Resource("http://purl.org/linked-data/sdmx/2009/measure#obsValue"),
		// new Variable("value1")});

		List<Node[]> mio_eur2eur_outputmembers = new ArrayList<Node[]>();
		mio_eur2eur_outputmembers.add(new Node[] {
				new Resource(
						"http://ontologycentral.com/2009/01/eurostat/ns#unit"),
				new Resource(
						"http://estatwrap.ontologycentral.com/dic/unit#EUR") });
		// mio_eur2eur_outputmembers.add(new Node[] {new Variable("outputcube"),
		// new
		// Resource("http://purl.org/linked-data/sdmx/2009/measure#obsValue"),
		// new Variable("value2")});

		String mio_eur2eur_function = "(1000000 * x)";

		ReconciliationCorrespondence mio_eur2eur_correspondence = new ReconciliationCorrespondence(
				"mio_eur2eur", mio_eur2eur_inputmembers, null,
				mio_eur2eur_outputmembers, mio_eur2eur_function);

		// Mioeur2eur(dataset): Converting MIO_EUR to EUR in GDP dataset
		LogicalOlapOp mio_eur2eur_op = new ConvertCubeOp(gdpbasecube,
				mio_eur2eur_correspondence, domainUri);

		// Dice mioeur2eur for B1G.
		// No dice necessary, directly possible with convert-context
		// List<Node[]> dicehierarchysignatureB1G = null;
		// List<List<Node[]>> dicemembercombinationsB1G = null;
		// LogicalOlapOp b1g = new DiceOp(mioeur2eur, dicehierarchysignatureB1G,
		// dicemembercombinationsB1G);

		// Slice indic_na.
		// No slice necessary, directly possible with convert-context
		// List<Node[]> sliceB1G = null;
		// LogicalOlapOp slicedb1g = new SliceOp(b1g, sliceB1G);

		// Dice mioeur2eur for D21_M_D31.
		// No dice necessary, directly possible with convert-context
		// List<Node[]> dicehierarchysignatureD21_M_D31 = null;
		// List<List<Node[]>> dicemembercombinationsD21_M_D31 = null;
		// LogicalOlapOp d21_m_d31 = new DiceOp(mioeur2eur,
		// dicehierarchysignatureD21_M_D31,
		// dicemembercombinationsD21_M_D31);

		// Slice indic_na.
		// List<Node[]> sliceD21_M_D31 = null;
		// LogicalOlapOp slicedd21_m_d31 = new SliceOp(d21_m_d31,
		// sliceD21_M_D31);

		// Drill-across B1G and D21...
		// LogicalOlapOp drillacross = new DrillAcrossOp(slicedb1g,
		// slicedd21_m_d31);

		// Computing Nominal GDP from single parts in new EUR dataset.
		// XXX: ComplexMeasureOp
		// LogicalOlapOp computegdp = new ConvertContextOp(drillacross, 1,
		// domainUri);

		List<Node[]> computegdp_inputmembers1 = new ArrayList<Node[]>();
		computegdp_inputmembers1
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/indic_na#B1G") });

		List<Node[]> computegdp_inputmembers2 = new ArrayList<Node[]>();
		computegdp_inputmembers2
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/indic_na#D21_M_D31") });

		List<Node[]> computegdp_outputmembers = new ArrayList<Node[]>();
		computegdp_outputmembers
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/indic_na#NGDP") });

		String computegdp_function = "(x1 + x2)";

		ReconciliationCorrespondence computegdp_correspondence = new ReconciliationCorrespondence(
				"computegdp", computegdp_inputmembers1,
				computegdp_inputmembers2, computegdp_outputmembers,
				computegdp_function);

		// String computegdp = "{" +
		// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> <http://estatwrap.ontologycentral.com/dic/indic_na#B1G> .\n"
		// +
		// "?obs1 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?value1 .\n"
		// +
		// "?obs2 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> <http://estatwrap.ontologycentral.com/dic/indic_na#D21_M_D31> .\n"
		// +
		// "?obs2 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?value2 .\n"
		// +
		// "?newvalue <http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas> \"(?value1 + ?value2)\" ."
		// +
		// "} => {" +
		// "_:newobs <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> <http://estatwrap.ontologycentral.com/dic/indic_na#NGDP> .\n"
		// +
		// "_:newobs <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?newvalue "
		// +
		// "} . ";

		// Computing Nominal GDP from single parts in new EUR dataset.
		LogicalOlapOp computegdp_op = new ConvertCubeOp(mio_eur2eur_op,
				mio_eur2eur_op, computegdp_correspondence, domainUri);

		/*
		 * Should contain: http://estatwrap.ontologycentral.com/dic/geo#UK;
		 * http://estatwrap.ontologycentral.com/dic/indic_na#NGDP;
		 * http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR; 2010;
		 * 1731809.0;
		 */

		// XXX Would I need to add to DSD: eurostat:indic_na dic_indic_na:NGDP;
		// ?

		// Second: Population dataset
		Node populationuri = new Resource(
				"http://estatwrap.ontologycentral.com/id/demo_pjan#ds");
		Restrictions populationrestrictions = new Restrictions();
		populationrestrictions.cubeNamePattern = populationuri;

		// Base-cube
		// XXX: We need to make sure that only the lowest members are queried
		// from each cube.

		// In order to fill the engine with data
		// XXX: Should be part of base-cube operator
		List<Node[]> populationcube = lde.getCubes(populationrestrictions);
		assertEquals(2, populationcube.size());
		Map<String, Integer> populationcubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(populationcube.get(0));
		System.out.println("CUBE_NAME: "
				+ populationcube.get(1)[populationcubemap.get("?CUBE_NAME")]);

		List<Node[]> populationcubemeasures = lde
				.getMeasures(populationrestrictions);

		List<Node[]> populationcubedimensions = lde
				.getDimensions(populationrestrictions);
		assertEquals(true, populationcubedimensions.size() > 1);

		List<Node[]> populationcubehierarchies = lde
				.getHierarchies(populationrestrictions);

		List<Node[]> populationcubelevels = lde
				.getLevels(populationrestrictions);

		List<Node[]> populationcubemembers = lde
				.getMembers(populationrestrictions);

		BaseCubeOp populationbasecube = new BaseCubeOp(populationcube,
				populationcubemeasures, populationcubedimensions,
				populationcubehierarchies, populationcubelevels,
				populationcubemembers);

		// XXX Would I need to add: Add indicator and unit to population dataset

		// Compute "slice" of population that does not use sex and age
		// dimensions
		// List<Node[]> slicesexage = null;
		// LogicalOlapOp slicedsexage = new SliceOp(populationbasecube,
		// slicesexage);

		// Drill-across gdp and population dataset
		// LogicalOlapOp drillacrossgdppopulation = new DrillAcrossOp(
		// slicedsexage, computegdp);

		List<Node[]> computegdppercapita_inputmembers1 = new ArrayList<Node[]>();
		computegdppercapita_inputmembers1
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/indic_na#NGDP") });
		computegdppercapita_inputmembers1.add(new Node[] {
				new Resource(
						"http://ontologycentral.com/2009/01/eurostat/ns#unit"),
				new Resource(
						"http://estatwrap.ontologycentral.com/dic/unit#EUR") });

		List<Node[]> computegdppercapita_inputmembers2 = new ArrayList<Node[]>();
		computegdppercapita_inputmembers2
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#sex"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/sex#T") });
		computegdppercapita_inputmembers2
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#age"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/age#TOTAL") });

		List<Node[]> computegdppercapita_outputmembers = new ArrayList<Node[]>();
		computegdppercapita_outputmembers
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/indic_na#NGDPH") });
		computegdppercapita_outputmembers
				.add(new Node[] {
						new Resource(
								"http://ontologycentral.com/2009/01/eurostat/ns#unit"),
						new Resource(
								"http://estatwrap.ontologycentral.com/dic/unit#EUR_HAB") });

		String computegdppercapita_function = "(x1 / x2)";

		ReconciliationCorrespondence computegdppercapita_correspondence = new ReconciliationCorrespondence(
				"computegdppercapita", computegdppercapita_inputmembers1,
				computegdppercapita_inputmembers2,
				computegdppercapita_outputmembers, computegdppercapita_function);

		// Compute GDP per Capita from GDP and Population
		// XXX: ComplexMeasureOp
		LogicalOlapOp computegdppercapita_op = new ConvertCubeOp(computegdp_op,
				populationbasecube, computegdppercapita_correspondence,
				domainUri);

		LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(
				computegdppercapita_op);

		String result = executeStatement(myplan);

		// XXX: Compare with:
		// http://estatwrap.ontologycentral.com/id/nama_aux_gph#ds

		// According to GDP per capita - annual Data [nama_aux_gph] correct.
		assertContains(
				"http://estatwrap.ontologycentral.com/dic/geo#UK; http://estatwrap.ontologycentral.com/dic/indic_na#NGDPH; http://estatwrap.ontologycentral.com/dic/unit#EUR_HAB; 2010; 27704.423967820803;",
				result);

	}

	/**
	 * Tries to get worldbank data from Sarven.
	 * 
	 * @throws OlapException
	 */
	public void test_GDP_per_Capita_Worldbank() throws OlapException {

		String domainUri = "http://141.52.218.13:8080/QB-Slicer/rest/mioeur2eur?dsUri=";

		// First: GDP current dataset
		Node gdpdsuri = new Resource(
				"http://worldbank.270a.info/dataset/NY.GDP.MKTP.CD");
		Restrictions gdprestrictions = new Restrictions();
		gdprestrictions.cubeNamePattern = gdpdsuri;

		// Base-cube
		// XXX: We need to make sure that only the lowest members are queried
		// from each cube.

		// In order to fill the engine with data
		List<Node[]> gdpcube = lde.getCubes(gdprestrictions);
		assertEquals(2, gdpcube.size());
		Map<String, Integer> gdpcubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(gdpcube.get(0));
		System.out.println("CUBE_NAME: "
				+ gdpcube.get(1)[gdpcubemap.get("?CUBE_NAME")]);

		List<Node[]> gdpcubemeasures = lde.getMeasures(gdprestrictions);

		List<Node[]> gdpcubedimensions = lde.getDimensions(gdprestrictions);
		assertEquals(true, gdpcubedimensions.size() > 1);

		List<Node[]> gdpcubehierarchies = lde.getHierarchies(gdprestrictions);

		List<Node[]> gdpcubelevels = lde.getLevels(gdprestrictions);

		List<Node[]> gdpcubemembers = lde.getMembers(gdprestrictions);

		// Base-cube
		BaseCubeOp gdpbasecube = new BaseCubeOp(gdpcube, gdpcubemeasures,
				gdpcubedimensions, gdpcubehierarchies, gdpcubelevels,
				gdpcubemembers);

		LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(gdpbasecube);

		executeStatement(myplan);
	}

	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
	}

	private String executeStatement(LogicalOlapQueryPlan queryplan) {
		String resultString = "";
		try {
			// For now, we simply return plan
			System.out.println("--------------");
			System.out.println("Logical plan:" + queryplan.toString());
			// Execute query return representation of physical query plan
			List<Node[]> result = this.lde.executeOlapQuery(queryplan);

			PhysicalOlapQueryPlan execplan = this.lde.getExecplan();
			System.out.println("Physical plan:" + execplan.toString());
			System.out.println("Result:");

			for (Node[] nodes : result) {
				for (Node node : nodes) {
					resultString += node.toString() + "; ";
					System.out.print(node.toString() + "; ");
				}
				resultString += "\n";
				System.out.println();
			}
			System.out.println("--------------");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
