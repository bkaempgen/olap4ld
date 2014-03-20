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
import org.olap4j.driver.olap4ld.linkeddata.DrillAcrossOp;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LinkedDataCubesEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.PhysicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.Restrictions;
import org.olap4j.driver.olap4ld.linkeddata.SliceOp;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.semanticweb.yars.nx.Node;

/**
 * Tests on executing drill-across.
 * 
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class Drill_across_QueryTest extends TestCase {

	private LinkedDataCubesEngine lde;

	public Drill_across_QueryTest() throws SQLException {

		Olap4ldUtil.prepareLogging();
		// Logging
		// For debugging purposes
		// Olap4ldUtil._log.setLevel(Level.CONFIG);

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
	}

	protected void setUp() throws SQLException {

	}

	protected void tearDown() throws Exception {

	}

	public void test_Example_GDP_per_capita_in_PPS_vs_Employment_rate_by_sex()
			throws OlapException {

		// We query one integrated dataset from GDP per capita dataset and
		// Employment rate, by sex dataset.
		// However, the MDX query on that integrated dataset is then transformed
		// into a
		// logical query plan that explicitly does drill-across over single
		// datasets.

		// First: GDP per capita dataset
		String gdpdsuri = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tec00114_ds.rdf#ds";
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

		List<Node[]> gdpsliceddimensions = new ArrayList<Node[]>();

		System.out.println("DIMENSION_UNIQUE_NAMES:");
		Map<String, Integer> gdpdimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(gdpcubedimensions.get(0));
		// Header
		gdpsliceddimensions.add(gdpcubedimensions.get(0));
		for (Node[] nodes : gdpcubedimensions) {
			String dimensionname = nodes[gdpdimensionmap
					.get("?DIMENSION_UNIQUE_NAME")].toString();
			System.out.println(dimensionname);
			if (dimensionname.equals("http://ontologycentral.com/2009/01/eurostat/ns#aggreg95") || dimensionname.equals("http://ontologycentral.com/2009/01/eurostat/ns#indic_na")) {
				gdpsliceddimensions.add(nodes);
			}
		}

		// XXX: SliceOp has to make sure that it does not do anything, if:
		// sliceddimension is empty, contains only one (header), or is null.
		LogicalOlapOp gdpslice = new SliceOp(gdpbasecube, gdpsliceddimensions);

		// Roll-up
		// XXX: We do not need roll-up

		// Second: Employment rate, by sex dataset
		String emplratedsuri = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tsdec420_ds.rdf#ds";
		Restrictions emplraterestrictions = new Restrictions();
		emplraterestrictions.cubeNamePattern = emplratedsuri;

		// Base-cube
		// XXX: We need to make sure that only the lowest members are queried
		// from each cube.

		// In order to fill the engine with data
		List<Node[]> emplratecube = lde.getCubes(emplraterestrictions);
		assertEquals(2, emplratecube.size());
		Map<String, Integer> emplratecubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(emplratecube.get(0));
		System.out.println("CUBE_NAME: "
				+ emplratecube.get(1)[emplratecubemap.get("?CUBE_NAME")]);

		List<Node[]> emplratecubemeasures = lde
				.getMeasures(emplraterestrictions);

		List<Node[]> emplratecubedimensions = lde
				.getDimensions(emplraterestrictions);
		assertEquals(true, emplratecubedimensions.size() > 1);

		List<Node[]> emplratecubehierarchies = lde
				.getHierarchies(emplraterestrictions);

		List<Node[]> emplratecubelevels = lde.getLevels(emplraterestrictions);

		List<Node[]> emplratecubemembers = lde.getMembers(emplraterestrictions);

		BaseCubeOp emplratebasecube = new BaseCubeOp(emplratecube,
				emplratecubemeasures, emplratecubedimensions,
				emplratecubehierarchies, emplratecubelevels,
				emplratecubemembers);

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

		List<Node[]> emplratesliceddimensions = new ArrayList<Node[]>();

		System.out.println("DIMENSION_UNIQUE_NAMES:");
		Map<String, Integer> emplratedimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(emplratecubedimensions.get(0));
		// Header
		emplratesliceddimensions.add(emplratecubedimensions.get(0));
		for (Node[] nodes : emplratecubedimensions) {
			String dimensionname = nodes[emplratedimensionmap
					.get("?DIMENSION_UNIQUE_NAME")].toString();
			System.out.println(dimensionname);
			if (dimensionname.equals("http://ontologycentral.com/2009/01/eurostat/ns#sex")) {
				emplratesliceddimensions.add(nodes);
			}
		}

		// XXX: SliceOp has to make sure that it does not do anything, if:
		// sliceddimension is empty, contains only one (header), or is null.
		LogicalOlapOp emplrateslice = new SliceOp(emplratebasecube,
				emplratesliceddimensions);

		// Roll-up
		// XXX: We do not need roll-up

		// Drill-across
		DrillAcrossOp drillacross = new DrillAcrossOp(gdpslice, emplrateslice);

		LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(drillacross);

		executeStatement(myplan);
	}



	@SuppressWarnings("unused")
	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
	}

	private void executeStatement(LogicalOlapQueryPlan queryplan) {
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
					System.out.print(node.toString() + "; ");
				}
				System.out.println();
			}
			System.out.println("--------------");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
