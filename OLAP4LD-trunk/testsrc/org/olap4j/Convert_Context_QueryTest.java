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
import org.olap4j.driver.olap4ld.linkeddata.ConvertContextOp;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LinkedDataCubesEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.PhysicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.Restrictions;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.semanticweb.yars.nx.Node;

/**
 * Tests on executing drill-across.
 * 
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class Convert_Context_QueryTest extends TestCase {

	private LinkedDataCubesEngine lde;

	public Convert_Context_QueryTest() throws SQLException {

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

	public void test_GDP_Mioeur2eur()
			throws OlapException {

		// First: GDP per capita dataset
		String gdpdsuri = "http://estatwrap.ontologycentral.com/id/nama_gdp_c#ds";
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

		// Convert-context		LogicalOlapOp convertgdp = new ConvertContextOp(gdpbasecube, 1);
		
		LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(convertgdp);

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

			PhysicalOlapQueryPlan execplan = this.lde.getExecplan(queryplan);
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
