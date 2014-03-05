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
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LinkedDataCubesEngine;
import org.olap4j.driver.olap4ld.linkeddata.Restrictions;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.semanticweb.yars.nx.Node;

/**
 * Unit tests for metadata queries directly on an LDCE engine, e.g. 
 * on "EmbeddedSesameEngine_GDP_per_Capita", an engine that collects
 * data for Open Government Data. 
 *  
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class Metadata_QueryTest extends TestCase {

	private LinkedDataCubesEngine lde;

	public Metadata_QueryTest() throws SQLException {

		Olap4ldUtil.prepareLogging();
		// Logging
		// For debugging purposes
		Olap4ldUtil._log.setLevel(Level.CONFIG);

		// For monitoring usage
		// Olap4ldUtil._log.setLevel(Level.INFO);

		// For warnings (and errors) only
		// Olap4ldUtil._log.setLevel(Level.WARNING);

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

	public void test_Metadata_Queries() throws OlapException {
		// GDP per capita dataset
//		String gdpdsuri = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tec00114_ds.rdf#ds";
//		Restrictions gdprestrictions = new Restrictions();
//		gdprestrictions.cubeNamePattern = gdpdsuri;
		
		// No specific dataset, if we use GDP-per-Capita LDCE
		Restrictions gdprestrictions = new Restrictions();

		// In order to fill the engine with data
		List<Node[]> gdpcubes = lde.getCubes(gdprestrictions);
		assertEquals(2, gdpcubes.size());
		Map<String, Integer> gdpcubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(gdpcubes.get(0));
		System.out.println("CUBE_NAME: "
				+ gdpcubes.get(1)[gdpcubemap.get("?CUBE_NAME")]);

		for (Node[] nodes : gdpcubes) {
			for (Node node : nodes) {
				System.out.print(node.toString());
				System.out.print("|");
			}
			System.out.println();
		}

		List<Node[]> gdpcubemeasures = lde.getMeasures(gdprestrictions);

		for (Node[] nodes : gdpcubemeasures) {
			for (Node node : nodes) {
				System.out.print(node.toString());
				System.out.print("|");
			}
			System.out.println();
		}

		List<Node[]> gdpcubedimensions = lde.getDimensions(gdprestrictions);

		for (Node[] nodes : gdpcubedimensions) {
			for (Node node : nodes) {
				System.out.print(node.toString());
				System.out.print("|");
			}
			System.out.println();
		}

		List<Node[]> gdpcubehierarchies = lde.getHierarchies(gdprestrictions);

		for (Node[] nodes : gdpcubehierarchies) {
			for (Node node : nodes) {
				System.out.print(node.toString());
				System.out.print("|");
			}
			System.out.println();
		}

		List<Node[]> gdpcubelevels = lde.getLevels(gdprestrictions);

		for (Node[] nodes : gdpcubelevels) {
			for (Node node : nodes) {
				System.out.print(node.toString());
				System.out.print("|");
			}
			System.out.println();
		}

		List<Node[]> gdpcubemembers = lde.getMembers(gdprestrictions);

		for (Node[] nodes : gdpcubemembers) {
			for (Node node : nodes) {
				System.out.print(node.toString());
				System.out.print("|");
			}
			System.out.println();
		}
	}

	@SuppressWarnings("unused")
	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
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
