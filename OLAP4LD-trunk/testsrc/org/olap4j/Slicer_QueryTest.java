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
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.driver.olap4ld.helper.Restrictions;
import org.olap4j.driver.olap4ld.linkeddata.BaseCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.DiceOp;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LinkedDataEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.ProjectionOp;
import org.olap4j.driver.olap4ld.linkeddata.RollupOp;
import org.olap4j.driver.olap4ld.linkeddata.SliceOp;
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
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.util.Array;

/**
 * Tests on building a slicer. Input: Arbitrary dataset. Output: For each
 * 1-/2-dimensional slice a
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class Slicer_QueryTest extends TestCase {

	private LinkedDataEngine lde;

	public Slicer_QueryTest() throws SQLException {

		try {
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

	public void testSsb001ExampleMetadata() {
		String dsUri = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.ttl#ds";
		List<LogicalOlapQueryPlan> queryplans = getOneDimSlices(dsUri);
		for (LogicalOlapQueryPlan logicalOlapQueryPlan : queryplans) {
			executeStatement(logicalOlapQueryPlan);
		}
	}

	// public void testEurostatEmploymentRateExampleSlices() {
	// String dsUri =
	// "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tsdec420_ds.rdf#ds";
	//
	// List<LogicalOlapQueryPlan> queryplans = getOneDimSlices(dsUri);
	// for (LogicalOlapQueryPlan logicalOlapQueryPlan : queryplans) {
	// executeStatement(logicalOlapQueryPlan);
	// }
	// }

	private List<LogicalOlapQueryPlan> getOneDimSlices(String dsUri) {
		List<LogicalOlapQueryPlan> output = new ArrayList<LogicalOlapQueryPlan>();
		Restrictions restrictions = new Restrictions();
		restrictions.cubeNamePattern = dsUri;
		try {
			// In order to fill the engine with data
			List<Node[]> cubes = lde.getCubes(restrictions);
			assertEquals(2, cubes.size());

			List<Node[]> measures = lde.getMeasures(restrictions);
			assertEquals(true, measures.size() > 1);
			List<Node[]> dimensions = lde.getDimensions(restrictions);
			assertEquals(true, dimensions.size() > 1);
			// 1-dim
			// First is header
			boolean first = true;
			Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
					.getNodeResultFields(dimensions.get(0));
			for (Node[] dim2rollup : dimensions) {
				if (first) {
					first = false;
					continue;
				}
				// fix all other dim
				List<List<Node[]>> membercombinations = new ArrayList<List<Node[]>>();
				List<Node[]> fixeddims = new ArrayList<Node[]>();
				first = true;
				for (Node[] dim2fix : dimensions) {
					if (first) {
						first = false;
						fixeddims.add(dimensions.get(0));
					} else {
						if (!dim2fix[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
								.toString().equals(
										dim2rollup[dimensionmap
												.get("?DIMENSION_UNIQUE_NAME")]
												.toString())) {
							restrictions = new Restrictions();
							restrictions.cubeNamePattern = dsUri;
							restrictions.dimensionUniqueName = dim2fix[dimensionmap
									.get("?DIMENSION_UNIQUE_NAME")].toString();
							List<Node[]> members = lde.getMembers(restrictions);
							assertEquals(true, members.size() > 1);
							// After, every List<Node> in membercombinations is
							// one possible query
							membercombinations = cartesian_product(
									membercombinations, members);
							fixeddims.add(dim2fix);
						}
					}
				}
				// Query 1-dim
				// membercombinations does not have a header
				assertEquals(true, membercombinations.size() > 0);
				assertEquals(true, membercombinations.get(0).size() > 0);
				Map<String, Integer> membermap = Olap4ldLinkedDataUtil
						.getNodeResultFields(membercombinations.get(0).get(0));
				List<Node[]> hierarchysignature = new ArrayList<Node[]>();
				// First is header
				first = true;
				for (Node[] member : membercombinations.get(0)) {
					if (first) {
						first = false;
						continue;
					}
					restrictions = new Restrictions();
					restrictions.cubeNamePattern = dsUri;
					restrictions.dimensionUniqueName = member[membermap
					              							.get("?DIMENSION_UNIQUE_NAME")].toString();
					restrictions.hierarchyUniqueName = member[membermap
							.get("?HIERARCHY_UNIQUE_NAME")].toString();
					List<Node[]> hierarchies = lde.getHierarchies(restrictions);
					assertEquals(2, hierarchies.size());
					// At start, add header
					if (hierarchysignature.isEmpty()) {
						hierarchysignature.add(hierarchies.get(0));
					}
					hierarchysignature.add(hierarchies.get(1));
				}

				// One member combination contains a header and a list of
				// members (= one combination)
				// each combination will have the same signature
				for (List<Node[]> membercombination : membercombinations) {
					LogicalOlapOp basecube = new BaseCubeOp(cubes);
					LogicalOlapOp projection = new ProjectionOp(basecube,
							measures);
					List<List<Node[]>> aMembercombinations = new ArrayList<List<Node[]>>();
					aMembercombinations.add(membercombination);
					LogicalOlapOp dice = new DiceOp(projection,
							hierarchysignature, aMembercombinations);
					LogicalOlapOp slice = new SliceOp(dice,
							new ArrayList<Node[]>());

					// Watch out, should only be one
					restrictions = new Restrictions();
					restrictions.dimensionUniqueName = dim2rollup[dimensionmap
							.get("?DIMENSION_UNIQUE_NAME")].toString();
					List<Node[]> rollupssignature = lde
							.getHierarchies(restrictions);
					// Header is included
					assertEquals(2, rollupssignature.size());
					List<Node[]> rollups = lde.getLevels(restrictions);
					assertEquals(2, rollups.size());
					LogicalOlapOp rollup = new RollupOp(slice,
							rollupssignature, rollups);
					LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(
							rollup);
					output.add(myplan);
				}
			}

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return output;
	}

	/**
	 * Adds members to membercombinations, i.e., create new membercombinations
	 * by combining each single membercombinations with each single members.
	 * 
	 * @param membercombinations
	 * @param members
	 * @return
	 */
	private List<List<Node[]>> cartesian_product(
			List<List<Node[]>> membercombinations, List<Node[]> members) {
		List<List<Node[]>> newmembercombinations = new ArrayList<List<Node[]>>();

		// Three possibilities: 1) membercombinations empty 2) members empty 3)
		// none empty
		if (membercombinations.isEmpty()) {
			// First is header
			for (int i = 1; i<members.size(); i++) {
				Node[] member = members.get(i);
				List<Node[]> newmembercombination = new ArrayList<Node[]>();
				// Add header
				newmembercombination.add(members.get(0));
				newmembercombination.add(member);
				newmembercombinations.add(newmembercombination);
			}
			// Exclude header
			assertEquals(members.size()-1, newmembercombinations.size());
		} else if (members.isEmpty()) {
			newmembercombinations = membercombinations;
			assertEquals(membercombinations.size(),
					newmembercombinations.size());
		} else {
			// First is no header
			for (List<Node[]> membercombination : membercombinations) {
				// First is header
				boolean first = true;
				for (Node[] member : members) {
					if (first) {
						first = false;
					} else {
						List<Node[]> newmembercombination = new ArrayList<Node[]>();
						// First
						for (Node[] combinedmember : membercombination) {
							newmembercombination.add(combinedmember);
						}
						newmembercombination.add(member);
						newmembercombinations.add(newmembercombination);
					}
				}
			}
			// Assert that newmembercombinations contain
			// |membercombinations|*(|members|-1) elements
			assertEquals(membercombinations.size() * (members.size() - 1),
					newmembercombinations.size());
		}
		return newmembercombinations;
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
			System.out.println(queryplan.toString());
			//this.lde.executeOlapQuery(queryplan);
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
