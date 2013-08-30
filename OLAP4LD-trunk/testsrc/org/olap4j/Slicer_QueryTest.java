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
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.driver.olap4ld.helper.Restrictions;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LinkedDataEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapQueryPlan;
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

/**
 * Tests on building a slicer. Input: Arbitrary dataset. Output: For each 1-/2-dimensional slice
 * a 
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class Slicer_QueryTest extends TestCase {
	
	private LinkedDataEngine myLinkedData;

	public Slicer_QueryTest() throws SQLException {
		
		myLinkedData = new EmbeddedSesameEngine(null,
				null, null, "EMBEDDEDSESAME");
	}

	protected void setUp() throws SQLException {
		
	}

	protected void tearDown() throws Exception {

	}
	
	private LogicalOlapQueryPlan getOneDimSlices(String dsUri) {
		Restrictions restrictions = new Restrictions(null);
		restrictions.cubeNamePattern = dsUri;
		try {
			List<Node[]> cubes = myLinkedData.getCubes(restrictions);
			
			if (cubes.size() <= 1) {
				throw new UnsupportedOperationException();
			}
			
			Node[] cube = cubes.get(1);
			
			Cube cubeObject = new 
			
			
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	


	public void testEurostatEmploymentRateExampleSlices() {
		String name = "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tsdec420_ds.rdf#ds";
		
		LogicalOlapQueryPlan queryplan = getOneDimSlices(name);
		executeStatement(queryplan);
	}

	private void assertContains(String seek, String s) {
		if (s.indexOf(seek) < 0) {
			fail("expected to find '" + seek + "' in '" + s + "'");
		}
	}

	private void executeStatement(LogicalOlapQueryPlan queryplan) {
		try {
			this.myLinkedData.executeOlapQuery(queryplan);
		} catch (OlapException e) {
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
