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
import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.test.TestContext;

/**
 * Unit test for olap4j metadata methods.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class FIOSQueryTest extends TestCase {
	private final TestContext testContext = TestContext.instance();
	private final TestContext.Tester tester = testContext.getTester();
	private Connection connection;
	private OlapConnection olapConnection;
	private OlapStatement stmt;
	private MdxParserFactory parserFactory;
	private MdxParser parser;

	public FIOSQueryTest() throws SQLException {
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

		this.parserFactory = olapconnection.getParserFactory();
		this.parser = parserFactory.createMdxParser(olapconnection);
	}

	protected void tearDown() throws Exception {
		if (connection != null && !connection.isClosed()) {
			connection.close();
			connection = null;
		}
	}
	
	public void testFIOS_1() {
		String result = executeStatement("SELECT {[aHR0cDovL3lhaG9vZmluYW5jZXdyYXAuYXBwc3BvdC5jb20vdm9jYWIveWFob28j:issuer].[aHR0cDovL3lhaG9vZmluYW5jZXdyYXAuYXBwc3BvdC5jb20vdm9jYWIveWFob28j:issuer].[aHR0cDovL3lhaG9vZmluYW5jZXdyYXAuYXBwc3BvdC5jb20vdm9jYWIveWFob28j:issuer].[aHR0cDovL3lhaG9vZmluYW5jZXdyYXAuYXBwc3BvdC5jb20vdGlja2VyL01BIw==:id]} ON COLUMNS, {[dcterms:date].[dcterms:date].[dcterms:date].[2006-06-01]} ON ROWS FROM [aHR0cDovL3lhaG9vZmluYW5jZXdyYXAuYXBwc3BvdC5jb20vYXJjaGl2ZS9NQS8yMDA2LTA2LTAxIw==:dsd]");
		
		assertContains("6", result);
	}

	public void testFIOS_2() {
		 String result =
		 executeStatement("SELECT {[aHR0cDovL2VkZ2Fyd3JhcC5vbnRvbG9neWNlbnRyYWwuY29tL3ZvY2FiL2VkZ2FyIw==:issuer].[aHR0cDovL2VkZ2Fyd3JhcC5vbnRvbG9neWNlbnRyYWwuY29tL3ZvY2FiL2VkZ2FyIw==:issuer].[aHR0cDovL2VkZ2Fyd3JhcC5vbnRvbG9neWNlbnRyYWwuY29tL3ZvY2FiL2VkZ2FyIw==:issuer].[aHR0cDovL2VkZ2Fyd3JhcC5vbnRvbG9neWNlbnRyYWwuY29tL2Npay8xMTQxMzkxIw==:id]} ON COLUMNS, {[dcterms:date].[dcterms:date].[dcterms:date].[2008-09-30]} ON ROWS FROM [aHR0cDovL2VkZ2Fyd3JhcC5vbnRvbG9neWNlbnRyYWwuY29tL2FyY2hpdmUvMTE0MTM5MS8wMDAxMTkzMTI1LTA5LTIyMjA1OCM=:dsd]");
		
		 assertContains("1", result);
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

			SelectNode select = parser.parseSelect(mdxString);
			MdxValidator validator = parserFactory
					.createMdxValidator(olapConnection);
			select = validator.validateSelect(select);
			cset = stmt.executeOlapQuery(select);

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
