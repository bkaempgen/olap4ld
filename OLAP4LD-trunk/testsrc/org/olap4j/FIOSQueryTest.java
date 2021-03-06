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

	// public void testFIOS_Full_Member() {
	// String result =
	// executeStatement("SELECT {Hierarchize({[dctermsXXX3Adate].[dctermsXXX3Adate].[dctermsXXX3Adate].[2009ZZZ08ZZZ31]})} ON COLUMNS, {[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FcikXXX2F1013237XXX23id]} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FarchiveXXX2F1013237XXX2F0001193125ZZZ11ZZZ089990XXX23dsd]");
	//
	// assertContains("1", result);
	// }
	//
	// public void testFIOS_Only_Member() {
	// String result =
	// executeStatement("SELECT {Hierarchize({[2009ZZZ08ZZZ31]})} ON COLUMNS, {[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FcikXXX2F1013237XXX23id]} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FarchiveXXX2F1013237XXX2F0001193125ZZZ11ZZZ089990XXX23dsd]");
	//
	// assertContains("1", result);
	// }

	// public void testFIOS_With_Where() {
	// String result =
	// executeStatement("SELECT NON EMPTY {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FcikXXX2F1013237XXX23id]})} ON COLUMNS, NON EMPTY {Hierarchize({[sdmxZZZmeasureXXX3AobsValue]})} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FarchiveXXX2F1013237XXX2F0001193125ZZZ11ZZZ089990XXX23dsd] WHERE {Hierarchize({[2009ZZZ08ZZZ31]})}");
	//
	// assertContains("1", result);
	// }

	// public void testFIOS_DrillAcrossYahoo() {
	// String result =
	// executeStatement("SELECT {Hierarchize({[dctermsXXX3Adate].[dctermsXXX3Adate].[dctermsXXX3Adate].[2013ZZZ02ZZZ19]})} ON COLUMNS, {[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FcikXXX2F37996XXX23id]} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23dsd]");
	//
	// assertContains("19", result);
	// }
	//
	// public void testFIOS_DrillAcrossSEC() {
	// String result =
	// executeStatement("SELECT {Hierarchize({[dctermsXXX3Adate].[dctermsXXX3Adate].[dctermsXXX3Adate].[2009ZZZ06ZZZ30]})} ON COLUMNS, {[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FvocabXXX2FedgarXXX23issuer].[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FcikXXX2F37996XXX23id]} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23dsd]");
	//
	// assertContains("58", result);
	// }

	// public void testFIOS_SicLevelSEC() {
	// String result =
	// executeStatement("SELECT NON EMPTY {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FedgXXX2FsicXXX2F3711XXX23id]})} ON COLUMNS, NON EMPTY {Hierarchize({[sdmxZZZmeasureXXX3AobsValue]})} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23dsd]");
	//
	// assertContains("8899095016.43744934535308", result);
	// }

	// public void testFIOS_Members() {
	// String result =
	// executeStatement("SELECT NON EMPTY {Hierarchize({[sdmxZZZmeasureXXX3AobsValue]})} ON COLUMNS, NON EMPTY {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23issuerCodeListCompanyLevel].Members})} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FarchiveXXX2FHPQXXX2F2007ZZZ06ZZZ27XXX23dsd]");
	//
	// assertContains("8899095016.43744934535308", result);
	// }

	/**
	 * "Frage für NVIDIA den Durchschnitt jedes Subjects ab."
	 */
	// public void testFIOS_Query_Subjects_of_Dell() {
	// String result =
	// executeStatement("SELECT NON EMPTY {Hierarchize({[sdmxZZZmeasureXXX3AobsValue]})} ON COLUMNS, NON EMPTY {[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FvocabXXX23subject].Members} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23secyhofdsd] WHERE {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FcikXXX2F1045810XXX23id]})}");
	//
	// assertContains("| Open                                                                                                                                                                |           27.58 |",
	// result);
	// assertContains("| Gross profit                                                                                                                                                        |      4.761715E8 |",
	// result);
	// }

	/**
	 */
//	public void testFIOS_Query_Open_AssetsCurrent_Over_time() {
//		String result = executeStatement("SELECT NON EMPTY {Hierarchize({[dctermsXXX3Adate].Members})} ON COLUMNS, NON EMPTY {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FvocabXXX2FstockXXX23Open], [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FvocabXXX2FusZZZgaapZZZ2011ZZZ01ZZZ31XXX23AssetsCurrent]})} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23secyhofdsd] WHERE {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FcikXXX2F1045810XXX23id]})}");
//
//		assertContains(
//				"| Assets current  |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |  3.22695E9 |            |            |            |            |            |            |            |            |            |            |            |            |            |            | 3.500071E9 |            | 3.905358E9 |            |            |            | 4.032116E9 |            |            |            | 4.213851E9 |            |            |            | 4.424827E9 |            |            |            |            |            |",
//				result);
//	}

	/**
	 */
	public void testFIOS_Query_Open_DELL_HewlettPackard_Over_time() {
		String result = executeStatement("SELECT NON EMPTY {Hierarchize({[dctermsXXX3Adate].Members})} ON COLUMNS, NON EMPTY {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FcikXXX2F47217XXX23id],[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FcikXXX2F1045810XXX23id]})} ON ROWS FROM [httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FfiosXXX23secyhofdsd] WHERE {Hierarchize({[httpXXX3AXXX2FXXX2FpublicYYYbZZZkaempgenYYYdeXXX3A8080XXX2FvocabXXX2FstockXXX23Open]})}");

		assertContains(
				"| Assets current  |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |            |  3.22695E9 |            |            |            |            |            |            |            |            |            |            |            |            |            |            | 3.500071E9 |            | 3.905358E9 |            |            |            | 4.032116E9 |            |            |            | 4.213851E9 |            |            |            | 4.424827E9 |            |            |            |            |            |",
				result);
	}

	// public void testFIOS_2() {
	// String result =
	// executeStatement("SELECT {[httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23:issuer].[httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23:issuer].[httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23:issuer].[httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FcikXXX2F1141391XXX23:id]} ON COLUMNS, {[dcterms:date].[dcterms:date].[dcterms:date].[2008-09-30]} ON ROWS FROM [httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FarchiveXXX2F1141391XXX2F0001193125-09-222058XXX23:dsd]");
	//
	// assertContains("1", result);
	// }
	//
	// public void testFIOS_3() {
	//
	// String result =
	// executeStatement("SELECT NON EMPTY {Hierarchize({[httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FcikXXX2F1141391XXX23:id]})} ON COLUMNS, NON EMPTY {Hierarchize({[sdmx-measure:obsValue]})} ON ROWS FROM [httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FarchiveXXX2F1141391XXX2F0001193125-09-222058XXX23:dsd]");
	//
	// assertContains("1", result);
	//
	// }

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
