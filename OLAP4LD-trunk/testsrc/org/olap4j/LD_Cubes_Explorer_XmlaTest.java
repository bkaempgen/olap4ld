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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.olap4j.CellSetFormatterTest.Format;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;

/**
 * Unit test for LD-Cubes Explorer Queries on XMLA.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class LD_Cubes_Explorer_XmlaTest extends TestCase {

	private URL xmlauri;

	public LD_Cubes_Explorer_XmlaTest() throws SQLException {

		try {

			// this.tumblrWrite = new URL(
			// "http://141.52.218.137:8080/xmlaserver-trunk/xmla");
			this.xmlauri = new URL(
					"http://localhost:8080/xmlaserver-trunk/xmla");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void setUp() throws SQLException {

	}

	protected void tearDown() throws Exception {

	}

	public void testSsb001() {
		String cubename = "[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String aMeasurename = "[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount]";
		String aHierarchyname = "[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList]";
		String mdx = "SELECT NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String result = "Test";
		postAndTestXmlaDiscoverDatasources();
		postAndTestXmlaDbschemaCatalogs();
		postAndTestXmlaMdschemaCubes(cubename);
		postAndTestXmlaMdschemaMeasures(cubename, aMeasurename);
		postAndTestXmlaMdschemaHierarchies(cubename, aHierarchyname);
		postAndTestXmlaOlapQuery(mdx, result);
	}
	
	public void testEurostat() {
		String cubename = uriToMdx("http://estatwrap.ontologycentral.com/id/tec00114");
		String aMeasurename = "";
		String aHierarchyname = "";
		postAndTestXmlaDiscoverDatasources();
		postAndTestXmlaDbschemaCatalogs();
		postAndTestXmlaMdschemaCubes(cubename);
		postAndTestXmlaMdschemaMeasures(cubename, aMeasurename);
		postAndTestXmlaMdschemaHierarchies(cubename, aHierarchyname);

	}
	
	public void testEdgar() {
		String cubename = uriToMdx("http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/edgarwrap/0001193125-10-230379.rdf#ds");
		String aMeasurename = "[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]";
		String aHierarchyname = "[httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart]";
		postAndTestXmlaDiscoverDatasources();
		postAndTestXmlaDbschemaCatalogs();
		postAndTestXmlaMdschemaCubes(cubename);
		postAndTestXmlaMdschemaMeasures(cubename, aMeasurename);
		postAndTestXmlaMdschemaHierarchies(cubename, aHierarchyname);
		// First edgar query (works)
		String mdx = "SELECT {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]} ON COLUMNS,{Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]";
		// Complex edgar query (works actually also, however results in really long xmla result that probably cannot be properly parsed)
		mdx = " SELECT NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])},{Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23segment])}) ON COLUMNS, NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]";
		String result = "hello";
		postAndTestXmlaOlapQuery(mdx, result);
		
	}
	
	private String uriToMdx(String dsUri) {
		String name;
		try {
			name = URLEncoder.encode(dsUri, "UTF-8");
			name = name.replace("%", "XXX");
			name = name.replace(".", "YYY");
			name = name.replace("-", "ZZZ");
			return "["+name+"]";
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			return null;
	}

	private void postAndTestXmlaDiscoverDatasources() {
		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>DISCOVER_DATASOURCES</RequestType> <Restrictions> </Restrictions> <Properties> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		String response = post(data);

		String seek = "<DataSourceName>[XMLA olap4ld]</DataSourceName>";

		assertContains(seek, response);
	}

	private void postAndTestXmlaDbschemaCatalogs() {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>DBSCHEMA_CATALOGS</RequestType> <Restrictions> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		String response = post(data);

		String seek = "<CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME>";

		assertContains(seek, response);

	}

	public void postAndTestXmlaMdschemaCubes(String cubename) {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_CUBES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		String response = post(data);

		String seek = "<CUBE_NAME>" + cubename + "</CUBE_NAME>";

		assertContains(seek, response);
	}

	public void postAndTestXmlaMdschemaMeasures(String cubename,
			String aMeasurename) {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_MEASURES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		String response = post(data);

		String seek = "<MEASURE_NAME>" + aMeasurename + "</MEASURE_NAME>";

		assertContains(seek, response);
	}

	public void postAndTestXmlaMdschemaHierarchies(String cubename,
			String aHierarchyname) {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_HIERARCHIES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		String response = post(data);

		String seek = "<HIERARCHY_NAME>" + aHierarchyname + "</HIERARCHY_NAME>";

		assertContains(seek, response);
	}
	
	public void postAndTestXmlaOlapQuery(String mdx, String result) {
		
		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <Command> <Statement>"+mdx+"</Statement> </Command> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> <Format>Multidimensional</Format> <Content>SchemaData</Content> </PropertyList> </Properties> </Execute> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";
		
		String response = post(data);

		String seek = result;

		assertContains(seek, response);
	}

	private String post(String data) {

		try {
			// HttpClient client = new HttpClient();
			// PostMethod method = new PostMethod(url);
			HttpURLConnection http = (HttpURLConnection) xmlauri
					.openConnection();

			http.setDoOutput(true);
			http.setRequestMethod("POST");
			http.setRequestProperty("Content-Type", "text/xml");

			DataOutputStream dout = new DataOutputStream(http.getOutputStream());
			// OutputStreamWriter out = new
			// OutputStreamWriter(http.getOutputStream());

			// Send data
			http.connect();
			dout.writeBytes(data);
			// out.write(data);
			dout.flush();
			System.out.println(http.getResponseCode());
			System.out.println(http.getResponseMessage());
			// Get the response
			BufferedReader rd = new BufferedReader(new InputStreamReader(
					http.getInputStream()));

			String response = "";
			String line;
			while ((line = rd.readLine()) != null) {
				response += line;
			}
			System.out.println(response);

			dout.close();
			rd.close();

			return response;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

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
