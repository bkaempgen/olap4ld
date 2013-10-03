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
 * Unit test for LD-Cubes Explorer Queries on XMLA. As questions, we take the one's from the
 * small student survey.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class LD_Cubes_Explorer_XmlaTest extends TestCase {

	private URL xmlauri;

	public LD_Cubes_Explorer_XmlaTest() throws SQLException {

		try {
			
			this.xmlauri = new URL(
					"http://localhost:8080/xmlaserver-trunk/xmla");
//			this.xmlauri = new URL(
//					"http://141.52.218.137:8000/xmlaserver-trunk/xmla");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void setUp() throws SQLException {

	}

	protected void tearDown() throws Exception {

	}
	
	/**
	 * Name two measures that the "Example SSB dataset" contains. 
	 */
	public void test1() {
		String cubename = uriToMdx("http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.ttl#ds");
		String aMeasurename = "[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount]";
		String aHierarchyname = "[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList]";
		postAndTestXmlaDiscoverDatasources();
		postAndTestXmlaDbschemaCatalogs();
		postAndTestXmlaMdschemaCubes(cubename);
		postAndTestXmlaMdschemaMeasures(cubename, aMeasurename);
		postAndTestXmlaMdschemaHierarchies(cubename, aHierarchyname);
	}

	/**
	 * What overall revenue was made by "Customer 2" in the "Example SSB dataset"? 
	 */
	public void test2() {
		String mdx = "SELECT /* $session: b2edaabe-a806-4fec-eba5-ac74b4009f4b */ NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue]} ON COLUMNS , NON EMPTY {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String seek = "7116579.0";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	/**
	 * Has there been any revenue from Product "Part 2"?  
	 */
	public void test3() {
		String mdx = "SELECT /* $session: b2edaabe-a806-4fec-eba5-ac74b4009f4b */ NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue]} ON COLUMNS , NON EMPTY {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_partkeyCodeList])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String seek = "Part 2";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertEquals(
				false,
				response.contains(seek));
	}
	
	/**
	 *  From what Date is the revenue of Product "Part 1"? 
	 */
	public void test4() {
		String mdx = "SELECT /* $session: b2edaabe-a806-4fec-eba5-ac74b4009f4b */ NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue]} ON COLUMNS , NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_partkeyCodeList])}, {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_orderdateCodeList])}) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String seek = "19940101";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	/**
	 *  What was the average GDP per capita in PPS for Germany? 
	 */
	public void test5() {
		String mdx = "SELECT /* $session: b2edaabe-a806-4fec-eba5-ac74b4009f4b */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]} ON COLUMNS , NON EMPTY {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23ds]";
		String seek = "117.62";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	/**
	 *  What was the average GDP per capita in PPS for Germany in 2012? 
	 */
	public void test6() {
		String mdx = "SELECT /* $session: b2edaabe-a806-4fec-eba5-ac74b4009f4b */ NON EMPTY CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]}, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])}) ON COLUMNS , NON EMPTY {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23ds]";
		String seek = "122.0";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	/**
	 *  How many values does the dataset for GDP per capita in PPS have for 2012? 
	 */
	public void test7() {
		String mdx = "SELECT /* $session: 0e58c35f-e454-9d78-3dd3-4e495bf41fa6 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])}, {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsdYYYrdfXXX23cl_aggreg95])}) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftec00114_dsYYYrdfXXX23ds]";
		String seek = "43.0";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	/**
	 *  What was the average Stock Market "Adjusted Closing Price" for Bank of America Corporation on 12 December 2012?  
	 */
	public void test8() {
		String mdx = "SELECT /* $session: b2edaabe-a806-4fec-eba5-ac74b4009f4b */ NON EMPTY CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]}, {Members([httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FvocabXXX2FyahooXXX23subject])}) ON COLUMNS , NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FarchiveXXX2FBACXXX2F2012ZZZ12ZZZ12XXX23ds]";
		String seek = "10.59";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	/**
	 *  What is the problem with the HCO3 climate data at location AD0514? 
	 */
	public void test9() {
		String cubename = uriToMdx("http://smartdbwrap.appspot.com/id/locationdataset/AD0514/HCO3");
		postAndTestXmlaDiscoverDatasources();
		postAndTestXmlaDbschemaCatalogs();
		String response = postAndReturnXmlaMdschemaCubes(cubename);
		String seek = "Failed specification check: IC-12. No duplicate observations. ";
		assertContains(seek, response);
	}
	
	/**
	 *  What is the overall average "Available for sale securities noncurrent" in Balance Sheet for COSTCO WHOLESALE CORP, published on 2010-08-29? 
	 */
	public void test10() {
		String mdx = "SELECT /* $session: 0e58c35f-e454-9d78-3dd3-4e495bf41fa6 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23issuer])}, {Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])}) ON ROWS FROM [httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FarchiveXXX2F909832XXX2F0001193125ZZZ10ZZZ230379XXX23ds]";
		
		String response = postAndReturnXmlaOlapQuery(mdx);
		
		String seek = "3000000.0";
		assertContains(seek, response);
		
		seek = "Available for sale securities noncurrent";
		assertContains(seek, response);
	}
	
	/**
	 *  What were the "Available for sale securities gross realized losses" between 2009-08-31 and 2010-08-29 for COSTCO WHOLESALE CORP? 
	 */
	public void test11() {
		String mdx = "SELECT /* $session: 0e58c35f-e454-9d78-3dd3-4e495bf41fa6 */ NON EMPTY CrossJoin({[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG]}, CrossJoin({Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart])}, {Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtend])})) ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23issuer])}, {Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])}) ON ROWS FROM [httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FarchiveXXX2F909832XXX2F0001193125ZZZ10ZZZ230379XXX23ds]";
		
		String response = postAndReturnXmlaOlapQuery(mdx);
		String seek = "1000000.0";
		assertContains(seek, response);
		seek = "Available for sale securities gross realized losses";
		assertContains(seek, response);
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
	
	public String postAndReturnXmlaMdschemaCubes(String cubename) {
		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_CUBES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		return post(data);
	}

	public void postAndTestXmlaMdschemaCubes(String cubename) {

		String response = postAndReturnXmlaMdschemaCubes(cubename);

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
	
	public String postAndReturnXmlaOlapQuery(String mdx) {
		
		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <Command> <Statement>"+mdx+"</Statement> </Command> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> <Format>Multidimensional</Format> <Content>SchemaData</Content> </PropertyList> </Properties> </Execute> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";
		
		return post(data);
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
