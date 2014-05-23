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
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

/**
 * Unit test for LD-Cubes Explorer Queries on XMLA. As questions, we take the
 * one's from the small student survey.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class LDCX_DrillAcross_Evaluation_XmlaTest extends TestCase {

	private URL xmlauri;

	public LDCX_DrillAcross_Evaluation_XmlaTest() throws SQLException {

		try {
			
			// Prepare logging has to be done
			Olap4ldUtil.prepareLogging();

			// For this test, we need to run a server.
//			 this.xmlauri = new URL(
//			 "http://localhost:8080/xmlaserver-trunk/xmla");
			 //this.xmlauri = new URL("http://aifbkos.aifb.uni-karlsruhe.de:8080/xmlaserver-trunk/xmla");
			 this.xmlauri = new URL("http://km.aifb.kit.edu/services/xmlaserver-trunk/xmla");
//			 this.xmlauri = new URL(
			// "http://141.52.218.137:8000/xmlaserver-trunk/xmla");
//			this.xmlauri = new URL(
//					"http://www.ldcx.linked-data-cubes.org:8000/xmlaserver-trunk/xmla");
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
	 * 

	% First four experiments:
	% Employment rate: http://estatwrap.ontologycentral.com/id/t2020_10 (1992-2013) - replaced by
	% Energy dependence: http://estatwrap.ontologycentral.com/id/tsdcc310 (2001-2012)
	% GDP on RnD: http://estatwrap.ontologycentral.com/id/t2020_20 (from 1990 to 2012) - replaced by
	% Energy productivity: http://estatwrap.ontologycentral.com/id/t2020_rd310 (2000-2012)
	% Energy intensity: http://estatwrap.ontologycentral.com/id/tsdec360 (2001-2012)
	% Greenhouse gas emissions, base\ldots: http://estatwrap.ontologycentral.com/id/t2020_30 (1990-2011) - replaced by
	% Greenhouse gas emissions per capita: http://estatwrap.ontologycentral.com/id/t2020_rd300 (2000-2011)
	% Next four experiments:
	%  
	% Share of renewable energy: http://estatwrap.ontologycentral.com/id/t2020_31 (2004-2012)
	% People at risk of poverty or social exclusion: http://estatwrap.ontologycentral.com/id/t2020_50 (2004-2012)
	People living in households with very low work intensity: http://estatwrap.ontologycentral.com/id/t2020_51 (2004 - 2012)
	People at risk of poverty after social transfers: http://estatwrap.ontologycentral.com/id/t2020_52 (2003-2012)
	Severely materially deprived people: http://estatwrap.ontologycentral.com/id/t2020_53 (2003-2012)
	 * 
	 * 
	 */
	public void executeDrillAcrossEu2020indicators4() {

		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testEU2020-4 */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsAGGFUNCAVG], [httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValuehttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23dsAGGFUNCAVG]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdcc310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd310XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ft2020_rd300XXX23dsXXX2ChttpXXX3AXXX2FXXX2FestatwrapYYYontologycentralYYYcomXXX2FidXXX2Ftsdec360XXX23ds]";
		// |  | 2008 |      260.64 |       10.54 |       41.85 |        5.25 |
		String seek = "260.64";
		String response = postAndReturnXmlaOlapQuery(mdx);
		assertContains(seek, response);
	}
	
	

	private String uriToMdx(String dsUri) {
		String name;
		try {
			name = URLEncoder.encode(dsUri, "UTF-8");
			name = name.replace("%", "XXX");
			name = name.replace(".", "YYY");
			name = name.replace("-", "ZZZ");
			return "[" + name + "]";
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

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <Command> <Statement>"
				+ mdx
				+ "</Statement> </Command> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> <Format>Multidimensional</Format> <Content>SchemaData</Content> </PropertyList> </Properties> </Execute> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

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
