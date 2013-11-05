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
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.layout.TraditionalCellSetFormatter;

/**
 * Unit test for LD-Cubes Explorer Queries on XMLA. As questions, we take the
 * one's from the small student survey.
 * 
 * @version $Id: MetadataTest.java 482 2012-01-05 23:27:27Z jhyde $
 */
public class LDCX_Performance_Evaluation_XmlaTest extends TestCase {

	private URL xmlauri;

	public LDCX_Performance_Evaluation_XmlaTest() throws SQLException {

		try {
			
			Olap4ldUtil.prepareLogging();

			this.xmlauri = new URL(
					"http://localhost:8080/xmlaserver-trunk/xmla");
			// this.xmlauri = new URL(
			// "http://141.52.218.137:8000/xmlaserver-trunk/xmla");
//			 this.xmlauri = new URL(
//			 "http://www.ldcx.linked-data-cubes.org:8000/xmlaserver-trunk/xmla");
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
	 * Test for populating log for performance evaluation for bottleneck test
	 * with full-drill-down.
	 */
	public void testFullDrillDown() {

		int testsize = 5;

		for (int i = 0; i < testsize; i++) {
			testYahooFinance_Slice();
			testYahooFinance_DrillDown();

			testSsb_Slice();
			testSsb_DrillDown();

			testSmart_Slice();
			testSmart_DrillDown();

			testEurostatEmploymentRateBySex_Slice();
			testEurostatEmploymentRateBySex_DrillDown();

			testEdgarCostco_Slice();
			testEdgarCostco_DrillDown();
		}
	}
	
	/**
	 * http://yahoofinancewrap.appspot.com/archive/BAC/2012-12-12#ds
	 * 
	 * Dates
	 */
	public void testYahooFinance_Slice() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testYahooFinance_Slice */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])} ON ROWS FROM [httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FarchiveXXX2FBACXXX2F2012ZZZ12ZZZ12XXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	public void testYahooFinance_DrillDown() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testYahooFinance_DrillDown */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FvocabXXX2FyahooXXX23issuer])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FvocabXXX2FyahooXXX23segment])}, {Members([httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FvocabXXX2FyahooXXX23subject])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2FyahoofinancewrapYYYappspotYYYcomXXX2FarchiveXXX2FBACXXX2F2012ZZZ12ZZZ12XXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	/**
	 * http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/ssb001/ttl/example.
	 * ttl#ds
	 * 
	 * Customers
	 */
	public void testSsb_Slice() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testSsb_Slice */ NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_supplycost]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	public void testSsb_DrillDown() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testSsb_DrillDown */ NON EMPTY {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_supplycost]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_orderdateCodeList])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_partkeyCodeList])}, {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_suppkeyCodeList])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	/**
	 * http://smartdbwrap.appspot.com/id/locationdataset/AD0514/Q
	 * 
	 * Analysisobject
	 */
	public void testSmart_Slice() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testSmart_Slice */ NON EMPTY {[httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FobsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FobsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fanalysis_Object])} ON ROWS FROM [httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FidXXX2FlocationdatasetXXX2FAD0514XXX2FQ]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	public void testSmart_DrillDown() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testSmart_DrillDown */ NON EMPTY {[httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FobsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FobsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fanalysis_Object])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fday])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Flocation])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fmonth])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fstudy_Area])}, {Members([httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2Fyear])}))))) ON ROWS FROM [httpXXX3AXXX2FXXX2FsmartdbwrapYYYappspotYYYcomXXX2FidXXX2FlocationdatasetXXX2FAD0514XXX2FQ]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	/**
	 * http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/
	 * tsdec420_ds.rdf#ds
	 */
	public void testEurostatEmploymentRateBySex_Slice() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testEurostatEmploymentRateBySex_Slice */ NON EMPTY {[httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23employment_rate]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	public void testEurostatEmploymentRateBySex_DrillDown() {
		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testEurostatEmploymentRateBySex_DrillDown */ NON EMPTY {[httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23employment_rate]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FontologycentralYYYcomXXX2F2009XXX2F01XXX2FeurostatXXX2FnsXXX23geo])}, CrossJoin({Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsdYYYrdfXXX23cl_sex])}, {Members([httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FdcXXX2FtermsXXX2Fdate])})) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FestatwrapXXX2Ftsdec420_dsYYYrdfXXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		// System.out.println(response);
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}

	/**
	 * http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/edgarwrap/
	 * 0001193125-10-230379.rdf#ds
	 * 
	 * The full-drill-down query of edgar would not fit in main memory even if
	 * we give it 1000MB memory (-Xms64m -Xmx1000m). This may be different for
	 * VM but I will not try it. Instead, we do not use segment:
	 */
	public void testEdgarCostco_Slice() {

		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testEdgarCostco_Slice */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY {Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23issuer])} ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
	}
	
	public void testEdgarCostco_DrillDown() {

		String mdx = "SELECT /* $session: ldcx_performance_evaluation_testEdgarCostco_DrillDown */ NON EMPTY {[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCAVG],[httpXXX3AXXX2FXXX2FpurlYYYorgXXX2FlinkedZZZdataXXX2FsdmxXXX2F2009XXX2FmeasureXXX23obsValueAGGFUNCCOUNT]} ON COLUMNS, NON EMPTY CrossJoin({Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23issuer])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FedgarwrapYYYontologycentralYYYcomXXX2FvocabXXX2FedgarXXX23subject])}, CrossJoin({Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtend])}, {Members([httpXXX3AXXX2FXXX2FwwwYYYw3YYYorgXXX2F2002XXX2F12XXX2FcalXXX2FicalXXX23dtstart])}))) ON ROWS FROM [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2FedgarwrapXXX2F0001193125ZZZ10ZZZ230379YYYrdfXXX23ds]";
		String response = postAndReturnXmlaOlapQuery(mdx);
		// for now, we do not include a quality test
		String seek = "SOAP-ENV:Fault";
		assertEquals(false, response.contains(seek));
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

	private String postAndReturnXmlaDiscoverDatasources() {
		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>DISCOVER_DATASOURCES</RequestType> <Restrictions> </Restrictions> <Properties> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		return post(data);
	}

	private String postAndReturnXmlaDbschemaCatalogs() {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>DBSCHEMA_CATALOGS</RequestType> <Restrictions> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		return post(data);

	}

	public String postAndReturnXmlaMdschemaCubes(String cubename) {
		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_CUBES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		return post(data);
	}

	public String postAndReturnXmlaMdschemaMeasures(String cubename) {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_MEASURES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		return post(data);

	}

	public String postAndReturnXmlaMdschemaHierarchies(String cubename) {

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <RequestType>MDSCHEMA_HIERARCHIES</RequestType> <Restrictions> <RestrictionList> <CATALOG_NAME>[LdCatalogSchema]</CATALOG_NAME> <CUBE_NAME>"
				+ cubename
				+ "</CUBE_NAME> </RestrictionList> </Restrictions> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> </PropertyList> </Properties> </Discover> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		return post(data);
	}

	public String postAndReturnXmlaOlapQuery(String mdx) {

		Olap4ldUtil._log.info("Run performance evaluation query: ");

		long time = System.currentTimeMillis();

		String data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <SOAP-ENV:Body> <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\" SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"> <Command> <Statement>"
				+ mdx
				+ "</Statement> </Command> <Properties> <PropertyList> <DataSourceInfo>[LdCatalogSchema]</DataSourceInfo> <Catalog>[LdCatalogSchema]</Catalog> <Format>Multidimensional</Format> <Content>SchemaData</Content> </PropertyList> </Properties> </Execute> </SOAP-ENV:Body> </SOAP-ENV:Envelope>";

		String response = post(data);

		System.out.println(response);

		time = System.currentTimeMillis() - time;
		Olap4ldUtil._log.info("Run performance evaluation query: finished in "
				+ time + "ms.");

		return response;
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
