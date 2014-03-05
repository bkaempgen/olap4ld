/*
//
// Licensed to Benedikt Kämpgen under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Benedikt Kämpgen licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package org.olap4j.driver.olap4ld.linkeddata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.MemberNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Implements methods of XmlaOlap4jDatabaseMetadata, returning the specified
 * columns as nodes from a sparql endpoint.
 * 
 * @author b-kaempgen
 * 
 */
public class OpenVirtuosoEngine implements LinkedDataCubesEngine {

	/*
	 * Open Virtuoso is default triple store
	 */
	private int SPARQLSERVERTYPE = 2;
	private static final int QCRUMB = 1;
	private static final int OPENVIRTUOSO = 2;
	private static final int OPENVIRTUOSORULESET = 3;

	/**
	 * The type of SPARQL endpoint should be found out automatically and with
	 * the server string
	 */
	private static String SPARQLSERVERURL;

	private static final String DATASOURCEDESCRIPTION = "OLAP data from the statistical Linked Data cloud.";

	private static final String PROVIDERNAME = "The community.";

	private static String URL;

	private static final String DATASOURCEINFO = "Data following the Linked Data principles.";

	private static final String TABLE_CAT = "LdCatalog";

	private static final String TABLE_SCHEM = "LdSchema";

	/*
	 * Array of datasets of the database/catalog/schema that shall be integrated
	 */
	public String DATASOURCENAME;
	public String DATASOURCEVERSION;
	private ArrayList<String> datastructuredefinitions;
	private ArrayList<String> datasets;

	private HashMap<Integer, List<Node[]>> sparqlResultMap = new HashMap<Integer, List<Node[]>>();

	public OpenVirtuosoEngine(URL serverUrlObject,
			ArrayList<String> datastructuredefinitions,
			ArrayList<String> datasets, String databasename) {
		URL = serverUrlObject.toString();
		this.datastructuredefinitions = datastructuredefinitions;
		this.datasets = datasets;

		// TODO: I could add another version with a specific rule set for open
		// virtuoso
		if (databasename.equals("QCRUMB")) {
			SPARQLSERVERTYPE = QCRUMB;
			this.DATASOURCENAME = databasename;
			this.DATASOURCEVERSION = "1.0";
		}
		if (databasename.equals("OPENVIRTUOSO")) {
			SPARQLSERVERTYPE = OPENVIRTUOSO;
			DATASOURCENAME = databasename;
			DATASOURCEVERSION = "1.0";
		}
		if (databasename.equals("OPENVIRTUOSORULESET")) {
			SPARQLSERVERTYPE = OPENVIRTUOSORULESET;
			DATASOURCENAME = databasename;
			DATASOURCEVERSION = "1.0";
		}
		// Set URL
		SPARQLSERVERURL = serverUrlObject.toString();
	}

	/**
	 * Returns from String in order to retrieve information about these URIs
	 * 
	 * 
	 * Properly access the triple store: For dsd and ds we query separate
	 * graphs.
	 * 
	 * @param uris
	 * @return fromResult
	 */
	private String askForFrom(boolean isDsdQuery) {

		switch (SPARQLSERVERTYPE) {
		case QCRUMB:

			/*
			 * 
			 * Qcrumb has special requirements for querying. The given named
			 * graphs in qcrumbs case would already be the locations to be
			 * queried.
			 * 
			 * TODO: Think that through. Can we just query data using qcrumb? We
			 * would simply...
			 */
			/*
			 * Query for all the members of the dimension.
			 * 
			 * The problem is now, how to only query for members of the specific
			 * hierarchy?
			 */
			// First, for each dataset, I need the location
			// String dsFrom = askForFrom(datastructuredefinition
			// .toArray(new String[datastructuredefinition.size()]));
			//
			// // Get all dsd
			// String query = standardPrefix + "select ?ds ?dsd " + dsFrom
			// + "where { ?ds qb:structure ?dsd } ";
			// List<Node[]> dsdsdUris = sparql(query, true);
			//
			// // Now, for each dsd, I need the location
			// ArrayList<String> dsdArray = new ArrayList<String>();
			// boolean first = true;
			// for (Node[] nodes1 : dsdsdUris) {
			// if (first) {
			// first = false;
			// } else {
			// // Create array of dsds
			// dsdArray.add(nodes1[1].toString());
			// }
			// }
			// String dsdFrom = askForFrom(dsdArray.toArray(new String[dsdsdUris
			// .size() - 1]));
			//
			// /*
			// * TODO: How to consider equal dimensions?
			// */
			//
			// // Get all dimensions, with
			// query = standardPrefix
			// + "select ?dimension ?dimensionCaption ?dimensionRange"
			// + dsFrom
			// + " "
			// + dsdFrom
			// +
			// "where { ?ds qb:structure ?dsd. ?dsd qb:component ?compSpec. ?compSpec qb:dimension ?dimension. OPTIONAL{?dimension rdfs:label ?dimensionCaption}. OPTIONAL{?dimension rdfs:range ?dimensionRange} } ";
			// List<Node[]> dimensionUris = sparql(query, true);
			//
			// // Now, for each dimension, I need the location
			// ArrayList<String> dimensionArray = new ArrayList<String>();
			// boolean first1 = true;
			// for (Node[] nodes1 : dimensionUris) {
			// if (first1) {
			// first1 = false;
			// } else {
			// // Create array of dimensions
			// dimensionArray.add(nodes1[0].toString());
			// }
			// }
			// String dimensionFrom = askForFrom(dimensionArray
			// .toArray(new String[dsdsdUris.size() - 1]));
			//
			//
			// String fromResult = "";
			// for (String uri : uris) {
			// String fromLocation;
			// try {
			// fromLocation = askForLocation(uri);
			// fromResult += " from <" + fromLocation + "> ";
			// } catch (MalformedURLException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// }
			// return fromResult;
			return null;
		case OPENVIRTUOSO:
			// This is our hard-coded graph that we use for OLAP4LD
			// We do not use several named graphs, yet.
			// return " from <http://olap4ld/> ";
			// return " from <http://constructed.com> ";

			// return
			// " from <http://vmdeb18.deri.ie:8080/saiku-ui-2.2.RC/#> from <http://vmdeb18.deri.ie:8080/saiku-ui-2.2.RC/#ffiec>";

			/*
			 * Depending on whether we want to query for dsd or data, we ask
			 * different named graphs
			 */
			String fromResult = " ";
			if (isDsdQuery) {
				// create a from from dsd parameter
				for (String dsdGraph : datastructuredefinitions) {
					fromResult += "from <" + dsdGraph + "> ";
				}
			} else {
				// We query for all ds parameters
				for (String dsGraph : datasets) {
					fromResult += "from <" + dsGraph + "> ";

					// // Experiment, we query with Sponger
					// String spongerQuery =
					// "define get:refresh \"3600\" SELECT ?g FROM NAMED <"
					// + dsGraph
					// +
					// "> OPTION (get:soft \"soft\", get:method \"GET\") WHERE { graph ?g { ?s ?p ?o } } LIMIT 1";
					//
					// List<Node[]> cubeUris = sparql(spongerQuery, false);
				}

			}
			return fromResult;
		case OPENVIRTUOSORULESET:
			// This is our hard-coded graph that we use for OLAP4LD
			// We do not use several named graphs, yet.
			// return " from <http://olap4ld/> ";
			// return " from <http://constructed.com> ";

			// return
			// " from <http://vmdeb18.deri.ie:8080/saiku-ui-2.2.RC/#> from <http://vmdeb18.deri.ie:8080/saiku-ui-2.2.RC/#ffiec>";

			/*
			 * Depending on whether we want to query for dsd or data, we ask
			 * different named graphs
			 */
			fromResult = " ";
			if (isDsdQuery) {
				// create a from from dsd parameter
				for (String dsdGraph : datastructuredefinitions) {
					fromResult += "from <" + dsdGraph + "> ";
				}
			} else {
				// We query for all ds parameters
				for (String dsGraph : datasets) {
					fromResult += "from <" + dsGraph + "> ";
				}
			}
			return fromResult;
		default:
			return " ";
		}
	}

	/**
	 * Helper Method for asking for location
	 * 
	 * @param uri
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private String askForLocation(String uri) throws MalformedURLException {
		URL url;
		url = new URL(uri);

		HttpURLConnection.setFollowRedirects(false);
		HttpURLConnection connection;
		try {
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestProperty("Accept", "application/rdf+xml");
			String header = connection.getHeaderField("location");
			// TODO: Could be that we need to check whether bogus comes out
			// (e.g., Not found).
			if (header != null) {

				if (header.startsWith("http:")) {
					uri = header;
				} else {
					// Header only contains the local uri
					// Cut all off until first / from the back
					int index = uri.lastIndexOf("/");
					uri = uri.substring(0, index + 1) + header;
				}
			}
		} catch (IOException e) {
			throw new MalformedURLException(e.getMessage());
		}
		if (uri.endsWith(".ttl")) {
			throw new MalformedURLException(
					"Qcrumb cannot handle non-rdf files, yet");
		}
		return uri;
	}

	/**
	 * I think, caching some sparql results would be very useful.
	 * 
	 * I create a map between hash value of sparql query and the Nodes.
	 * 
	 * If the value is available, I return it.
	 * 
	 * However, when to empty the cache? I empty the cache if I populate a new
	 * cube.
	 * 
	 * @param query
	 * @param caching
	 * @return
	 */
	private List<Node[]> sparql(String query, Boolean caching) {

		// XXX: Caching on
		caching = true;

		Integer hash = null;
		if (caching) {
			hash = query.hashCode();
		}
		if (caching) {
			if (this.sparqlResultMap.containsKey(hash)) {
				return this.sparqlResultMap.get(hash);
			}
		}

		Olap4ldUtil._log.config("SPARQL query: " + query);
		List<Node[]> result;
		switch (SPARQLSERVERTYPE) {
		case QCRUMB:
			result = sparqlQcrumb(query);
			break;
		case OPENVIRTUOSO:
			result = sparqlOpenVirtuoso(query);
			break;
		case OPENVIRTUOSORULESET:
			result = sparqlOpenVirtuosoRuleset(query);
			break;
		default:
			result = null;
			break;
		}
		if (caching) {
			if (this.sparqlResultMap != null
					&& !this.sparqlResultMap.containsKey(hash)) {
				this.sparqlResultMap.put(hash, result);
			} else {
				// TODO: Problem: If this method is run simultaneously, it could
				// be that this happens.
				// throw new UnsupportedOperationException(
				// "Hash key should not be contained.");
			}
		}
		return result;
	}

	/**
	 * If new cube is created, I empty the cache of the Linked Data Engine
	 */
	public void emptySparqlResultCache() {
		this.sparqlResultMap.clear();
	}

	private List<Node[]> sparqlQcrumb(String query) {
		List<Node[]> myBindings = new ArrayList<Node[]>();

		try {
			// Better? ISO-8859-1
			String querysuffix = "?query=" + URLEncoder.encode(query, "UTF-8");
			/*
			 * Curly brackets should not be encoded, says qcrumb
			 * 
			 * querysuffix = querysuffix.replaceAll("%7B", "{"); querysuffix =
			 * querysuffix.replaceAll("%7D", "}");
			 * 
			 * Stimmt nicht: Das Problem ist NX-Darstellung und leere bindings.
			 * 
			 * Momentan wird dazu einfach <null> verwendet.
			 */

			String rulesuffix = "&rules=";
			String acceptsuffix = "&accept=text%2Fnx";

			String fullurl = SPARQLSERVERURL + querysuffix + rulesuffix
					+ acceptsuffix;
			// String fullurl =
			// "http://qcrumb.com/sparql?query=PREFIX+sdmx-measure%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fsdmx%2F2009%2Fmeasure%23>%0D%0APREFIX+dcterms%3A+<http%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F>%0D%0APREFIX+eus%3A+<http%3A%2F%2Fontologycentral.com%2F2009%2F01%2Feurostat%2Fns%23>%0D%0APREFIX+rdf%3A+<http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23>%0D%0APREFIX+qb%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fcube%23>%0D%0APREFIX+rdfs%3A+<http%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23>%0D%0A%0D%0ASELECT+%3Ftime+%3Fvalue+%3Fgeo%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdata%2Ftsieb020>%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdic%2Fgeo>%0D%0AWHERE+{%0D%0A++%3Fs+qb%3Adataset+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fid%2Ftsieb020%23ds>+.%0D%0A++%3Fs+dcterms%3Adate+%3Ftime+.%0D%0A++%3Fs+eus%3Ageo+%3Fg+.%0D%0A++%3Fg+rdfs%3Alabel+%3Fgeo+.%0D%0A++%3Fs+sdmx-measure%3AobsValue+%3Fvalue+.%0D%0A++FILTER+(lang(%3Fgeo)+%3D+\"\")%0D%0A}+ORDER+BY+%3Fgeo%0D%0A&rules=&accept=application%2Fsparql-results%2Bjson"

			HttpURLConnection con = (HttpURLConnection) new URL(fullurl)
					.openConnection();
			// TODO: How to properly set header? Does not work therefore
			// manually
			// added
			con.setRequestProperty("Accept", "application/sparql-results+json");
			con.setRequestMethod("POST");

			if (con.getResponseCode() != 200) {
				throw new RuntimeException("lookup on " + fullurl
						+ " resulted HTTP in status code "
						+ con.getResponseCode());
			}

			// String test = convertStreamToString(con.getInputStream());

			NxParser nxp = new NxParser(con.getInputStream());

			Node[] nxx;
			while (nxp.hasNext()) {
				nxx = nxp.next();
				myBindings.add(nxx);
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return myBindings;
	}

	private List<Node[]> sparqlOpenVirtuoso(String query) {
		List<Node[]> myBindings = new ArrayList<Node[]>();

		try {

			// Better? ISO-8859-1
			String querysuffix = "?query=" + URLEncoder.encode(query, "UTF-8");
			/*
			 * Curly brackets should not be encoded, says qcrumb
			 * 
			 * querysuffix = querysuffix.replaceAll("%7B", "{"); querysuffix =
			 * querysuffix.replaceAll("%7D", "}");
			 * 
			 * Stimmt nicht: Das Problem ist NX-Darstellung und leere bindings.
			 * 
			 * Momentan wird dazu einfach <null> verwendet.
			 */

			// String acceptsuffix = "&accept=text%2Fn3";

			String fullurl = SPARQLSERVERURL + querysuffix;
			// String fullurl =
			// "http://qcrumb.com/sparql?query=PREFIX+sdmx-measure%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fsdmx%2F2009%2Fmeasure%23>%0D%0APREFIX+dcterms%3A+<http%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F>%0D%0APREFIX+eus%3A+<http%3A%2F%2Fontologycentral.com%2F2009%2F01%2Feurostat%2Fns%23>%0D%0APREFIX+rdf%3A+<http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23>%0D%0APREFIX+qb%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fcube%23>%0D%0APREFIX+rdfs%3A+<http%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23>%0D%0A%0D%0ASELECT+%3Ftime+%3Fvalue+%3Fgeo%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdata%2Ftsieb020>%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdic%2Fgeo>%0D%0AWHERE+{%0D%0A++%3Fs+qb%3Adataset+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fid%2Ftsieb020%23ds>+.%0D%0A++%3Fs+dcterms%3Adate+%3Ftime+.%0D%0A++%3Fs+eus%3Ageo+%3Fg+.%0D%0A++%3Fg+rdfs%3Alabel+%3Fgeo+.%0D%0A++%3Fs+sdmx-measure%3AobsValue+%3Fvalue+.%0D%0A++FILTER+(lang(%3Fgeo)+%3D+\"\")%0D%0A}+ORDER+BY+%3Fgeo%0D%0A&rules=&accept=application%2Fsparql-results%2Bjson"

			HttpURLConnection con = (HttpURLConnection) new URL(fullurl)
					.openConnection();
			// TODO: How to properly set header? Does not work therefore
			// manually
			// added
			// con.setRequestProperty("Accept",
			// "application/sparql-results+json");
			// con.setRequestProperty("Accept", "text/n3");
			con.setRequestProperty("Accept", "application/sparql-results+xml");
			con.setRequestMethod("POST");

			if (con.getResponseCode() != 200) {
				throw new RuntimeException("lookup on " + fullurl
						+ " resulted HTTP in status code "
						+ con.getResponseCode());
			}

			// InputStream inputStream = con.getInputStream();
			// String test = XmlaOlap4jUtil.convertStreamToString(inputStream);
			// _log.config("XML output: " + test);

			// Transform sparql xml to nx
			InputStream nx = Olap4ldLinkedDataUtil.transformSparqlXmlToNx(con
					.getInputStream());
			String test2 = Olap4ldLinkedDataUtil.convertStreamToString(nx);
			Olap4ldUtil._log.config("NX output: " + test2);
			nx.reset();

			NxParser nxp = new NxParser(nx);

			Node[] nxx;
			while (nxp.hasNext()) {
				try {
					nxx = nxp.next();
					myBindings.add(nxx);
				} catch (Exception e) {
					Olap4ldUtil._log
							.warning("NxParser: Could not parse properly: "
									+ e.getMessage());
				}
				;
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return myBindings;
	}

	private List<Node[]> sparqlOpenVirtuosoRuleset(String query) {
		List<Node[]> myBindings = new ArrayList<Node[]>();

		try {
			// obsValue sameAs
			// String inferencesuffix = URLEncoder
			// .encode("define input:inference \"http://localhost:8890/schema/property_rules2\" \n",
			// "UTF-8");
			// owl same as
			String inferencesuffix = URLEncoder.encode(
					"define input:same-as \"yes\" \n", "UTF-8");

			// Better? ISO-8859-1
			String querysuffix = "?query=" + inferencesuffix
					+ URLEncoder.encode(query, "UTF-8");
			/*
			 * Curly brackets should not be encoded, says qcrumb
			 * 
			 * querysuffix = querysuffix.replaceAll("%7B", "{"); querysuffix =
			 * querysuffix.replaceAll("%7D", "}");
			 * 
			 * Stimmt nicht: Das Problem ist NX-Darstellung und leere bindings.
			 * 
			 * Momentan wird dazu einfach <null> verwendet.
			 */

			// String acceptsuffix = "&accept=text%2Fn3";

			String fullurl = SPARQLSERVERURL + querysuffix;
			// String fullurl =
			// "http://qcrumb.com/sparql?query=PREFIX+sdmx-measure%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fsdmx%2F2009%2Fmeasure%23>%0D%0APREFIX+dcterms%3A+<http%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F>%0D%0APREFIX+eus%3A+<http%3A%2F%2Fontologycentral.com%2F2009%2F01%2Feurostat%2Fns%23>%0D%0APREFIX+rdf%3A+<http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23>%0D%0APREFIX+qb%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fcube%23>%0D%0APREFIX+rdfs%3A+<http%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23>%0D%0A%0D%0ASELECT+%3Ftime+%3Fvalue+%3Fgeo%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdata%2Ftsieb020>%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdic%2Fgeo>%0D%0AWHERE+{%0D%0A++%3Fs+qb%3Adataset+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fid%2Ftsieb020%23ds>+.%0D%0A++%3Fs+dcterms%3Adate+%3Ftime+.%0D%0A++%3Fs+eus%3Ageo+%3Fg+.%0D%0A++%3Fg+rdfs%3Alabel+%3Fgeo+.%0D%0A++%3Fs+sdmx-measure%3AobsValue+%3Fvalue+.%0D%0A++FILTER+(lang(%3Fgeo)+%3D+\"\")%0D%0A}+ORDER+BY+%3Fgeo%0D%0A&rules=&accept=application%2Fsparql-results%2Bjson"

			HttpURLConnection con = (HttpURLConnection) new URL(fullurl)
					.openConnection();
			// TODO: How to properly set header? Does not work therefore
			// manually
			// added
			// con.setRequestProperty("Accept",
			// "application/sparql-results+json");
			// con.setRequestProperty("Accept", "text/n3");
			con.setRequestProperty("Accept", "application/sparql-results+xml");
			con.setRequestMethod("POST");

			if (con.getResponseCode() != 200) {
				throw new RuntimeException("lookup on " + fullurl
						+ " resulted HTTP in status code "
						+ con.getResponseCode());
			}

			// InputStream inputStream = con.getInputStream();
			// String test = XmlaOlap4jUtil.convertStreamToString(inputStream);
			// _log.info("XML output: " + test);

			// Transform sparql xml to nx
			InputStream nx = Olap4ldLinkedDataUtil.transformSparqlXmlToNx(con
					.getInputStream());
			String test2 = Olap4ldLinkedDataUtil.convertStreamToString(nx);
			Olap4ldUtil._log.config("NX output: " + test2);
			nx.reset();

			NxParser nxp = new NxParser(nx);

			Node[] nxx;
			while (nxp.hasNext()) {
				try {
					nxx = nxp.next();
					myBindings.add(nxx);
				} catch (Exception e) {
					Olap4ldUtil._log
							.warning("NxParser: Could not parse properly: "
									+ e.getMessage());
				}
				;
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return myBindings;
	}

	/**
	 * 
	 * @return
	 */
	public List<Node[]> getDatabases(Restrictions restrictions) {

		/*
		 * DISCOVER_DATASOURCES(new MetadataColumn("DataSourceName"), new
		 * MetadataColumn("DataSourceDescription"), new MetadataColumn("URL"),
		 * new MetadataColumn("DataSourceInfo"), new MetadataColumn(
		 * "ProviderName"), new MetadataColumn("ProviderType"), new
		 * MetadataColumn("AuthenticationMode")),
		 */

		List<Node[]> results = new ArrayList<Node[]>();
		Node[] bindingNames = new Node[] { new Variable("?DATA_SOURCE_NAME"),
				new Variable("?DATA_SOURCE_DESCRIPTION"),
				new Variable("?PROVIDER_NAME"), new Variable("?URL"),
				new Variable("?DATA_SOURCE_INFO") };
		results.add(bindingNames);

		Node[] triple = new Node[] { new Literal(DATASOURCENAME),
				new Literal(DATASOURCEDESCRIPTION), new Literal(PROVIDERNAME),
				new Literal(URL), new Literal(DATASOURCEINFO) };
		results.add(triple);
		return results;
	}

	public List<Node[]> getCatalogs(Restrictions restrictions) {
		/*
		 * DBSCHEMA_CATALOGS( new MetadataColumn("CATALOG_NAME"), new
		 * MetadataColumn( "DESCRIPTION"), new MetadataColumn("ROLES"), new
		 * MetadataColumn("DATE_MODIFIED"))
		 */
		List<Node[]> results = new ArrayList<Node[]>();

		Node[] bindingNames = new Node[] { new Variable("?TABLE_CAT") };
		results.add(bindingNames);

		Node[] triple = new Node[] { new Literal(TABLE_CAT) };
		results.add(triple);

		return results;
	}

	/**
	 * 
	 * @return
	 */
	public List<Node[]> getSchemas(Restrictions restrictions) {
		List<Node[]> results = new ArrayList<Node[]>();
		/*
		 * DBSCHEMA_SCHEMATA(new MetadataColumn( "CATALOG_NAME"), new
		 * MetadataColumn("SCHEMA_NAME"), new MetadataColumn("SCHEMA_OWNER"))
		 */
		Node[] bindingNames = new Node[] { new Variable("?TABLE_SCHEM"),
				new Variable("?TABLE_CAT") };
		results.add(bindingNames);

		Node[] triple = new Node[] { new Literal(TABLE_SCHEM),
				new Literal(TABLE_CAT),
				// No owner
				new Literal("") };
		results.add(triple);

		return results;
	}

	/**
	 * 
	 * Get Cubes from the triple store.
	 * 
	 * Here, the restrictions are strict restrictions without patterns.
	 * 
	 * ==Task: Show proper captions== Problem: Where to take captions from?
	 * rdfs:label Problem: There might be several rdfs:label -> only English
	 * When creating such dsds, we could give the dsd an english label Also, we
	 * need an english label for dimension, hierarchy, level, members Cell
	 * Values will be numeric and not require language
	 * 
	 * @return Node[]{}
	 */
	public List<Node[]> getCubes(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

		// If new cube is created, I empty the cache of the Linked Data Engine
		// this.emptySparqlResultCache();

		/*
		 * To improve performance, we filter for certain cubes. We either find
		 * the cube name in the context or in the restrictions.
		 * 
		 * Question: How do we cache? For all metadata queries, we cache the
		 * SPARQL queries. If new cube is created, I empty the cache of the
		 * Linked Data Engine.
		 */
		// String cubeURI = null;
		// if (context != null && context.olap4jCube != null) {
		// // The name should not have any brackets.
		// cubeURI = LdOlap4jUtil
		// .decodeUriForMdx(context.olap4jCube.getName());
		// }
		// for (int i = 0; i < restrictions.length; i++) {
		// if (restrictions[i].equals("CUBE_NAME")) {
		// cubeURI = LdOlap4jUtil.decodeUriForMdx(restrictions[i + 1]
		// .toString());
		// break;
		// }
		// }

		// Add filter for certain cube
		// additionalFilters +=
		// "FILTER (?CUBE_NAME = <http://public.b-kaempgen.de:8080/fios#secyhofdsd>)";

		// TODO: For now, we only use the non-language-tag CAPTION and
		// DESCRIPTION
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "select distinct \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"CUBE\" as ?CUBE_TYPE min(?DESCRIPTION) as ?DESCRIPTION min(?CUBE_CAPTION) as ?CUBE_CAPTION "
				+ askForFrom(true)
				+ "where { ?ds qb:structure ?CUBE_NAME. ?CUBE_NAME a qb:DataStructureDefinition. OPTIONAL {?CUBE_NAME rdfs:label ?CUBE_CAPTION FILTER ( lang(?CUBE_CAPTION) = \"en\" )} OPTIONAL {?CUBE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"en\" )} "
				+ additionalFilters + "}"
				+ "group by ?CUBE_NAME order by ?CUBE_NAME";

		List<Node[]> result = sparql(query, true);

		/*
		 * Check on restrictions that the interface makes:
		 * 
		 * Restrictions are strong restrictions, no fuzzy, since those wild
		 * cards have been eliminated before.
		 */
		// List<Node[]> result = applyRestrictions(cubeUris, restrictions);
		return result;

	}

	/**
	 * Every measure also needs to be listed as member. When I create the dsd, I
	 * add obsValue as a dimension, but also as a measure. However, members of
	 * the measure dimension would typically all be named differently from the
	 * measure (e.g., obsValue5), therefore, we do not find a match. The problem
	 * is, that getMembers() has to return the measures. So, either, in the dsd,
	 * we need to add a dimension with the measure as a member, or, the query
	 * for the members should return for measures the measure property as
	 * member.
	 * 
	 * 
	 * Here, all the measure properties are returned.
	 * 
	 * @param context
	 * @param metadataRequest
	 * @param restrictions
	 * @return
	 */
	public List<Node[]> getMeasures(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

		// ///////////QUERY//////////////////////////
		/*
		 * TODO: How to consider equal measures?
		 */

		// Boolean values need to be returned as "true" or "false".
		// Get all measures
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "select \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?MEASURE_UNIQUE_NAME ?MEASURE_UNIQUE_NAME as ?MEASURE_NAME min(?MEASURE_CAPTION) as ?MEASURE_CAPTION \"5\" as ?DATA_TYPE \"true\" as ?MEASURE_IS_VISIBLE min(?MEASURE_AGGREGATOR) as ?MEASURE_AGGREGATOR min(?EXPRESSION) as ?EXPRESSION "
				+ askForFrom(true)
				// Hint: For now, MEASURE_CAPTION is simply the
				// MEASURE_DIMENSION.
				// Important: Only measures are queried
				// XXX: EXPRESSION also needs to be attached to
				// ?COMPONENT_SPECIFICATION
				+ " where { ?CUBE_NAME qb:component ?COMPONENT_SPECIFICATION "
				+ ". ?COMPONENT_SPECIFICATION qb:measure ?MEASURE_UNIQUE_NAME. OPTIONAL {?MEASURE_UNIQUE_NAME rdfs:label ?MEASURE_CAPTION FILTER ( lang(?MEASURE_CAPTION) = \"en\" )} OPTIONAL {?COMPONENT_SPECIFICATION qb:aggregator ?MEASURE_AGGREGATOR } OPTIONAL {?COMPONENT_SPECIFICATION qb:expression ?EXPRESSION } "
				+ additionalFilters
				+ "} "
				+ "group by ?CUBE_NAME ?MEASURE_UNIQUE_NAME order by ?CUBE_NAME ?MEASURE_UNIQUE_NAME ";
		List<Node[]> result = sparql(query, true);

		// List<Node[]> result = applyRestrictions(measureUris, restrictions);
		return result;
	}

	/**
	 * Get possible dimensions (component properties) for each cube from the
	 * triple store.
	 * 
	 * Approach: I create the output from Linked Data, and then I filter it
	 * using the restrictions.
	 * 
	 * I have to also return the Measures dimension for each cube.
	 * 
	 * @return Node[]{?dsd ?dimension ?compPropType ?name}
	 * @throws MalformedURLException
	 */
	public List<Node[]> getDimensions(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

		// Get all dimensions
		String query = "";
		query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "select \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME as ?DIMENSION_NAME ?DIMENSION_UNIQUE_NAME min(?DIMENSION_CAPTION) as ?DIMENSION_CAPTION min(?DESCRIPTION) as ?DESCRIPTION \"0\" as ?DIMENSION_TYPE \"0\" as ?DIMENSION_ORDINAL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:label ?DIMENSION_CAPTION FILTER ( lang(?DIMENSION_CAPTION) = \"en\" )} OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"en\" )} "
				+ additionalFilters
				+ "} "
				+ "group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME order by ?CUBE_NAME ?DIMENSION_NAME ";

		List<Node[]> result = sparql(query, true);

		// List<Node[]> result = applyRestrictions(dimensionUris, restrictions);

		/*
		 * Get all measures: We query for all cubes and simply add measure
		 * information.
		 * 
		 * We need "distinct" in case we have several measures per cube.
		 */

		if (isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			// Here, we again need specific filters, since only cube makes
			// sense
			String specificFilters = "";
			if (restrictions.cubeNamePattern != null) {
				specificFilters = " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}

			// In this case, we do ask for a measure dimension.
			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "select distinct \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?DIMENSION_CAPTION \"Measures\" as ?DESCRIPTION \"2\" as ?DIMENSION_TYPE \"0\" as ?DIMENSION_ORDINAL "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec "
					+ ". ?compSpec qb:measure ?MEASURE_PROPERTY. "
					+ specificFilters + "} "
					+ "order by ?CUBE_NAME ?MEASURE_PROPERTY";

			List<Node[]> result2 = sparql(query, true);

			// List<Node[]> result2 = applyRestrictions(memberUris2,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : result2) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		return result;
	}

	/**
	 * We query for a measure if dim, hier, levl = measure OR if nothing is
	 * stated.
	 * 
	 * @param dimensionUniqueName
	 * @param hierarchyUniqueName
	 * @param levelUniqueName
	 * @return
	 */
	private boolean isMeasureQueriedFor(String dimensionUniqueName,
			String hierarchyUniqueName, String levelUniqueName) {
		// If one is set, it should not be Measures, not.
		// Watch out: square brackets are needed.
		boolean notExplicitlyStated = dimensionUniqueName == null
				&& hierarchyUniqueName == null && levelUniqueName == null;
		boolean explicitlyStated = (dimensionUniqueName != null && dimensionUniqueName
				.equals("Measures"))
				|| (hierarchyUniqueName != null && hierarchyUniqueName
						.equals("Measures"))
				|| (levelUniqueName != null && levelUniqueName
						.equals("Measures"));

		return notExplicitlyStated || explicitlyStated;

	}

	/**
	 * 
	 * Return hierarchies
	 * 
	 * @param context
	 * @param metadataRequest
	 * @param restrictions
	 * @return
	 */
	public List<Node[]> getHierarchies(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

		// Get all hierarchies with codeLists
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "select \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?HIERARCHY_NAME min(?HIERARCHY_CAPTION) as ?HIERARCHY_CAPTION min(?DESCRIPTION) as ?DESCRIPTION "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. OPTIONAL {?HIERARCHY_UNIQUE_NAME rdfs:label ?HIERARCHY_CAPTION FILTER ( lang(?HIERARCHY_CAPTION) = \"en\" ) } OPTIONAL {?HIERARCHY_UNIQUE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"en\" ) } "
				+ additionalFilters
				+ "} "
				+ "group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ";
		List<Node[]> result = sparql(query, true);

		// TODO: No sorting done, yet

		// List<Node[]> result = applyRestrictions(hierarchyResults,
		// restrictions);

		// Get measure dimensions, but only if neither dim, hier, lev is set and
		// not "Measures".
		if (isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			// Here, we again need specific filters, since only cube makes
			// sense
			String specificFilters = "";
			if (restrictions.cubeNamePattern != null) {
				specificFilters = " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}

			// In this case, we do ask for a measure hierarchy.
			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "select distinct \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?HIERARCHY_UNIQUE_NAME \"Measures\" as ?HIERARCHY_NAME \"Measures\" as ?HIERARCHY_CAPTION \"Measures\" as ?DESCRIPTION "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec "
					+ ". ?compSpec qb:measure ?MEASURE_PROPERTY. "
					+ specificFilters
					+ "} order by ?CUBE_NAME ?MEASURE_PROPERTY ?HIERARCHY_NAME ";

			List<Node[]> result2 = sparql(query, true);

			// List<Node[]> result2 = applyRestrictions(memberUris2,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : result2) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Get dimension hierarchies without codeList, but only if hierarchy is
		// not set and different from dimension unique name
		if (restrictions.hierarchyUniqueName != null
				&& !restrictions.hierarchyUniqueName
						.equals(restrictions.dimensionUniqueName)) {
			// we do not need to do this
		} else {

			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "select \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_CAPTION ?DIMENSION_UNIQUE_NAME as ?DESCRIPTION "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec "
					+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. FILTER NOT EXISTS { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. } "
					+ additionalFilters
					+ "} order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ";

			List<Node[]> result3 = sparql(query, true);

			// List<Node[]> result3 = applyRestrictions(memberUris3,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : result3) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		return result;
	}

	/**
	 * 
	 * @param context
	 * @param metadataRequest
	 * @param restrictions
	 * @return
	 */
	public List<Node[]> getLevels(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

		// Get all levels of code lists without levels
		// TODO: LEVEL_CARDINALITY is not solved, yet.
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "select \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?LEVEL_CAPTION ?HIERARCHY_UNIQUE_NAME as ?LEVEL_NAME \"N/A\" as ?DESCRIPTION \"1\" as ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. FILTER NOT EXISTS { ?LEVEL_UNIQUE_NAME skos:inScheme ?HIERARCHY_UNIQUE_NAME. }"
				+ additionalFilters
				+ " }"
				+ " order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_NUMBER ";
		List<Node[]> result = sparql(query, true);

		// List<Node[]> result = applyRestrictions(levelResults, restrictions);

		// TODO: No sorting done, yet

		// TODO: Add properly modeled level
		query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "select \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME min(?LEVEL_CAPTION) as ?LEVEL_CAPTION ?LEVEL_UNIQUE_NAME as ?LEVEL_NAME min(?DESCRIPTION) as ?DESCRIPTION ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skos:inScheme ?HIERARCHY_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skosclass:depth ?LEVEL_NUMBER. OPTIONAL {?LEVEL_UNIQUE_NAME rdfs:label ?LEVEL_CAPTION FILTER ( lang(?LEVEL_CAPTION) = \"en\" ) } OPTIONAL {?LEVEL_UNIQUE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"en\" ) }"
				+ additionalFilters
				+ "}"
				+ "group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_NUMBER ";
		List<Node[]> result0 = sparql(query, true);

		// List<Node[]> result0 = applyRestrictions(memberUris0, restrictions);

		// Add all of result2 to result
		boolean first = true;
		for (Node[] nodes : result0) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		// Distinct for several measures per cube.
		// Add measures levels
		// Second, ask for the measures (which are also members), but only if
		// measure

		if (isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			// Here, we again need specific filters, since only cube makes
			// sense
			String specificFilters = "";
			if (restrictions.cubeNamePattern != null) {
				specificFilters = " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}

			// In this case, we do ask for a measure dimension.
			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "select distinct \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?HIERARCHY_UNIQUE_NAME \"Measures\" as ?LEVEL_UNIQUE_NAME \"Measures\" as ?LEVEL_CAPTION \"Measures\" as ?LEVEL_NAME \"Measures\" as ?DESCRIPTION \"1\" as ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec "
					+ ". ?compSpec qb:measure ?MEASURE_PROPERTY. "
					+ specificFilters + "} " + "order by ?MEASURE_PROPERTY";

			List<Node[]> result2 = sparql(query, true);

			// List<Node[]> result2 = applyRestrictions(memberUris2,
			// restrictions);

			// Add all of result2 to result
			first = true;
			for (Node[] nodes : result2) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Add levels for dimensions without codelist, but only if hierarchy and
		// dimension names are equal
		if (restrictions.hierarchyUniqueName != null
				&& !restrictions.hierarchyUniqueName
						.equals(restrictions.dimensionUniqueName)) {
			// we do not need to do this
		} else {

			// We need specific filter
			String specificFilters = "";
			if (restrictions.cubeNamePattern != null) {
				specificFilters += " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}
			if (restrictions.dimensionUniqueName != null) {
				specificFilters += " FILTER (?DIMENSION_UNIQUE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.dimensionUniqueName)
						+ ">) ";
			}

			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "select \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?LEVEL_CAPTION ?DIMENSION_UNIQUE_NAME as ?LEVEL_NAME \"N/A\" as ?DESCRIPTION \"1\" as ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec "
					+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. FILTER NOT EXISTS { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. }  "
					+ specificFilters
					+ "} "
					+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?LEVEL_NUMBER ";

			// Second, ask for the measures (which are also members)
			List<Node[]> result3 = sparql(query, true);

			// List<Node[]> result3 = applyRestrictions(memberUris3,
			// restrictions);

			// Add all of result3 to result
			first = true;
			for (Node[] nodes : result3) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		return result;
	}

	/**
	 * Important issues to remember: Every measure also needs to be listed as
	 * member. When I create the dsd, I add obsValue as a dimension, but also as
	 * a measure. However, members of the measure dimension would typically all
	 * be named differently from the measure (e.g., obsValue5), therefore, we do
	 * not find a match. The problem is, that getMembers() has to return the
	 * measures. So, either, in the dsd, we need to add a dimension with the
	 * measure as a member, or, the query for the members should return for
	 * measures the measure property as member.
	 * 
	 * The dimension/hierarchy/level of a measure should always be "Measures".
	 * 
	 * Typically, a measure should not have a codeList, since we can have many
	 * many members. If a measure does not have a codelist, the bounding would
	 * still work, since The componentProperty is existing, but no hierarchy...
	 * 
	 * For caption of members, we should eventually use
	 * http://www.w3.org/2004/02/skos/core#notation skos:notation, since members
	 * are in rdf represented as skos:Concept and this is the proper way to give
	 * them a representation.
	 * 
	 * Assumptions of this method:
	 * 
	 * The restrictions are set up only as follows 1) cube, dim, hier, level 2)
	 * cube, dim, hier, level, member, null 3) cube, dim, hier, level, member,
	 * treeOp
	 * 
	 * The members are only modelled as follows 1) Measure Member (member of the
	 * measure dimension) 2) Level Member (member of a regular dimension) 3) Top
	 * Concept Member (member via skos:topConcept) 4) Degenerated Member (member
	 * without code list)
	 * 
	 * @return Node[]{?memberURI ?name}
	 * @throws MalformedURLException
	 */
	public List<Node[]> getMembers(Restrictions restrictions) {

		List<Node[]> result = new ArrayList<Node[]>();
		List<Node[]> intermediaryresult = null;

		// Create header
		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?DIMENSION_UNIQUE_NAME"),
				new Variable("?HIERARCHY_UNIQUE_NAME"),
				new Variable("?LEVEL_UNIQUE_NAME"),
				new Variable("?LEVEL_NUMBER"), new Variable("?MEMBER_NAME"),
				new Variable("?MEMBER_UNIQUE_NAME"),
				new Variable("?MEMBER_CAPTION"), new Variable("?MEMBER_TYPE"),
				new Variable("?PARENT_UNIQUE_NAME"),
				new Variable("?PARENT_LEVEL") };
		result.add(header);

		// Measure Member
		if (isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {
			intermediaryresult = getMeasureMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		// Normal Member
		// Watch out: No square brackets
		if ((restrictions.dimensionUniqueName == null || !restrictions.dimensionUniqueName
				.equals("Measures"))
				|| (restrictions.hierarchyUniqueName == null || !restrictions.hierarchyUniqueName
						.equals("Measures"))
				|| (restrictions.levelUniqueName == null || !restrictions.levelUniqueName
						.equals("Measures"))) {

			intermediaryresult = getLevelMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		// If we still do not have members, then we might have top members
		// only
		if (result.size() == 1) {

			intermediaryresult = getHasTopConceptMembers(restrictions);

			addToResult(intermediaryresult, result);
		}

		// If we still do not have members, then we might have degenerated
		// members
		if (result.size() == 1) {
			// Members without codeList
			intermediaryresult = getDegeneratedMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		return result;
	}

	private List<Node[]> getMeasureMembers(Restrictions restrictions) {
		String additionalFilters = "";

		/*
		 * I would assume that if TREE_OP is set, we have a unique member given
		 * and either want its children, its siblings, its parent, self,
		 * ascendants, or descendants.
		 */
		if (restrictions.tree != null && (restrictions.tree & 8) != 8) {

			// Assumption 1: Treeop only uses Member
			if (restrictions.memberUniqueName == null) {
				throw new UnsupportedOperationException(
						"If a treeMask is given, we should also have a unique member name!");
			}

			if ((restrictions.tree & 1) == 1) {
				// CHILDREN
				Olap4ldUtil._log.config("TreeOp:CHILDREN");

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.config("TreeOp:SIBLINGS");

				if (restrictions.cubeNamePattern != null) {
					additionalFilters += " FILTER (?CUBE_NAME = <"
							+ Olap4ldLinkedDataUtil
									.convertMDXtoURI(restrictions.cubeNamePattern)
							+ ">) ";
				}
			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.config("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.config("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.config("TreeOp:ANCESTORS");
			}

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.config("TreeOp:SELF");

			if (restrictions.cubeNamePattern != null) {
				additionalFilters += " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}
			if (restrictions.memberUniqueName != null) {
				additionalFilters += " FILTER (?MEASURE_UNIQUE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.memberUniqueName)
						+ ">) ";
			}

		}

		// Second, ask for the measures (which are also members)
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes()
				+ "Select \""
				+ TABLE_CAT
				+ "\" as ?CATALOG_NAME \""
				+ TABLE_SCHEM
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?HIERARCHY_UNIQUE_NAME \"Measures\" as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEASURE_UNIQUE_NAME as ?MEMBER_UNIQUE_NAME ?MEASURE_UNIQUE_NAME as ?MEMBER_NAME min(?MEMBER_CAPTION) as ?MEMBER_CAPTION \"3\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"0\" as ?PARENT_LEVEL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?COMPONENT_SPECIFICATION. "
				+ "?COMPONENT_SPECIFICATION qb:measure ?MEASURE_UNIQUE_NAME. OPTIONAL {?MEASURE_UNIQUE_NAME rdfs:label ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"en\" OR lang(?MEMBER_CAPTION) = \"\" ) } "
				+ additionalFilters
				+ "} "
				+ "group by ?CUBE_NAME ?MEASURE_UNIQUE_NAME order by ?MEASURE_UNIQUE_NAME";

		List<Node[]> memberUris2 = sparql(query, true);

		return memberUris2;
	}

	/**
	 * Finds specific typical members.
	 * 
	 * @param restrictions
	 * 
	 * @return
	 */
	private List<Node[]> getLevelMembers(Restrictions restrictions) {
		String query = "";
		String additionalFilters = createFilterForRestrictions(restrictions);
		List<Node[]> intermediaryresult;

		/*
		 * I would assume that if TREE_OP is set, we have a unique member given
		 * and either want its children, its siblings, its parent, self,
		 * ascendants, or descendants.
		 */
		if (restrictions.tree != null && (restrictions.tree & 8) != 8) {

			// Assumption 1: Treeop only uses Member
			if (restrictions.memberUniqueName == null) {
				throw new UnsupportedOperationException(
						"If a treeMask is given, we should also have a unique member name!");
			}

			if ((restrictions.tree & 1) == 1) {
				// CHILDREN
				Olap4ldUtil._log.config("TreeOp:CHILDREN");

				// Here, we need a specific filter
				additionalFilters = " FILTER (?PARENT_UNIQUE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.memberUniqueName)
						+ ">) ";

				query = Olap4ldLinkedDataUtil.getStandardPrefixes()
						+ "Select \""
						+ TABLE_CAT
						+ "\" as ?CATALOG_NAME \""
						+ TABLE_SCHEM
						+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME min(?MEMBER_CAPTION) as ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE ?PARENT_UNIQUE_NAME min(?PARENT_LEVEL) as ?PARENT_LEVEL "
						+ askForFrom(true)
						+ " where { ?CUBE_NAME qb:component ?compSpec. "
						+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skos:inScheme ?HIERARCHY_UNIQUE_NAME. ?MEMBER_UNIQUE_NAME skos:member ?LEVEL_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skosclass:depth ?LEVEL_NUMBER. "
						+ "?MEMBER_UNIQUE_NAME skos:narrower ?PARENT_UNIQUE_NAME. ?PARENT_UNIQUE_NAME skos:member ?PARENT_LEVEL_UNIQUE_NAME. ?PARENT_LEVEL_UNIQUE_NAME skosclass:depth ?PARENT_LEVEL. OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"en\" OR lang(?MEMBER_CAPTION) = \"\" ) } "
						+ additionalFilters
						+ "} "
						+ " group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?PARENT_UNIQUE_NAME ?MEMBER_UNIQUE_NAME order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";
				intermediaryresult = sparql(query, true);

				return intermediaryresult;

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.config("TreeOp:SIBLINGS");

			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.config("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.config("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.config("TreeOp:ANCESTORS");
			}

			throw new UnsupportedOperationException(
					"TreeOp and getLevelMember failed.");

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.config("TreeOp:SELF");

			// XXX: Can be aligned with other sparql query??
			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "Select \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME min(?MEMBER_CAPTION) as ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE ?PARENT_UNIQUE_NAME min(?PARENT_LEVEL) as ?PARENT_LEVEL "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec. "
					+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skos:inScheme ?HIERARCHY_UNIQUE_NAME. ?MEMBER_UNIQUE_NAME skos:member ?LEVEL_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skosclass:depth ?LEVEL_NUMBER. "
					+ "OPTIONAL { ?MEMBER_UNIQUE_NAME skos:narrower ?PARENT_UNIQUE_NAME. ?PARENT_UNIQUE_NAME skosclass:depth ?PARENT_LEVEL. }"
					+ " OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"en\" OR lang(?MEMBER_CAPTION) = \"\" ) } "
					+ additionalFilters
					+ "} "
					+ "group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?PARENT_UNIQUE_NAME ?MEMBER_UNIQUE_NAME order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

			List<Node[]> memberUris2 = sparql(query, true);

			return memberUris2;
		}
	}

	/**
	 * Returns all hasTopConcept members of the cube.
	 * 
	 * @param dimensionUniqueName
	 * @param cubeNamePattern
	 * 
	 * @param cubeNamePattern
	 * @return
	 */
	private List<Node[]> getHasTopConceptMembers(Restrictions restrictions) {
		String query = "";

		/*
		 * I would assume that if TREE_OP is set, we have a unique member given
		 * and either want its children, its siblings, its parent, self,
		 * ascendants, or descendants.
		 */
		if (restrictions.tree != null && (restrictions.tree & 8) != 8) {

			// Assumption 1: Treeop only uses Member
			if (restrictions.memberUniqueName == null) {
				throw new UnsupportedOperationException(
						"If a treeMask is given, we should also have a unique member name!");
			}

			if ((restrictions.tree & 1) == 1) {
				// CHILDREN
				Olap4ldUtil._log.config("TreeOp:CHILDREN");

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.config("TreeOp:SIBLINGS");

			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.config("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.config("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.config("TreeOp:ANCESTORS");
			}

			throw new UnsupportedOperationException(
					"TreeOp and getLevelMember failed.");

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.config("TreeOp:SELF");

			// Here, we again need specific filters, since only cube makes
			// sense
			String specificFilters = "";
			if (restrictions.cubeNamePattern != null) {
				specificFilters += " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}
			if (restrictions.dimensionUniqueName != null) {
				specificFilters += " FILTER (?DIMENSION_UNIQUE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.dimensionUniqueName)
						+ ">) ";
			}

			// First, ask for all members
			// Get all members of hierarchies without levels, that simply
			// define
			// skos:hasTopConcept members with skos:notation.
			query = Olap4ldLinkedDataUtil.getStandardPrefixes()
					+ "Select \""
					+ TABLE_CAT
					+ "\" as ?CATALOG_NAME \""
					+ TABLE_SCHEM
					+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME min(?MEMBER_CAPTION) as ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"null\" as ?PARENT_LEVEL "
					+ askForFrom(true)
					+ " where { ?CUBE_NAME qb:component ?compSpec. "
					+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?HIERARCHY_UNIQUE_NAME skos:hasTopConcept ?MEMBER_UNIQUE_NAME. "
					+ " OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"en\" OR lang(?MEMBER_CAPTION) = \"\" ) } "
					+ specificFilters
					+ " } "
					+ " group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

			List<Node[]> memberUris = sparql(query, true);

			return memberUris;

		}

	}

	/**
	 * For degenerated dimensions, we have to assume that either dim, hier, or
	 * level are given.
	 * 
	 * @return
	 */
	private List<Node[]> getDegeneratedMembers(Restrictions restrictions) {

		if (restrictions.tree != null && (restrictions.tree & 8) != 8) {

			// Assumption 1: Treeop only uses Member
			if (restrictions.memberUniqueName == null) {
				throw new UnsupportedOperationException(
						"If a treeMask is given, we should also have a unique member name!");
			}

			if ((restrictions.tree & 1) == 1) {
				// CHILDREN
				Olap4ldUtil._log.config("TreeOp:CHILDREN");

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.config("TreeOp:SIBLINGS");

			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.config("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.config("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.config("TreeOp:ANCESTORS");
			}

			throw new UnsupportedOperationException(
					"TreeOp and getLevelMember failed.");

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.config("TreeOp:SELF");

			// First we ask for the dimensionWithoutHierarchies
			String dimensionWithoutHierarchies = null;
			if (restrictions.dimensionUniqueName != null) {
				dimensionWithoutHierarchies = Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.dimensionUniqueName);
			} else if (restrictions.hierarchyUniqueName != null) {
				dimensionWithoutHierarchies = Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.hierarchyUniqueName);
			} else if (restrictions.levelUniqueName != null) {
				dimensionWithoutHierarchies = Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.levelUniqueName);
			}

			// Here, we need specific filters, since only cube and dimension
			// unique name makes sense
			String alteredadditionalFilters = "";
			if (restrictions.cubeNamePattern != null) {
				alteredadditionalFilters += " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}
			if (restrictions.memberUniqueName != null) {
				// Check whether uri or Literal
				String resource = Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.memberUniqueName);
				if (isResourceAndNotLiteral(resource)) {
					alteredadditionalFilters += " FILTER (?MEMBER_UNIQUE_NAME = <"
							+ resource + ">) ";
				} else {
					alteredadditionalFilters += " FILTER (str(?MEMBER_UNIQUE_NAME) = \""
							+ resource + "\") ";
				}
			}

			// It is possible that dimensionWithoutHierarchies is null
			List<Node[]> memberUris1 = null;
			String query = null;

			// XXX: Necessary distinction? I think not...
			if (dimensionWithoutHierarchies == null) {

				query = Olap4ldLinkedDataUtil.getStandardPrefixes()
						+ "Select distinct \""
						+ TABLE_CAT
						+ "\" as ?CATALOG_NAME \""
						+ TABLE_SCHEM
						+ "\" as ?SCHEMA_NAME ?CUBE_NAME "
						+ " ?DIMENSION_UNIQUE_NAME "
						+ " ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME "
						+ "?DIMENSION_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME min(?MEMBER_CAPTION) as ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"null\" as ?PARENT_LEVEL "
						// Since we query for instances
						+ askForFrom(true)
						+ askForFrom(false)
						+ " where { ?CUBE_NAME qb:component ?compSpec. "
						+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. "
						+ "?obs qb:dataSet ?ds. ?ds qb:structure ?CUBE_NAME. "
						+ "?obs ?DIMENSION_UNIQUE_NAME ?MEMBER_UNIQUE_NAME. "
						+ " OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"en\" OR lang(?MEMBER_CAPTION) = \"\" ) } "
						+ alteredadditionalFilters
						+ "} "
						+ " group by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME order by ?CUBE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

				memberUris1 = sparql(query, true);

			} else {

				query = Olap4ldLinkedDataUtil.getStandardPrefixes()
						+ "Select distinct \""
						+ TABLE_CAT
						+ "\" as ?CATALOG_NAME \""
						+ TABLE_SCHEM
						+ "\" as ?SCHEMA_NAME ?CUBE_NAME \""
						+ dimensionWithoutHierarchies
						+ "\" as ?DIMENSION_UNIQUE_NAME \""
						+ dimensionWithoutHierarchies
						+ "\" as ?HIERARCHY_UNIQUE_NAME \""
						+ dimensionWithoutHierarchies
						+ "\" as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME min(?MEMBER_CAPTION) as ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"null\" as ?PARENT_LEVEL "
						// Since we query for instances
						+ askForFrom(true)
						+ askForFrom(false)
						+ " where { ?CUBE_NAME qb:component ?compSpec. "
						+ "?compSpec qb:dimension <"
						+ dimensionWithoutHierarchies
						+ ">. "
						+ "?obs qb:dataSet ?ds. ?ds qb:structure ?CUBE_NAME. ?obs <"
						+ dimensionWithoutHierarchies
						+ "> ?MEMBER_UNIQUE_NAME. "
						+ " OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"en\" OR lang(?MEMBER_CAPTION) = \"\" ) } "
						+ alteredadditionalFilters
						+ "} "
						+ " group by ?CUBE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME order by ?CUBE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

				memberUris1 = sparql(query, true);

			}

			return memberUris1;

		}

	}

	private boolean isResourceAndNotLiteral(String resource) {
		return resource.startsWith("http:");
	}

	private String createFilterForRestrictions(Restrictions restrictions) {
		// We need to create a filter for the specific restriction
		String cubeNamePatternFilter = (restrictions.cubeNamePattern != null) ? " FILTER (?CUBE_NAME = <"
				+ Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.cubeNamePattern) + ">) "
				: "";
		String dimensionUniqueNameFilter = (restrictions.dimensionUniqueName != null) ? " FILTER (?DIMENSION_UNIQUE_NAME = <"
				+ Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.dimensionUniqueName)
				+ ">) " : "";
		String hierarchyUniqueNameFilter = (restrictions.hierarchyUniqueName != null) ? " FILTER (?HIERARCHY_UNIQUE_NAME = <"
				+ Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.hierarchyUniqueName)
				+ ">) " : "";
		String levelUniqueNameFilter = (restrictions.levelUniqueName != null) ? " FILTER (?LEVEL_UNIQUE_NAME = <"
				+ Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.levelUniqueName) + ">) "
				: "";
		String memberUniqueNameFilter = (restrictions.memberUniqueName != null) ? " FILTER (str(?MEMBER_UNIQUE_NAME) = str(<"
				+ Olap4ldLinkedDataUtil
						.convertMDXtoURI(restrictions.memberUniqueName)
				+ ">)) " : "";

		return cubeNamePatternFilter + dimensionUniqueNameFilter
				+ hierarchyUniqueNameFilter + levelUniqueNameFilter
				+ memberUniqueNameFilter;
	}

	/**
	 * Adds intermediary results to result.
	 * 
	 * @param intermediaryresult
	 * @param result
	 */
	private void addToResult(List<Node[]> intermediaryresult,
			List<Node[]> result) {
		boolean first = true;
		for (Node[] nodes : intermediaryresult) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}
	}

	public List<Node[]> getSets(Restrictions restrictions) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Input: We get the queried cube. We get a list of levels of inquired
	 * dimensions. Also a list of measures. And for each diced dimension a set
	 * of members.
	 * 
	 * @return The SPARQL output of the relational format of the
	 *         multidimensional result.
	 */
	public List<Node[]> executeOlapQuery(Cube cube, List<Level> slicesrollups,
			List<Position> dices, List<Measure> projections) {
		// cube is olap4ld
		Cube mycube = (Cube) cube;

		String dsd = Olap4ldLinkedDataUtil.convertMDXtoURI(mycube
				.getUniqueName());

		// Now, we have all neccessary data
		String selectClause = "";
		String whereClause = "";
		String groupByClause = "group by ";
		String orderByClause = "order by ";

		// Typically, a cube is a dataset (actually, it represents a cube, but,
		// anyway)

		whereClause += " ?obs qb:dataSet ?ds." + " ?ds qb:structure <" + dsd
				+ ">.";

		// HashMaps for level height of dimension
		HashMap<Integer, Integer> levelHeightMap = new HashMap<Integer, Integer>();

		for (Level level : slicesrollups) {

			// At the moment, we do not support hierarchy/roll-up, but simply
			// query for all dimension members
			// String hierarchyURI = LdOlap4jUtil.decodeUriForMdx(hierarchy
			// .getUniqueName());
			String dimensionProperty = Olap4ldLinkedDataUtil
					.convertMDXtoURI(level.getDimension().getUniqueName());
			// XXX Why exactly do I include the level?
			String levelURI = Olap4ldLinkedDataUtil.convertMDXtoURI(level
					.getUniqueName());

			// Now, depending on level depth we need to behave differently
			// Slicer level is the number of property-value pairs from the most
			// granular value
			// For that, the maximum depth and the level depth are used.
			// TODO: THis is quite a simple approach, does not consider possible
			// access restrictions.
			Integer levelHeight = (level.getHierarchy().getLevels().size() - 1 - level
					.getDepth());

			// Note as inserted.
			levelHeightMap.put(dimensionProperty.hashCode(), levelHeight);

			whereClause += addLevelPropertyPath(0, levelHeight,
					dimensionProperty, levelURI);

			String dimensionPropertyVariable = makeUriToParameter(dimensionProperty);
			selectClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
			groupByClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
			orderByClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
		}

		// For each filter tuple
		if (dices != null && !dices.isEmpty()) {
			// We assume that each position has the same metadata (i.e., Levels)
			// so that we only need to add graph patterns for the first position
			for (Member member : dices.get(0).getMembers()) {
				// We should be testing whether diceslevelHeight higher than
				// slicesRollupsLevelHeight
				// Dices Level Height
				Integer diceslevelHeight = (member.getHierarchy().getLevels()
						.size() - 1 - member.getLevel().getDepth());
				String dimensionProperty = Olap4ldLinkedDataUtil
						.convertMDXtoURI(member.getLevel().getDimension()
								.getUniqueName());
				Integer slicesRollupsLevelHeight = (levelHeightMap
						.containsKey(dimensionProperty.hashCode())) ? levelHeightMap
						.get(dimensionProperty.hashCode()) : 0;
				if (member.getMemberType() != Member.Type.MEASURE
						&& diceslevelHeight > slicesRollupsLevelHeight) {

					String levelURI = Olap4ldLinkedDataUtil
							.convertMDXtoURI(member.getLevel().getUniqueName());

					whereClause += addLevelPropertyPath(
							slicesRollupsLevelHeight, diceslevelHeight,
							dimensionProperty, levelURI);

				}
			}

			whereClause += "FILTER (";
			List<String> orList = new ArrayList<String>();

			for (Position position : dices) {

				// Each position is an OR
				List<String> andList = new ArrayList<String>();

				for (Member member : position.getMembers()) {
					// We need to know the variable to filter
					// First, we need to convert it to URI representation.
					String dimensionPropertyVariable = makeUriToParameter(Olap4ldLinkedDataUtil
							.convertMDXtoURI(member.getLevel().getDimension()
									.getUniqueName()));

					// We need to know the member to filter
					String memberResource = Olap4ldLinkedDataUtil
							.convertMDXtoURI(member.getUniqueName());

					// Need to know the level of the member
					Integer diceslevelHeight = (member.getHierarchy()
							.getLevels().size() - 1 - member.getLevel()
							.getDepth());

					if (isResourceAndNotLiteral(memberResource)) {
						andList.add(" ?" + dimensionPropertyVariable
								+ diceslevelHeight + " = " + "<"
								+ memberResource + "> ");
					} else {
						// For some reason, we need to convert the variable
						// using str.
						andList.add(" str(?" + dimensionPropertyVariable
								+ diceslevelHeight + ") = " + "\""
								+ memberResource + "\" ");
					}

				}

				orList.add(Olap4ldLinkedDataUtil.implodeArray(
						andList.toArray(new String[0]), " AND "));
			}
			whereClause += Olap4ldLinkedDataUtil.implodeArray(
					orList.toArray(new String[0]), " OR ");
			whereClause += ") ";

		}

		// This huge thing probably tries to combine filter expressions to
		// efficient SPARQL expressions
		// // First, what dimension?
		// // If Measures, then do not filter (since the
		// // selectionpredicates also contain measures, since that was
		// // easier to do beforehand)
		// try {
		// if (position.get(0).getDimension().getDimensionType()
		// .equals(Dimension.Type.MEASURE)) {
		// continue;
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // We assume that all contained members are of the same
		// // dimension and the same level
		// String slicerDimensionProperty = LdOlap4jUtil
		// .convertMDXtoURI(position.get(0).getDimension()
		// .getUniqueName());
		// String slicerDimensionPropertyVariable = makeParameter(position
		// .get(0).getDimension().getUniqueName());
		//
		// /*
		// * slicerLevelNumber would give me the difference between the
		// * maximal depth of all levels and the level depth (presumably
		// * the same for all tuples). This tells us the number of
		// * property-value pairs from the most granular element.
		// */
		// Integer slicerLevelNumber = (position.get(0).getLevel()
		// .getHierarchy().getLevels().size() - 1 - position
		// .get(0).getLevel().getDepth());
		//
		// /*
		// * Here, instead of going through all the members and filter for
		// * them one by one, we need to "compress" the filter expression
		// * somehow.
		// */
		// // Check whether the leveltype is integer and define smallest
		// // and biggest
		// boolean isInteger = true;
		// int smallestTuple = 0;
		// int biggestTuple = 0;
		// int smallestLevelMember = 0;
		// int biggestLevelMember = 0;
		// try {
		// // Initialize
		// smallestTuple = Integer.parseInt(position.get(0)
		// .getUniqueName());
		// biggestTuple = smallestTuple;
		// // Search for the smallest and the tallest member of
		// // slicerTuple
		// for (Member member : position) {
		//
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestTuple) {
		// smallestTuple = x;
		// }
		// if (x > biggestTuple) {
		// biggestTuple = x;
		// }
		// }
		// // Initialize
		// smallestLevelMember = smallestTuple;
		// biggestLevelMember = smallestTuple;
		// // Search for the smallest and the tallest member of all
		// // level
		// // members
		// try {
		// for (Member member : position.get(0).getLevel()
		// .getMembers()) {
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestLevelMember) {
		// smallestLevelMember = x;
		// }
		// if (x > biggestLevelMember) {
		// biggestLevelMember = x;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// } catch (NumberFormatException nFE) {
		// isInteger = false;
		// }
		//
		// boolean isRestricted = false;
		// try {
		// for (Member levelMember : position.get(0).getLevel()
		// .getMembers()) {
		// if (isInteger) {
		// // Then we do not need to do this
		// break;
		// }
		// boolean isContained = false;
		// for (Member slicerMember : position) {
		// // if any member is not contained in the
		// // slicerTuple, it is restricted
		// if (levelMember.getUniqueName().equals(
		// slicerMember.getUniqueName())) {
		// isContained = true;
		// break;
		// }
		// }
		// if (!isContained) {
		// isRestricted = true;
		// break;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // Now, we create the string that shall be put between filter.
		// String filterstring = "";
		// if (isInteger) {
		//
		// if (smallestTuple > smallestLevelMember) {
		// filterstring = "?" + slicerDimensionPropertyVariable
		// + " >= " + smallestTuple + " AND ";
		// }
		// if (biggestTuple < biggestLevelMember) {
		// filterstring += "?" + slicerDimensionPropertyVariable
		// + " <= " + biggestTuple;
		// }
		//
		// } else if (isRestricted) {
		// String[] slicerTupleArray = new String[position.size()];
		//
		// for (int j = 0; j < slicerTupleArray.length; j++) {
		//
		// Member member = position.get(j);
		// String memberString = LdOlap4jUtil
		// .convertMDXtoURI(member.getUniqueName());
		//
		// // Check whether member is uri or literal value
		// if (memberString.startsWith("http://")) {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = <"
		// + memberString + "> ";
		// } else {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = \""
		// + memberString + "\" ";
		// }
		// slicerTupleArray[j] = memberString;
		// }
		//
		// filterstring = LdOlap4jUtil.implodeArray(slicerTupleArray,
		// " || ");
		// } else {
		// // Nothing to do, there is nothing to filter for.
		// break;
		// }
		//
		// // Only if this kind of level has not been selected, yet, we
		// // need to do it.
		// if (levelHeightMap.containsKey(slicerDimensionProperty
		// .hashCode())) {
		// query += " FILTER(" + filterstring + ").";
		//
		// } else {
		//
		// // Since we are on a specific level, we need to add
		// // intermediary
		// // property-values
		// if (slicerLevelNumber == 0) {
		// query += "?obs <" + slicerDimensionProperty + "> ?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// } else {
		// // Level path will start with ?obs and end with
		// // slicerConcept
		// // // First, what dimension?
		// // If Measures, then do not filter (since the
		// // selectionpredicates also contain measures, since that was
		// // easier to do beforehand)
		// try {
		// if (position.get(0).getDimension().getDimensionType()
		// .equals(Dimension.Type.MEASURE)) {
		// continue;
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // We assume that all contained members are of the same
		// // dimension and the same level
		// String slicerDimensionProperty = LdOlap4jUtil
		// .convertMDXtoURI(position.get(0).getDimension()
		// .getUniqueName());
		// String slicerDimensionPropertyVariable = makeParameter(position
		// .get(0).getDimension().getUniqueName());
		//
		// /*
		// * slicerLevelNumber would give me the difference between the
		// * maximal depth of all levels and the level depth (presumably
		// * the same for all tuples). This tells us the number of
		// * property-value pairs from the most granular element.
		// */
		// Integer slicerLevelNumber = (position.get(0).getLevel()
		// .getHierarchy().getLevels().size() - 1 - position
		// .get(0).getLevel().getDepth());
		//
		// /*
		// * Here, instead of going through all the members and filter for
		// * them one by one, we need to "compress" the filter expression
		// * somehow.
		// */
		// // Check whether the leveltype is integer and define smallest
		// // and biggest
		// boolean isInteger = true;
		// int smallestTuple = 0;
		// int biggestTuple = 0;
		// int smallestLevelMember = 0;
		// int biggestLevelMember = 0;
		// try {
		// // Initialize
		// smallestTuple = Integer.parseInt(position.get(0)
		// .getUniqueName());
		// biggestTuple = smallestTuple;
		// // Search for the smallest and the tallest member of
		// // slicerTuple
		// for (Member member : position) {
		//
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestTuple) {
		// smallestTuple = x;
		// }
		// if (x > biggestTuple) {
		// biggestTuple = x;
		// }
		// }
		// // Initialize
		// smallestLevelMember = smallestTuple;
		// biggestLevelMember = smallestTuple;
		// // Search for the smallest and the tallest member of all
		// // level
		// // members
		// try {
		// for (Member member : position.get(0).getLevel()
		// .getMembers()) {
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestLevelMember) {
		// smallestLevelMember = x;
		// }
		// if (x > biggestLevelMember) {
		// biggestLevelMember = x;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// } catch (NumberFormatException nFE) {
		// isInteger = false;
		// }
		//
		// boolean isRestricted = false;
		// try {
		// for (Member levelMember : position.get(0).getLevel()
		// .getMembers()) {
		// if (isInteger) {
		// // Then we do not need to do this
		// break;
		// }
		// boolean isContained = false;
		// for (Member slicerMember : position) {
		// // if any member is not contained in the
		// // slicerTuple, it is restricted
		// if (levelMember.getUniqueName().equals(
		// slicerMember.getUniqueName())) {
		// isContained = true;
		// break;
		// }
		// }
		// if (!isContained) {
		// isRestricted = true;
		// break;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // Now, we create the string that shall be put between filter.
		// String filterstring = "";
		// if (isInteger) {
		//
		// if (smallestTuple > smallestLevelMember) {
		// filterstring = "?" + slicerDimensionPropertyVariable
		// + " >= " + smallestTuple + " AND ";
		// }
		// if (biggestTuple < biggestLevelMember) {
		// filterstring += "?" + slicerDimensionPropertyVariable
		// + " <= " + biggestTuple;
		// }
		//
		// } else if (isRestricted) {
		// String[] slicerTupleArray = new String[position.size()];
		//
		// for (int j = 0; j < slicerTupleArray.length; j++) {
		//
		// Member member = position.get(j);
		// String memberString = LdOlap4jUtil
		// .convertMDXtoURI(member.getUniqueName());
		//
		// // Check whether member is uri or literal value
		// if (memberString.startsWith("http://")) {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = <"
		// + memberString + "> ";
		// } else {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = \""
		// + memberString + "\" ";
		// }
		// slicerTupleArray[j] = memberString;
		// }
		//
		// filterstring = LdOlap4jUtil.implodeArray(slicerTupleArray,
		// " || ");
		// } else {
		// // Nothing to do, there is nothing to filter for.
		// break;
		// }
		//
		// // Only if this kind of level has not been selected, yet, we
		// // need to do it.
		// if (levelHeightMap.containsKey(slicerDimensionProperty
		// .hashCode())) {
		// query += " FILTER(" + filterstring + ").";
		//
		// } else {
		//
		// // Since we are on a specific level, we need to add
		// // intermediary
		// // property-values
		// if (slicerLevelNumber == 0) {
		// query += "?obs <" + slicerDimensionProperty + "> ?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// } else {
		// // Level path will start with ?obs and end with
		// // slicerConcept
		// String levelPath = "";
		//
		// for (int i = 0; i < slicerLevelNumber; i++) {
		// levelPath += " ?" + slicerDimensionPropertyVariable
		// + i + ". ?"
		// + slicerDimensionPropertyVariable + i
		// + " skos:narrower ";
		// }
		//
		// query += "?obs <" + slicerDimensionProperty + "> "
		// + levelPath + "?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// }
		// }
		// }
		// } String levelPath = "";
		//
		// for (int i = 0; i < slicerLevelNumber; i++) {
		// levelPath += " ?" + slicerDimensionPropertyVariable
		// + i + ". ?"
		// + slicerDimensionPropertyVariable + i
		// + " skos:narrower ";
		// }
		//
		// query += "?obs <" + slicerDimensionProperty + "> "
		// + levelPath + "?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// }
		// }
		// }
		// }

		// Now, for each measure, we create a measure value column.
		// Any measure should be contained only once, unless calculated measures
		HashMap<Integer, Boolean> measureMap = new HashMap<Integer, Boolean>();

		for (Measure measure : projections) {

			// For formulas, this is different
			if (measure.getAggregator().equals(Measure.Aggregator.CALCULATED)) {

				/*
				 * For now, hard coded. Here, I also partly evaluate a query
				 * string, thus, I could do the same with filters.
				 */
				Measure measure1 = (Measure) ((MemberNode) ((CallNode) measure
						.getExpression()).getArgList().get(0)).getMember();
				Measure measure2 = (Measure) ((MemberNode) ((CallNode) measure
						.getExpression()).getArgList().get(1)).getMember();

				String operatorName = ((CallNode) measure.getExpression())
						.getOperatorName();

				// Measure
				// Measure property has aggregator attached to it " avg".
				String measureProperty1 = Olap4ldLinkedDataUtil
						.convertMDXtoURI(measure1.getUniqueName()).replace(
								" " + measure.getAggregator().name(), "");
				// We do not encode the aggregation function in the measure, any
				// more.
				String measurePropertyVariable1 = makeUriToParameter(measure1
						.getUniqueName());

				String measureProperty2 = Olap4ldLinkedDataUtil
						.convertMDXtoURI(measure2.getUniqueName()).replace(
								" " + measure.getAggregator().name(), "");
				String measurePropertyVariable2 = makeUriToParameter(measure2
						.getUniqueName());

				// We take the aggregator from the measure
				selectClause += " " + measure1.getAggregator().name() + "(?"
						+ measurePropertyVariable1 + " " + operatorName + " "
						+ "?" + measurePropertyVariable2 + ")";

				// I have to select them only, if we haven't inserted any
				// before.
				if (!measureMap.containsKey(measureProperty1.hashCode())) {
					whereClause += "?obs <" + measureProperty1 + "> ?"
							+ measurePropertyVariable1 + ". ";
					measureMap.put(measureProperty1.hashCode(), true);
				}
				if (!measureMap.containsKey(measureProperty2.hashCode())) {
					whereClause += "?obs <" + measureProperty2 + "> ?"
							+ measurePropertyVariable2 + ". ";
					measureMap.put(measureProperty2.hashCode(), true);
				}

			} else {

				// Measure
				String measureProperty = Olap4ldLinkedDataUtil.convertMDXtoURI(
						measure.getUniqueName()).replace(
						" " + measure.getAggregator().name(), "");
				String measurePropertyVariable = makeUriToParameter(measure
						.getUniqueName());

				// We take the aggregator from the measure
				// Since we use OPTIONAL, there might be empty columns, which is
				// why we need
				// to convert them to decimal.
				selectClause += " " + measure.getAggregator().name()
						+ "(xsd:decimal(?" + measurePropertyVariable + "))";

				// Optional clause is used for the case that several measures
				// are selected.
				if (!measureMap.containsKey(measureProperty.hashCode())) {
					whereClause += "OPTIONAL { ?obs <" + measureProperty
							+ "> ?" + measurePropertyVariable + ". }";
					measureMap.put(measureProperty.hashCode(), true);
				}
			}
		}

		// Now that we consider DSD in queries, we need to consider DSD
		// locations
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes() + "select "
				+ selectClause + askForFrom(false) + askForFrom(true)
				+ "where { " + whereClause + "}" + groupByClause
				+ orderByClause;

		return sparql(query, true);
	}

	/**
	 * This method returns graph patterns for a property path for levels.
	 * 
	 * @param levelStart
	 *            startingPoint of level
	 * @param levelHeight
	 *            endPoint of level
	 * @param dimensionProperty
	 * @param levelURI
	 * @return
	 */
	private String addLevelPropertyPath(Integer levelStart,
			Integer levelHeight, String dimensionProperty, String levelURI) {

		String whereClause = "";

		String dimensionPropertyVariable = makeUriToParameter(dimensionProperty);

		if (levelHeight == 0) {
			/*
			 * For now, we simply query for all and later match it to the pivot
			 * table. A more performant way would be to filter only for those
			 * dimension members, that are mentioned by any position. Should be
			 * easy possible to do that if we simply ask each position for the
			 * i'th position member.
			 */
			whereClause += "?obs <" + dimensionProperty + "> ?"
					+ dimensionPropertyVariable + levelHeight + ". ";

			// If level height is 0, it could be that no level is existing which
			// is why we leave that
			// pattern out.

		} else {

			// Level path will start with ?obs and end with
			// slicerConcept
			String levelPath = "";

			for (int i = levelStart; i < levelHeight; i++) {
				levelPath += " ?" + dimensionPropertyVariable + i + ". ?"
						+ dimensionPropertyVariable + i + " skos:narrower ";
			}

			whereClause += "?obs <" + dimensionProperty + "> " + levelPath
					+ "?" + dimensionPropertyVariable + levelHeight + ". ";

			// And this concept needs to be contained in the level.
			whereClause += "?" + dimensionPropertyVariable + levelHeight
					+ " skos:member <" + levelURI + ">. ";
			// Removed part of hasTopConcept, since we know the members
			// already.

		}

		return whereClause;

	}

	/**
	 * SPARQL does not allow all characters as parameter names, e.g.,
	 * ?sdmx-measure:obsValue. Therefore, we transform the URI representation
	 * into a parameter.
	 * 
	 * @param uriRepresentation
	 * @return
	 */
	private String makeUriToParameter(String uriRepresentation) {
		// We simply remove all special characters
		uriRepresentation = uriRepresentation.replaceAll("[^a-zA-Z0-9]+", "");
		return uriRepresentation;
	}

	@Override
	public List<Node[]> executeOlapQuery(LogicalOlapQueryPlan queryplan) {
		// XXX: Will not work, since OV uses different syntax than Sesame
		Olap2SparqlSesameVisitor r2a = new Olap2SparqlSesameVisitor(
				null);

		PhysicalOlapIterator newRoot;
		try {
			newRoot = (PhysicalOlapIterator) queryplan.visitAll(r2a);

			Olap4ldUtil._log.info("Physical query plan: " + newRoot);

			PhysicalOlapQueryPlan ap = new PhysicalOlapQueryPlan(newRoot);

			PhysicalOlapIterator resultIterator = ap.getIterator();

			List<Node[]> result = new ArrayList<Node[]>();
			while (resultIterator.hasNext()) {
				Object nextObject = resultIterator.next();
				// Will be Node[]
				Node[] node = (Node[]) nextObject;
				result.add(node);
			}
			return result;

		} catch (QueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void rollback() {
		;
	}

	public static void main(String[] args) {
		try {

			List<Node[]> myBindings = new ArrayList<Node[]>();

			String query = "prefix surgipedia: <http://surgipedia.sfb125.de/wiki/Special:URIResolver/> SELECT * WHERE { ?opphase a surgipedia:Category-3AOperation_Phase . ?opphase rdfs:label ?label.} LIMIT 100 ";

			String querysuffix = "?query=" + URLEncoder.encode(query, "UTF-8");

			String serverurl = "http://aifb-ls3-vm2.aifb.kit.edu:8890/sparql";

			String fullurl = serverurl + querysuffix;

			HttpURLConnection con = (HttpURLConnection) new URL(fullurl)
					.openConnection();

			con.setRequestProperty("Accept", "text/plain");
			// con.setRequestProperty("Accept", "text/n3");
			//con.setRequestProperty("Accept", "application/sparql-results+xml");
			con.setRequestMethod("POST");

			if (con.getResponseCode() != 200) {
				throw new RuntimeException("lookup on " + fullurl
						+ " resulted HTTP in status code "
						+ con.getResponseCode());
			}
			
			InputStream xml = con.getInputStream();

//			// Transform sparql xml to nx
//			javax.xml.transform.TransformerFactory tf = javax.xml.transform.TransformerFactory
//					.newInstance("net.sf.saxon.TransformerFactoryImpl", Thread
//							.currentThread().getContextClassLoader());
//
//			Transformer t;
//
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//			t = tf.newTransformer(new StreamSource(Olap4ldLinkedDataUtil.class
//					.getResourceAsStream("/xml2nx.xsl")));
//
//			StreamSource ssource = new StreamSource(xml);
//			StreamResult sresult = new StreamResult(baos);
//
//			t.transform(ssource, sresult);
//
//			// We need to make InputStream out of OutputStream
//			ByteArrayInputStream nx = new ByteArrayInputStream(
//					baos.toByteArray());
//
//			// String test2 = Olap4ldLinkedDataUtil.convertStreamToString(nx);
//
//			nx.reset();
//
//			NxParser nxp = new NxParser(nx);
			
			NxParser nxp = new NxParser(xml);

			Node[] nxx;
			while (nxp.hasNext()) {
				System.out.println();
				try {
					nxx = nxp.next();
					myBindings.add(nxx);
					for (int i = 0; i < nxx.length; i++) {
						Node node = nxx[i];
						System.out.print(node.toString() + "; ");
					}
				} catch (Exception e) {
					System.out.println("Could not parse properly.");
				}
				;
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public PhysicalOlapQueryPlan getExecplan(LogicalOlapQueryPlan queryplan)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}
}
