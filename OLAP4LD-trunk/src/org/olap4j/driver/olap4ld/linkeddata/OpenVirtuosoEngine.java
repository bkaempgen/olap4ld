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
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * The OpenVirtuosoEngine uses an Open Virtuoso SPARQL endpoint for executing
 * metadata or olap queries.
 * 
 * @author b-kaempgen
 * 
 */
public class OpenVirtuosoEngine implements LinkedDataCubesEngine {

	/**
	 * The type of SPARQL endpoint should be found out automatically and with
	 * the server string
	 */
	private static String SPARQLSERVERURL;

	/*
	 * Open Virtuoso is default triple store
	 */
	private int SPARQLSERVERTYPE = 2;
	private static final int QCRUMB = 1;
	private static final int OPENVIRTUOSO = 2;
	private static final int OPENVIRTUOSORULESET = 3;

	// Meta data attributes
	private static final String DATASOURCEDESCRIPTION = "OLAP data from the statistical Linked Data cloud.";

	private static final String PROVIDERNAME = "The community.";

	private static String URL;

	private static final String DATASOURCEINFO = "Data following the Linked Data principles.";

	private static final String TABLE_CAT = "LdCatalogSchema";

	private static final String TABLE_SCHEM = "LdCatalogSchema";

	public String DATASOURCENAME;

	public String DATASOURCEVERSION;

	// Each typical sparql query assumes the following prefixes.
	public String TYPICALPREFIXES = "PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#> PREFIX skos:    <http://www.w3.org/2004/02/skos/core#> PREFIX qb:      <http://purl.org/linked-data/cube#> PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#> PREFIX owl:     <http://www.w3.org/2002/07/owl#> ";

	// Helper attributes

	/**
	 * Map of locations that have been loaded into the embedded triple store.
	 */
	private HashMap<Integer, Boolean> loadedMap = new HashMap<Integer, Boolean>();

	private Integer MAX_LOAD_TRIPLE_SIZE = 1000000000;

	private Integer MAX_COMPLEX_CONSTRAINTS_TRIPLE_SIZE = 5000;

	private Integer LOADED_TRIPLE_SIZE = 0;

	private PhysicalOlapQueryPlan execplan;

	private HashMap<Integer, List<Node[]>> sparqlResultMap;

	// Not needed any more since we use materialisation.
	// private List<List<Node>> equivalenceList;

	public OpenVirtuosoEngine(URL serverUrlObject,
			List<String> datastructuredefinitions, List<String> datasets,
			String databasename) throws OlapException {

		// That is the SPARQL engine URL
		URL = serverUrlObject.toString();

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
		SPARQLSERVERURL = URL.toString();
		initialize();
	}

	private PhysicalOlapQueryPlan createExecplan(LogicalOlapQueryPlan queryplan)
			throws OlapException {

		/*
		 * Currently, I distinguish:
		 * 
		 * Drill-Across for queries with Drill-Across at the top, OLAP-to-SPARQL
		 * queries below.
		 * 
		 * DerivedDataset for queries with Convert-Cube or BaseCubeOp.
		 * 
		 * OLAP-to-SPARQL for queries with OLAP-to-SPARQL queries
		 * 
		 * I want to have Drill-Across for queries with Drill-Across at the top,
		 * OLAP-to-SPARQL queries below, and Convert-Cube or Merge-Cubes at the
		 * end.
		 */

		LogicalToPhysical logicaltophysical = new LogicalToPhysical(this);

		PhysicalOlapIterator newRoot;
		// Transform into physical query plan
		newRoot = (PhysicalOlapIterator) logicaltophysical
				.compile(queryplan._root);
		PhysicalOlapQueryPlan execplan = new PhysicalOlapQueryPlan(newRoot);

		return execplan;
	}

	public PhysicalOlapQueryPlan getExecplan() {
		return this.execplan;
	}

	/**
	 * For OPENVIRTUOSO, no specific initialisation is needed.
	 */
	private void initialize() {
		;
	}

	/**
	 * We now implement the pre-processing pipeline that shall result in a fully
	 * integrated database (triple store, data warehouse). (Cal, A., Calvanese,
	 * D., Giacomo, G. De, & Lenzerini, M. (2002). Data Integration under
	 * Integrity Constraints, 262–279.)
	 * 
	 * @throws
	 * @throws OlapException
	 */
	private void preload() throws OlapException {
		// Not needed here.
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
	@Deprecated
	private String askForFrom(boolean isDsdQuery) {
		return "";
	}

	public void executeSparqlConstructQuery(String constructquery) {
		// Not needed.
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
	 *            (not used)
	 * @return
	 */
	public List<Node[]> executeSparqlSelectQuery(String query, boolean caching) {

		Olap4ldUtil._log.config("SPARQL query: " + query);
	
		// XXX: Caching false
		caching = false;

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
			//result = sparqlQcrumb(query);
			result = null;
			break;
		case OPENVIRTUOSO:
			result = sparqlOpenVirtuoso(query);
			break;
		case OPENVIRTUOSORULESET:
			//result = sparqlOpenVirtuosoRuleset(query);
			result = null;
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

	public boolean isLoaded(URL resource) {

		if (loadedMap.get(resource.toString().hashCode()) != null
				&& loadedMap.get(resource.toString().hashCode()) == true) {

			Olap4ldUtil._log.info("Is loaded: " + resource.toString()
					+ ", Hash: " + resource.toString().hashCode());

			return true;
		} else {

			Olap4ldUtil._log.info("Is not yet loaded: " + resource.toString()
					+ ", Hash: " + resource.toString().hashCode());

			return false;
		}
	}

	public void setLoaded(URL resource) {
		Olap4ldUtil._log.info("Set loaded: " + resource.toString() + ", Hash: "
				+ resource.toString().hashCode());

		loadedMap.put(resource.toString().hashCode(), true);
	}

	/**
	 * Loads resource in store if 1) URI and location of resource not already
	 * loaded 2) number of triples has not reached maximum.
	 * 
	 * @param location
	 * @throws OlapException
	 */
	private void loadInStore(URL noninformationuri) throws OlapException {
		// Not needed.
	}

	/**
	 * We load all data for a cube. We also normalise and do integrity checks.
	 * 
	 * @param location
	 */
	private void loadCube(URL noninformationuri) throws OlapException {
		// Not needed.
	}

	public static List<ReconciliationCorrespondence> getReconciliationCorrespondences(
			boolean askForMergeCorrespondences) {

		List<ReconciliationCorrespondence> correspondences = new ArrayList<ReconciliationCorrespondence>();

		// // MIO2EUR
		// List<Node[]> mio_eur2eur_inputmembers = new ArrayList<Node[]>();
		// mio_eur2eur_inputmembers
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#unit"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR") });
		//
		// mio_eur2eur_inputmembers.add(new Node[] { new
		//
		// Resource("http://purl.org/linked-data/sdmx/2009/measure#obsValue"),
		// new Variable("value1") });
		//
		// List<Node[]> mio_eur2eur_outputmembers = new ArrayList<Node[]>();
		// mio_eur2eur_outputmembers.add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#unit"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/unit#EUR") });
		// mio_eur2eur_outputmembers
		// .add(new Node[] {
		// new Variable("outputcube"),
		// new
		//
		// Resource(
		// "http://purl.org/linked-data/sdmx/2009/measure#obsValue"),
		// new Variable("value2") });
		//
		// String mio_eur2eur_function = "(1000000 * x)";
		//
		// ReconciliationCorrespondence mio_eur2eur_correspondence = new
		// ReconciliationCorrespondence(
		// "MIO2EUR", mio_eur2eur_inputmembers, null,
		// mio_eur2eur_outputmembers, mio_eur2eur_function);
		// if (!askForMergeCorrespondences) {
		// correspondences.add(mio_eur2eur_correspondence);
		// }
		//
		// // COMPUTE_GDP
		//
		// List<Node[]> computegdp_inputmembers1 = new ArrayList<Node[]>();
		// computegdp_inputmembers1
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/indic_na#B1G") });
		//
		// List<Node[]> computegdp_inputmembers2 = new ArrayList<Node[]>();
		// computegdp_inputmembers2
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/indic_na#D21_M_D31") });
		//
		// List<Node[]> computegdp_outputmembers = new ArrayList<Node[]>();
		// computegdp_outputmembers
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/indic_na#NGDP") });
		//
		// String computegdp_function = "(x1 + x2)";
		//
		// ReconciliationCorrespondence computegdp_correspondence = new
		// ReconciliationCorrespondence(
		// "COMP_GDP", computegdp_inputmembers1, computegdp_inputmembers2,
		// computegdp_outputmembers, computegdp_function);
		// if (askForMergeCorrespondences) {
		// correspondences.add(computegdp_correspondence);
		// }
		//
		// // COMPUTE_GDP_PER_CAPITA
		//
		// List<Node[]> computegdppercapita_inputmembers1 = new
		// ArrayList<Node[]>();
		// computegdppercapita_inputmembers1
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/indic_na#NGDP") });
		// computegdppercapita_inputmembers1.add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#unit"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/unit#EUR") });
		//
		// List<Node[]> computegdppercapita_inputmembers2 = new
		// ArrayList<Node[]>();
		// computegdppercapita_inputmembers2
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#sex"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/sex#T") });
		// computegdppercapita_inputmembers2
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#age"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/age#TOTAL") });
		//
		// List<Node[]> computegdppercapita_outputmembers = new
		// ArrayList<Node[]>();
		// computegdppercapita_outputmembers
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/indic_na#NGDPH") });
		// computegdppercapita_outputmembers
		// .add(new Node[] {
		// new Resource(
		// "http://ontologycentral.com/2009/01/eurostat/ns#unit"),
		// new Resource(
		// "http://estatwrap.ontologycentral.com/dic/unit#EUR_HAB") });
		//
		// String computegdppercapita_function = "(x1 / x2)";
		//
		// ReconciliationCorrespondence computegdppercapita_correspondence = new
		// ReconciliationCorrespondence(
		// "COMP_GDP_CAP", computegdppercapita_inputmembers1,
		// computegdppercapita_inputmembers2,
		// computegdppercapita_outputmembers, computegdppercapita_function);
		// if (askForMergeCorrespondences) {
		// correspondences.add(computegdppercapita_correspondence);
		// }

		// COMPUTE_YES
		// ReconciliationCorrespondence computeyes_correspondence;
		// List<Node[]> computeyes_inputmembers1 = new ArrayList<Node[]>();
		// computeyes_inputmembers1
		// .add(new Node[] {
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#variable"),
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/variable.rdf#v590_2") });
		//
		// List<Node[]> computeyes_inputmembers2 = new ArrayList<Node[]>();
		// computeyes_inputmembers2
		// .add(new Node[] {
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#variable"),
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/variable.rdf#v590_3") });
		//
		// List<Node[]> computeyes_outputmembers = new ArrayList<Node[]>();
		// computeyes_outputmembers
		// .add(new Node[] {
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#variable"),
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/variable.rdf#v590_2+3") });
		//
		// String computeyes_function = "(x1 + x2)";
		//
		// computeyes_correspondence = new ReconciliationCorrespondence(
		// "COMP_YES", computeyes_inputmembers1, computeyes_inputmembers2,
		// computeyes_outputmembers, computeyes_function);
		//
		// if (askForMergeCorrespondences) {
		// correspondences.add(computeyes_correspondence);
		// }

		// COMPUTE\_PERCENTAGENOS
		// ReconciliationCorrespondence computepercentagenos_correspondence;
		// List<Node[]> computepercentagenos_inputmembers1 = new
		// ArrayList<Node[]>();
		// computepercentagenos_inputmembers1
		// .add(new Node[] {
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#variable"),
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/variable.rdf#v590_1") });
		//
		// List<Node[]> computepercentagenos_inputmembers2 = new
		// ArrayList<Node[]>();
		// computepercentagenos_inputmembers2
		// .add(new Node[] {
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#variable"),
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/variable.rdf#v590_2+3") });
		//
		// List<Node[]> computepercentagenos_outputmembers = new
		// ArrayList<Node[]>();
		// computepercentagenos_outputmembers
		// .add(new Node[] {
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#variable"),
		// new Resource(
		// "http://lod.gesis.org/lodpilot/ALLBUS/variable.rdf#v590_2+3") });
		// // Not yet needed since manual drill-across:
		// // computepercentagenos_outputmembers
		// // .add(new Node[] {
		// // new Resource(
		// // "http://ontologycentral.com/2009/01/eurostat/ns#indic_na"),
		// // new Resource(
		// // "http://estatwrap.ontologycentral.com/dic/indic_na#RGDPG") });
		//
		// String computepercentagenos_function = "(x1 / (x1 + x2))";
		//
		// computepercentagenos_correspondence = new
		// ReconciliationCorrespondence(
		// "COMP_PERCNOS", computepercentagenos_inputmembers1,
		// computepercentagenos_inputmembers2,
		// computepercentagenos_outputmembers,
		// computepercentagenos_function);
		//
		// if (askForMergeCorrespondences) {
		// correspondences.add(computepercentagenos_correspondence);
		// }

		return correspondences;
	}

	/**
	 * Check whether we query for "Measures".
	 * 
	 * @param dimensionUniqueName
	 * @param hierarchyUniqueName
	 * @param levelUniqueName
	 * @return
	 */
	private boolean isMeasureQueriedForExplicitly(Node dimensionUniqueName,
			Node hierarchyUniqueName, Node levelUniqueName) {
		// If one is set, it should not be Measures, not.
		// Watch out: no square brackets are needed.
		boolean explicitlyStated = (dimensionUniqueName != null && dimensionUniqueName
				.toString()
				.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME))
				|| (hierarchyUniqueName != null && hierarchyUniqueName
						.toString().equals(
								Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME))
				|| (levelUniqueName != null && levelUniqueName.toString()
						.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME));

		return explicitlyStated;

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
	 * This is both called for metadata queries and OLAP queries.
	 * 
	 * @return Node[]{}
	 */
	public List<Node[]> getCubes(Restrictions restrictions)
			throws OlapException {

		Olap4ldUtil._log.config("Linked Data Engine: Get Cubes...");

		// I once preloaded some data.
		// We now assume that we can also query for a global dataset identified
		// by a
		// comma separated list of datasets. For now, since we are interested in
		// all possible
		// derived datasets from a set of datasets.
		// Yet, we probably should either start with an MDX query or an Logical
		// Operator Tree
		// defining an information need. Although we might also just be
		// interested in all
		// possible derived datasets of a set of datasets.
		try {
			preload();
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<Node[]> result = new ArrayList<Node[]>();
		// Check whether Drill-across query
		// XXX: Wildcard delimiter
		if (restrictions.cubeNamePattern != null) {

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");

			Olap4ldUtil._log.info("Load dataset: " + datasets.length
					+ " datasets crawled.");

			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				Restrictions newrestrictions = new Restrictions();
				newrestrictions.cubeNamePattern = new Resource(dataset);

				List<Node[]> intermediaryresult = getCubesPerDataSet(newrestrictions);

				// Add to result
				boolean first = true;
				for (Node[] nodes : intermediaryresult) {
					if (first) {
						if (i == 0) {
							result.add(nodes);
						}
						first = false;
						continue;
					}
					// We do not want to have the single datasets returned.
					// We always have only one cube, the global cube.
					// result.add(nodes);
				}
			}

		} else {

			result = getCubesPerDataSet(restrictions);

		}

		/*
		 * Now that we have loaded all cube, we need to implement entity
		 * consolidation.
		 * 
		 * We create an equivalence table. Then, for each dimension unique name,
		 * we have one equivalence class. Then we can do as before.
		 */

		// List<Node[]> myresult = sparql(querytemplate, true);
		// // Add all of result2 to result
		// boolean first = true;
		// for (Node[] nodes : myresult) {
		// if (first) {
		// first = false;
		// continue;
		// }
		// result.add(nodes);
		// }

		// We do not do that anymore but use materialisation.
		// Now that we have loaded all data cubes, we can compute the
		// equivalence list.
		/*
		 * Load equivalence statements from triple store
		 */

		// Olap4ldUtil._log.info("Load dataset: create equivalence list started.");
		// long time = System.currentTimeMillis();
		//
		// List<Node[]> equivs = getEquivalenceStatements();
		//
		// this.equivalenceList = createEquivalenceList(equivs);
		//
		// time = System.currentTimeMillis() - time;
		// Olap4ldUtil._log
		// .info("Load dataset: create equivalence list finished in "
		// + time + "ms.");

		// Now, add "virtual cube"
		// ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?CUBE_TYPE ?CUBE_CAPTION
		// ?DESCRIPTION
		String globalcubename = "";
		if (restrictions.cubeNamePattern == null) {

			Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
					.getNodeResultFields(result.get(0));

			// Concatenate all cubes.
			boolean first = true;
			for (Node[] nodes : result) {
				if (first) {
					// First header;
					first = false;
					continue;
				}

				if (!globalcubename.equals("")) {
					globalcubename += ",";
				}
				globalcubename += nodes[cubemap.get("?CUBE_NAME")].toString();
			}

		} else {
			globalcubename = restrictions.cubeNamePattern.toString();
		}

		// XXX: The virtual cube should actually not be given to users. Users
		// simply issue queries over available datasets.
		Node[] virtualcube = new Node[] { new Literal(TABLE_CAT),
				new Literal(TABLE_SCHEM), new Resource(globalcubename),
				new Literal("CUBE"), new Literal("Global Cube"),
				new Literal("This is the global cube.") };
		result.add(virtualcube);

		Olap4ldUtil._log
				.info("Load datasets: Number of loaded triples for all datasets: "
						+ this.LOADED_TRIPLE_SIZE);

		// Check max loaded
		String query = "PREFIX qb: <http://purl.org/linked-data/cube#> select (count(?s) as ?count) where {?s qb:dataSet ?ds}";
		List<Node[]> countobservationsresult = executeSparqlSelectQuery(query, false);
		Integer countobservation = new Integer(
				countobservationsresult.get(1)[0].toString());

		Olap4ldUtil._log
				.info("Load datasets: Number of observations for all datasets: "
						+ countobservation);

		/*
		 * Check on restrictions that the interface makes:
		 * 
		 * Restrictions are strong restrictions, no fuzzy, since those wild
		 * cards have been eliminated before.
		 */
		// List<Node[]> result = applyRestrictions(cubeUris, restrictions);
		return result;

	}

	private List<Node[]> getCubesPerDataSet(Restrictions restrictions)
			throws OlapException {
		List<Node[]> result = new ArrayList<Node[]>();

		// Before loading, I should check first, whether already loaded.
		URL noninformationuri;

		try {
			if (restrictions.cubeNamePattern == null) {
				// There is nothing to load
				Olap4ldUtil._log
						.config("If no cubeNamePattern is given, we cannot load a cube.");
			} else {
				noninformationuri = new URL(
						restrictions.cubeNamePattern.toString());
				URL informationuri = Olap4ldLinkedDataUtil
						.askForLocation(noninformationuri);
				if (!isLoaded(noninformationuri) || !isLoaded(informationuri)) {

					// For now, we simply preload.
					loadCube(noninformationuri);

				}
				setLoaded(noninformationuri);
				setLoaded(informationuri);
			}

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String additionalFilters = createFilterForRestrictions(restrictions);

		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("sesame_getCubes_regular.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		result = executeSparqlSelectQuery(querytemplate, true);

		return result;
	}

	private void checkIntegrityConstraints() throws OlapException {
		// Possibly needed later.
	}

	/**
	 * According to QB specification, a cube may be provided in abbreviated form
	 * so that inferences first have to be materialised to properly query a
	 * cube.
	 * 
	 * @throws OlapException
	 */
	public void runNormalizationAlgorithm() throws OlapException {
		// Not needed.
	}

	/**
	 * Returns canonical for a node.
	 * 
	 * If none is found, simply the node is returned.
	 * 
	 * @param canonical
	 * @return
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private Node getCanonical(Node canonical) {
		// for (List<Node> equivalenceClass : equivalenceList) {
		// for (Node node : equivalenceClass) {
		//
		// if (node.equals(canonical)) {
		// canonical = equivalenceClass.get(0);
		// break;
		// }
		// }
		// }
		// return canonical;
		return null;
	}

	/**
	 * 
	 * @param equivs
	 *            - equiv[0] first same as and equiv[1] second same as entity.
	 * @return
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private List<List<Node>> createEquivalenceList(List<Node[]> equivs) {
		List<List<Node>> newequivalenceList = new ArrayList<List<Node>>();

		// HashMap<String, Integer> invertedindex = new HashMap<String,
		// Integer>();
		//
		// boolean first = true;
		// for (Node[] equiv : equivs) {
		//
		// // First is header
		// if (first) {
		// first = false;
		// continue;
		// }
		//
		// String A = equiv[0].toString();
		// String B = equiv[1].toString();
		//
		// // Store equiv
		//
		// // Find A
		// Integer rA = invertedindex.get(A);
		//
		// // Find B
		// Integer rB = invertedindex.get(B);
		//
		// if (rA == null && rB == null) {
		// // new row rAB
		//
		//
		// ArrayList<Node> newEquivalenceClass = new ArrayList<Node>();
		// newEquivalenceClass.add(A);
		// newEquivalenceClass.add(B);
		// newequivalenceList.add(newEquivalenceClass);
		//
		// } else if (rA != null && rB != null) {
		// // merge rA and rB
		//
		// // We create new list
		//
		// List<Node> rAB = new ArrayList<Node>();
		//
		// for (Node node : rB) {
		// rAB.add(node);
		// }
		// for (Node node : rA) {
		// rAB.add(node);
		// }
		// newequivalenceList.remove(rB);
		// newequivalenceList.remove(rA);
		// newequivalenceList.add(rAB);
		//
		// } else if (rA != null) {
		// // add B to rA
		//
		// rA.add(B);
		//
		// } else if (rB != null) {
		// // add A to rB
		//
		// rB.add(A);
		// }
		//
		// }

		return newequivalenceList;
	}

	@Deprecated
	private List<Node> getEquivalenceClassOfNode(Node resource) {
		// List<Node> equivalenceClass = new ArrayList<Node>();
		// equivalenceClass.add(resource);
		// for (List<Node> iterable_element : this.equivalenceList) {
		// for (Node node : iterable_element) {
		// if (resource.equals(node)) {
		// equivalenceClass = iterable_element;
		// break;
		// }
		// }
		// }
		// return equivalenceClass;
		return null;
	}

	@Deprecated
	public List<Node[]> getEquivalenceStatements() {

		// /*
		// * More directed better?
		// *
		// * {$this->getStandardPrefixes()} select ?same1 ?same2
		// * {$this->getStandardFrom()} where { ?dsd qb:component ?comp. ?comp
		// * ?componentProp ?same1. ?same1 owl:sameAs ?same2 }
		// */
		//
		// // Same as between dimensions and members.
		// String query =
		// "PREFIX owl: <http://www.w3.org/2002/07/owl#> PREFIX qb: <http://purl.org/linked-data/cube#> select ?same1 ?same2 where "
		// +
		// "{ { ?dsd qb:component ?comp. ?comp ?componentProp ?same1. ?same1 owl:sameAs ?same2 } UNION { ?obs a qb:Observation. ?obs ?dimension ?same1. ?same1 owl:sameAs ?same2 } }	";
		//
		// List<Node[]> myresult = sparql(query, true);
		//
		// Olap4ldUtil._log.info("Number of equivalence statements: "
		// + (myresult.size() - 1));
		//
		// return myresult;
		return null;
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
	public List<Node[]> getDimensions(Restrictions restrictions)
			throws OlapException {

		Olap4ldUtil._log.config("Linked Data Engine: Get Dimensions...");

		List<Node[]> result = new ArrayList<Node[]>();

		// Check whether Drill-across query
		// XXX: Wildcard delimiter
		if (restrictions.cubeNamePattern != null) {

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				Node saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = new Resource(dataset);

				List<Node[]> intermediaryresult = getDimensionsPerDataSet(restrictions);

				restrictions.cubeNamePattern = saverestrictioncubePattern;

				// Add to result
				boolean first = true;

				for (Node[] anIntermediaryresult : intermediaryresult) {
					if (first) {
						if (i == 0) {
							result.add(anIntermediaryresult);
						}
						first = false;
						continue;
					}

					// Add the single dimensions of the datasets to be
					// transformed with createGlobalDimensions.
					result.add(anIntermediaryresult);
				}
			}

		} else {
			result = getDimensionsPerDataSet(restrictions);
		}

		// Create global cube which is intersection of all dimensions and new
		// cube name
		return createGlobalDimensions(restrictions, result);
	}

	private List<Node[]> createGlobalDimensions(Restrictions restrictions,
			List<Node[]> intermediaryresult) {

		List<Node[]> result = new ArrayList<Node[]>();

		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(intermediaryresult.get(0));

		// Add to result
		boolean first = true;

		for (Node[] anIntermediaryresult : intermediaryresult) {

			if (first) {
				first = false;
				result.add(anIntermediaryresult);
				continue;
			}

			// Also add dimension to global cube

			Node[] newnode = new Node[9];
			newnode[dimensionmap.get("?CATALOG_NAME")] = anIntermediaryresult[dimensionmap
					.get("?CATALOG_NAME")];
			newnode[dimensionmap.get("?SCHEMA_NAME")] = anIntermediaryresult[dimensionmap
					.get("?SCHEMA_NAME")];
			// New cube name of global cube
			if (restrictions.cubeNamePattern == null) {
				newnode[dimensionmap.get("?CUBE_NAME")] = anIntermediaryresult[dimensionmap
						.get("?CUBE_NAME")];
			} else {
				newnode[dimensionmap.get("?CUBE_NAME")] = restrictions.cubeNamePattern;
			}

			newnode[dimensionmap.get("?DIMENSION_NAME")] = anIntermediaryresult[dimensionmap
					.get("?DIMENSION_NAME")];

			// Needs to be canonical name
			newnode[dimensionmap.get("?DIMENSION_UNIQUE_NAME")] = anIntermediaryresult[dimensionmap
					.get("?DIMENSION_UNIQUE_NAME")];

			newnode[dimensionmap.get("?DIMENSION_CAPTION")] = anIntermediaryresult[dimensionmap
					.get("?DIMENSION_CAPTION")];
			newnode[dimensionmap.get("?DIMENSION_ORDINAL")] = anIntermediaryresult[dimensionmap
					.get("?DIMENSION_ORDINAL")];
			newnode[dimensionmap.get("?DIMENSION_TYPE")] = anIntermediaryresult[dimensionmap
					.get("?DIMENSION_TYPE")];
			newnode[dimensionmap.get("?DESCRIPTION")] = anIntermediaryresult[dimensionmap
					.get("?DESCRIPTION")];

			// Only add if not already contained.
			boolean contained = false;
			for (Node[] aResult : result) {
				boolean sameDimension = aResult[dimensionmap
						.get("?DIMENSION_UNIQUE_NAME")].toString().equals(
						newnode[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
								.toString());
				boolean sameCube = aResult[dimensionmap.get("?CUBE_NAME")]
						.toString().equals(
								newnode[dimensionmap.get("?CUBE_NAME")]
										.toString());

				if (sameDimension && sameCube) {
					contained = true;
				}
			}

			if (!contained) {
				result.add(newnode);
			}
		}
		return result;
	}

	private List<Node[]> getDimensionsPerDataSet(Restrictions restrictions) {
		String additionalFilters = createFilterForRestrictions(restrictions);
		List<Node[]> result = new ArrayList<Node[]>();
		// Create header
		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?DIMENSION_NAME"),
				new Variable("?DIMENSION_UNIQUE_NAME"),
				new Variable("?DIMENSION_CAPTION"),
				new Variable("?DIMENSION_ORDINAL"),
				new Variable("?DIMENSION_TYPE"), new Variable("?DESCRIPTION") };
		result.add(header);

		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			// Get all dimensions
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getDimensions_regular.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);
			// Add all of result2 to result
			boolean first = true;
			for (Node[] anIntermediaryresult : myresult) {
				if (first) {
					first = false;
					continue;
				}

				result.add(anIntermediaryresult);
			}
		}

		// We try to find measures
		if (true) {

			// In this case, we do ask for a measure dimension.
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getDimensions_measure_dimension.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);

			// List<Node[]> result2 = applyRestrictions(memberUris2,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Use canonical identifier
		// result = replaceIdentifiersWithCanonical(result);

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
	public List<Node[]> getMeasures(Restrictions restrictions)
			throws OlapException {

		Olap4ldUtil._log.config("Linked Data Engine: Get Measures...");

		List<Node[]> result = new ArrayList<Node[]>();

		// Check whether Drill-across query
		// XXX: Wildcard delimiter
		if (restrictions.cubeNamePattern != null) {

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				Node saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = new Resource(dataset);

				List<Node[]> intermediaryresult = getMeasuresPerDataSet(restrictions);

				restrictions.cubeNamePattern = saverestrictioncubePattern;

				// Add to result
				boolean first = true;
				for (Node[] anIntermediaryresult : intermediaryresult) {
					if (first) {
						if (i == 0) {
							result.add(anIntermediaryresult);
						}
						first = false;
						continue;
					}

					// We do not want to have the single datasets returned.
					// result.add(anIntermediaryresult);

					// Also add measure to global cube
					Map<String, Integer> map = Olap4ldLinkedDataUtil
							.getNodeResultFields(intermediaryresult.get(0));

					Node[] newnode = new Node[10];
					newnode[map.get("?CATALOG_NAME")] = anIntermediaryresult[map
							.get("?CATALOG_NAME")];
					newnode[map.get("?SCHEMA_NAME")] = anIntermediaryresult[map
							.get("?SCHEMA_NAME")];
					newnode[map.get("?CUBE_NAME")] = restrictions.cubeNamePattern;
					newnode[map.get("?MEASURE_UNIQUE_NAME")] = anIntermediaryresult[map
							.get("?MEASURE_UNIQUE_NAME")];
					newnode[map.get("?MEASURE_NAME")] = anIntermediaryresult[map
							.get("?MEASURE_NAME")];
					newnode[map.get("?MEASURE_CAPTION")] = anIntermediaryresult[map
							.get("?MEASURE_CAPTION")];
					newnode[map.get("?DATA_TYPE")] = anIntermediaryresult[map
							.get("?DATA_TYPE")];
					newnode[map.get("?MEASURE_IS_VISIBLE")] = anIntermediaryresult[map
							.get("?MEASURE_IS_VISIBLE")];
					newnode[map.get("?MEASURE_AGGREGATOR")] = anIntermediaryresult[map
							.get("?MEASURE_AGGREGATOR")];
					newnode[map.get("?EXPRESSION")] = anIntermediaryresult[map
							.get("?EXPRESSION")];

					// Only add if not already contained.
					// For measures, we add them all.
					// boolean contained = false;
					// for (Node[] aResult : result) {
					// boolean sameDimension = aResult[map
					// .get("?MEASURE_UNIQUE_NAME")].toString()
					// .equals(newnode[map
					// .get("?MEASURE_UNIQUE_NAME")]
					// .toString());
					// boolean sameCube = aResult[map
					// .get("?CUBE_NAME")].toString().equals(
					// newnode[map.get("?CUBE_NAME")]
					// .toString());
					//
					// if (sameDimension && sameCube) {
					// contained = true;
					// }
					// }
					//
					// if (!contained) {
					// result.add(newnode);
					// }

					result.add(newnode);
				}

			}

		} else {

			result = getMeasuresPerDataSet(restrictions);

		}

		return result;

	}

	private List<Node[]> getMeasuresPerDataSet(Restrictions restrictions) {
		String additionalFilters = createFilterForRestrictions(restrictions);

		// ///////////QUERY//////////////////////////
		/*
		 * TODO: How to consider equal measures?
		 */

		// Boolean values need to be returned as "true" or "false".
		// Get all measures
		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("virtuoso_getMeasures.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> result = executeSparqlSelectQuery(querytemplate, true);

		// Here, we also include measures without aggregation function.
		// We have also added these measures as members to getMembers().
		querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("sesame_getMeasures_withoutimplicit.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> result2 = executeSparqlSelectQuery(querytemplate, true);

		// List<Node[]> result = applyRestrictions(measureUris, restrictions);

		// Add all of result2 to result
		boolean first = true;
		for (Node[] nodes : result2) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		// Use canonical identifier
		// result = replaceIdentifiersWithCanonical(result);

		return result;
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
	public List<Node[]> getHierarchies(Restrictions restrictions)
			throws OlapException {

		Olap4ldUtil._log.config("Linked Data Engine: Get Hierarchies...");

		List<Node[]> result = new ArrayList<Node[]>();

		// Check whether Drill-across query
		// XXX: Wildcard delimiter
		if (restrictions.cubeNamePattern != null) {

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				Node saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = new Resource(dataset);

				List<Node[]> intermediaryresult = getHierarchiesPerDataSet(restrictions);

				restrictions.cubeNamePattern = saverestrictioncubePattern;

				// Add to result
				boolean first = true;
				for (Node[] anIntermediaryresult : intermediaryresult) {
					if (first) {
						if (i == 0) {
							result.add(anIntermediaryresult);
						}
						first = false;
						continue;
					}

					// Add the single hierarchies of the datasets to be
					// transformed with createGlobalHierarchies.
					result.add(anIntermediaryresult);
				}

			}

		} else {

			result = getHierarchiesPerDataSet(restrictions);

		}

		// Create global hierarchies which is intersection of all hierarchies
		// and new
		// cube name
		return createGlobalHierarchies(restrictions, result);
	}

	private List<Node[]> createGlobalHierarchies(Restrictions restrictions,
			List<Node[]> intermediaryresult) {
		List<Node[]> result = new ArrayList<Node[]>();

		boolean first = true;
		for (Node[] anIntermediaryresult : intermediaryresult) {

			if (first) {
				first = false;
				result.add(anIntermediaryresult);
				continue;
			}

			// Also add hierarchy to global cube
			Map<String, Integer> hierarchymap = Olap4ldLinkedDataUtil
					.getNodeResultFields(intermediaryresult.get(0));

			Node[] newnode = new Node[9];
			newnode[hierarchymap.get("?CATALOG_NAME")] = anIntermediaryresult[hierarchymap
					.get("?CATALOG_NAME")];
			newnode[hierarchymap.get("?SCHEMA_NAME")] = anIntermediaryresult[hierarchymap
					.get("?SCHEMA_NAME")];

			// New cube name of global cube
			if (restrictions.cubeNamePattern == null) {
				newnode[hierarchymap.get("?CUBE_NAME")] = anIntermediaryresult[hierarchymap
						.get("?CUBE_NAME")];
			} else {
				newnode[hierarchymap.get("?CUBE_NAME")] = restrictions.cubeNamePattern;
			}
			newnode[hierarchymap.get("?DIMENSION_UNIQUE_NAME")] = anIntermediaryresult[hierarchymap
					.get("?DIMENSION_UNIQUE_NAME")];
			newnode[hierarchymap.get("?HIERARCHY_UNIQUE_NAME")] = anIntermediaryresult[hierarchymap
					.get("?HIERARCHY_UNIQUE_NAME")];
			newnode[hierarchymap.get("?HIERARCHY_NAME")] = anIntermediaryresult[hierarchymap
					.get("?HIERARCHY_NAME")];
			newnode[hierarchymap.get("?HIERARCHY_CAPTION")] = anIntermediaryresult[hierarchymap
					.get("?HIERARCHY_CAPTION")];
			newnode[hierarchymap.get("?DESCRIPTION")] = anIntermediaryresult[hierarchymap
					.get("?DESCRIPTION")];
			newnode[hierarchymap.get("?HIERARCHY_MAX_LEVEL_NUMBER")] = anIntermediaryresult[hierarchymap
					.get("?HIERARCHY_MAX_LEVEL_NUMBER")];

			// Only add if not already contained.
			boolean contained = false;
			for (Node[] aResult : result) {
				boolean sameDimension = aResult[hierarchymap
						.get("?DIMENSION_UNIQUE_NAME")].toString().equals(
						newnode[hierarchymap.get("?DIMENSION_UNIQUE_NAME")]
								.toString());
				boolean sameHierarchy = aResult[hierarchymap
						.get("?HIERARCHY_UNIQUE_NAME")].toString().equals(
						newnode[hierarchymap.get("?HIERARCHY_UNIQUE_NAME")]
								.toString());
				boolean sameCube = aResult[hierarchymap.get("?CUBE_NAME")]
						.toString().equals(
								newnode[hierarchymap.get("?CUBE_NAME")]
										.toString());

				if (sameDimension && sameHierarchy && sameCube) {
					contained = true;
				}
			}

			if (!contained) {
				result.add(newnode);
			}
		}
		return result;
	}

	private List<Node[]> getHierarchiesPerDataSet(Restrictions restrictions) {
		String additionalFilters = createFilterForRestrictions(restrictions);

		List<Node[]> result = new ArrayList<Node[]>();

		// Create header
		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?DIMENSION_UNIQUE_NAME"),
				new Variable("?HIERARCHY_UNIQUE_NAME"),
				new Variable("?HIERARCHY_NAME"),
				new Variable("?HIERARCHY_CAPTION"),
				new Variable("?DESCRIPTION"),
				new Variable("?HIERARCHY_MAX_LEVEL_NUMBER") };
		result.add(header);

		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			// Get all hierarchies with codeLists
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getHierarchies_regular.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);

			// Add all of result to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}

		}

		// List<Node[]> result = applyRestrictions(hierarchyResults,
		// restrictions);

		// Try to find measure dimensions.
		if (true) {

			// In this case, we do ask for a measure hierarchy.
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getHierarchies_measure_dimension.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);

			// List<Node[]> result2 = applyRestrictions(memberUris2,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Get dimension hierarchies without codeList, but only if hierarchy is
		// not set and different from dimension unique name
		/*
		 * * Note in spec:
		 * "Every dimension declared in a qb:DataStructureDefinition must have a declared rdfs:range."
		 * Note in spec:
		 * "Every dimension with range skos:Concept must have a qb:codeList." <=
		 * This means, we do not necessarily need a code list in many cases.
		 * But, if we have a code list, then: "If a dimension property has a
		 * qb:codeList, then the value of the dimension property on every
		 * qb:Observation must be in the code list."
		 */
		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getHierarchies_without_codelist.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);

			// List<Node[]> result3 = applyRestrictions(memberUris3,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Use canonical identifier
		// result = replaceIdentifiersWithCanonical(result);

		return result;
	}

	@Deprecated
	public List<Node[]> replaceIdentifiersWithCanonical(List<Node[]> result) {
		//
		// List<Node[]> newresult = new ArrayList<Node[]>();
		//
		// for (Node[] anIntermediaryresult : result) {
		// Node[] newnode = new Node[anIntermediaryresult.length];
		// for (int i = 0; i < anIntermediaryresult.length; i++) {
		// newnode[i] = getCanonical(anIntermediaryresult[i]);
		// }
		// newresult.add(newnode);
		// }
		//
		// return newresult;
		return null;
	}

	/**
	 * 
	 * @param context
	 * @param metadataRequest
	 * @param restrictions
	 * @return
	 */
	public List<Node[]> getLevels(Restrictions restrictions)
			throws OlapException {

		Olap4ldUtil._log.config("Linked Data Engine: Get Levels...");

		List<Node[]> result = new ArrayList<Node[]>();

		// Check whether Drill-across query
		// XXX: Wildcard delimiter
		if (restrictions.cubeNamePattern != null) {

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				Node saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = new Resource(dataset);

				List<Node[]> intermediaryresult = getLevelsPerDataSet(restrictions);

				restrictions.cubeNamePattern = saverestrictioncubePattern;

				// Add to result
				boolean first = true;
				for (Node[] anIntermediaryresult : intermediaryresult) {
					if (first) {
						if (i == 0) {
							result.add(anIntermediaryresult);
						}
						first = false;
						continue;
					}
					// We do not want to have the single datasets returned.
					// result.add(anIntermediaryresult);

					// Also add dimension to global cube
					Map<String, Integer> levelmap = Olap4ldLinkedDataUtil
							.getNodeResultFields(intermediaryresult.get(0));

					Node[] newnode = new Node[12];
					newnode[levelmap.get("?CATALOG_NAME")] = anIntermediaryresult[levelmap
							.get("?CATALOG_NAME")];
					newnode[levelmap.get("?SCHEMA_NAME")] = anIntermediaryresult[levelmap
							.get("?SCHEMA_NAME")];
					newnode[levelmap.get("?CUBE_NAME")] = restrictions.cubeNamePattern;
					newnode[levelmap.get("?DIMENSION_UNIQUE_NAME")] = anIntermediaryresult[levelmap
							.get("?DIMENSION_UNIQUE_NAME")];
					newnode[levelmap.get("?HIERARCHY_UNIQUE_NAME")] = anIntermediaryresult[levelmap
							.get("?HIERARCHY_UNIQUE_NAME")];
					newnode[levelmap.get("?LEVEL_UNIQUE_NAME")] = anIntermediaryresult[levelmap
							.get("?LEVEL_UNIQUE_NAME")];
					newnode[levelmap.get("?LEVEL_CAPTION")] = anIntermediaryresult[levelmap
							.get("?LEVEL_CAPTION")];
					newnode[levelmap.get("?LEVEL_NAME")] = anIntermediaryresult[levelmap
							.get("?LEVEL_NAME")];
					newnode[levelmap.get("?DESCRIPTION")] = anIntermediaryresult[levelmap
							.get("?DESCRIPTION")];
					newnode[levelmap.get("?LEVEL_NUMBER")] = anIntermediaryresult[levelmap
							.get("?LEVEL_NUMBER")];
					newnode[levelmap.get("?LEVEL_CARDINALITY")] = anIntermediaryresult[levelmap
							.get("?LEVEL_CARDINALITY")];
					newnode[levelmap.get("?LEVEL_TYPE")] = anIntermediaryresult[levelmap
							.get("?LEVEL_TYPE")];

					// Only add if not already contained.
					boolean contained = false;
					for (Node[] aResult : result) {
						boolean sameDimension = aResult[levelmap
								.get("?DIMENSION_UNIQUE_NAME")].toString()
								.equals(newnode[levelmap
										.get("?DIMENSION_UNIQUE_NAME")]
										.toString());

						boolean sameHierarchy = aResult[levelmap
								.get("?HIERARCHY_UNIQUE_NAME")].toString()
								.equals(newnode[levelmap
										.get("?HIERARCHY_UNIQUE_NAME")]
										.toString());

						boolean sameLevel = aResult[levelmap
								.get("?LEVEL_UNIQUE_NAME")].toString().equals(
								newnode[levelmap.get("?LEVEL_UNIQUE_NAME")]
										.toString());

						boolean sameCube = aResult[levelmap.get("?CUBE_NAME")]
								.toString().equals(
										newnode[levelmap.get("?CUBE_NAME")]
												.toString());

						if (sameDimension && sameHierarchy && sameLevel
								&& sameCube) {
							contained = true;
						}
					}

					if (!contained) {
						result.add(newnode);
					}
				}

			}

		} else {

			result = getLevelsPerDataSet(restrictions);

		}

		return result;
	}

	private List<Node[]> getLevelsPerDataSet(Restrictions restrictions) {
		String additionalFilters = createFilterForRestrictions(restrictions);

		List<Node[]> result = new ArrayList<Node[]>();

		// Create header
		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?DIMENSION_UNIQUE_NAME"),
				new Variable("?HIERARCHY_UNIQUE_NAME"),
				new Variable("?LEVEL_UNIQUE_NAME"),
				new Variable("?LEVEL_CAPTION"), new Variable("?LEVEL_NAME"),
				new Variable("?DESCRIPTION"), new Variable("?LEVEL_NUMBER"),
				new Variable("?LEVEL_CARDINALITY"), new Variable("?LEVEL_TYPE") };
		result.add(header);

		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			// TODO: Add regularly modeled levels (without using xkos)
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getLevels_regular.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);
			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}

			// Get all levels of code lists using xkos
			// TODO: LEVEL_CARDINALITY is not solved, yet.
			querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getLevels_xkos.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			myresult = executeSparqlSelectQuery(querytemplate, true);

			// Add all of result2 to result
			first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}

		}

		// Distinct for several measures per cube.
		// Add measures levels
		// Second, ask for the measures (which are also members), but only if
		// measure

		if (true) {

			// In this case, we do ask for a measure dimension.
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getLevels_measure_dimension.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);

			// List<Node[]> result2 = applyRestrictions(memberUris2,
			// restrictions);

			// Add all of result2 to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Add levels for dimensions without codelist, but only if hierarchy and
		// dimension names are equal
		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getLevels_without_codelist.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			// Second, ask for the measures (which are also members)
			List<Node[]> myresult = executeSparqlSelectQuery(querytemplate, true);

			// List<Node[]> result3 = applyRestrictions(memberUris3,
			// restrictions);

			// Add all of result3 to result
			boolean first = true;
			for (Node[] nodes : myresult) {
				if (first) {
					first = false;
					continue;
				}
				result.add(nodes);
			}
		}

		// Use canonical identifier
		// result = replaceIdentifiersWithCanonical(result);

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
	public List<Node[]> getMembers(Restrictions restrictions)
			throws OlapException {

		Olap4ldUtil._log.config("Linked Data Engine: Get Members...");

		List<Node[]> result = new ArrayList<Node[]>();

		// Check whether Drill-across query
		// XXX: Wildcard delimiter
		if (restrictions.cubeNamePattern != null) {

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				// XXX: Refactor: used at every getXXX()
				Node saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = new Resource(dataset);

				List<Node[]> intermediaryresult = getMembersPerDataSet(restrictions);

				restrictions.cubeNamePattern = saverestrictioncubePattern;

				// Add to result
				boolean first = true;
				for (Node[] anIntermediaryresult : intermediaryresult) {
					if (first) {
						if (i == 0) {
							result.add(anIntermediaryresult);
						}
						first = false;
						continue;
					}

					// We do not want to have the single datasets returned.
					// result.add(anIntermediaryresult);

					// Also add dimension to global cube
					Map<String, Integer> membermap = Olap4ldLinkedDataUtil
							.getNodeResultFields(intermediaryresult.get(0));

					Node[] newnode = new Node[13];
					newnode[membermap.get("?CATALOG_NAME")] = anIntermediaryresult[membermap
							.get("?CATALOG_NAME")];
					newnode[membermap.get("?SCHEMA_NAME")] = anIntermediaryresult[membermap
							.get("?SCHEMA_NAME")];
					newnode[membermap.get("?CUBE_NAME")] = restrictions.cubeNamePattern;
					newnode[membermap.get("?DIMENSION_UNIQUE_NAME")] = anIntermediaryresult[membermap
							.get("?DIMENSION_UNIQUE_NAME")];
					newnode[membermap.get("?HIERARCHY_UNIQUE_NAME")] = anIntermediaryresult[membermap
							.get("?HIERARCHY_UNIQUE_NAME")];
					newnode[membermap.get("?LEVEL_UNIQUE_NAME")] = anIntermediaryresult[membermap
							.get("?LEVEL_UNIQUE_NAME")];
					newnode[membermap.get("?LEVEL_NUMBER")] = anIntermediaryresult[membermap
							.get("?LEVEL_NUMBER")];
					newnode[membermap.get("?MEMBER_UNIQUE_NAME")] = anIntermediaryresult[membermap
							.get("?MEMBER_UNIQUE_NAME")];
					newnode[membermap.get("?MEMBER_NAME")] = anIntermediaryresult[membermap
							.get("?MEMBER_NAME")];
					newnode[membermap.get("?MEMBER_CAPTION")] = anIntermediaryresult[membermap
							.get("?MEMBER_CAPTION")];
					newnode[membermap.get("?MEMBER_TYPE")] = anIntermediaryresult[membermap
							.get("?MEMBER_TYPE")];
					newnode[membermap.get("?PARENT_UNIQUE_NAME")] = anIntermediaryresult[membermap
							.get("?PARENT_UNIQUE_NAME")];
					newnode[membermap.get("?PARENT_LEVEL")] = anIntermediaryresult[membermap
							.get("?PARENT_LEVEL")];

					// Only add if not already contained.
					boolean contained = false;
					for (Node[] aResult : result) {
						boolean sameDimension = aResult[membermap
								.get("?DIMENSION_UNIQUE_NAME")].toString()
								.equals(newnode[membermap
										.get("?DIMENSION_UNIQUE_NAME")]
										.toString());

						boolean sameHierarchy = aResult[membermap
								.get("?HIERARCHY_UNIQUE_NAME")].toString()
								.equals(newnode[membermap
										.get("?HIERARCHY_UNIQUE_NAME")]
										.toString());

						boolean sameLevel = aResult[membermap
								.get("?LEVEL_UNIQUE_NAME")].toString().equals(
								newnode[membermap.get("?LEVEL_UNIQUE_NAME")]
										.toString());

						boolean sameMember = aResult[membermap
								.get("?MEMBER_UNIQUE_NAME")].toString().equals(
								newnode[membermap.get("?MEMBER_UNIQUE_NAME")]
										.toString());
						boolean sameCube = aResult[membermap.get("?CUBE_NAME")]
								.toString().equals(
										newnode[membermap.get("?CUBE_NAME")]
												.toString());

						if (sameDimension && sameHierarchy && sameLevel
								&& sameMember && sameCube) {
							contained = true;
						}
					}

					if (!contained) {
						result.add(newnode);
					}
				}

			}

		} else {

			result = getMembersPerDataSet(restrictions);

		}

		return result;
	}

	private List<Node[]> getMembersPerDataSet(Restrictions restrictions) {
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
		if (true) {
			
			// XXX From now on, I return only one measure.
			
			intermediaryresult = getMeasureMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		// Regular members
		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			intermediaryresult = getHasTopConceptMembers(restrictions);

			addToResult(intermediaryresult, result);
		}

//		// Xkos members
//		// Watch out: No square brackets
//		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
//				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {
//
//			intermediaryresult = getXkosMembers(restrictions);
//
//			addToResult(intermediaryresult, result);
//
//		}
//
//		// If we still do not have members, then we might have degenerated
//		// members
//		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
//				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {
//			// Members without codeList
//			intermediaryresult = getDegeneratedMembers(restrictions);
//
//			addToResult(intermediaryresult, result);
//
//		}

		// Use canonical identifier
		// result = replaceIdentifiersWithCanonical(result);

		return result;
	}

	private List<Node[]> getMeasureMembers(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

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
							+ restrictions.cubeNamePattern + ">) ";
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

		}

		// Second, ask for the measures (which are also members)
		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("virtuoso_getMembers_measure_members.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> memberUris2 = executeSparqlSelectQuery(querytemplate, true);

		return memberUris2;
	}

	/**
	 * Finds specific typical members.
	 * 
	 * @param restrictions
	 * 
	 * @return
	 */
	private List<Node[]> getXkosMembers(Restrictions restrictions) {

		String additionalFilters = createFilterForRestrictions(restrictions);

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
						+ restrictions.memberUniqueName + ">) ";

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

		}

		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("sesame_getMembers_xkos.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> memberUris2 = executeSparqlSelectQuery(querytemplate, true);

		return memberUris2;
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

		String additionalFilters = createFilterForRestrictions(restrictions);

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

			// First, ask for all members
			// Get all members of hierarchies without levels, that simply
			// define
			// skos:hasTopConcept members with skos:notation.
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getMembers_topConcept.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					additionalFilters);

			List<Node[]> memberUris = executeSparqlSelectQuery(querytemplate, true);

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

		String additionalFilters = createFilterForRestrictions(restrictions);

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

		}

		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("sesame_getMembers_degenerated.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> memberUris1 = executeSparqlSelectQuery(querytemplate, true);

		return memberUris1;

	}

	@SuppressWarnings("unused")
	private boolean isResourceAndNotLiteral(String resource) {
		return resource.startsWith("http:");
	}

	private String createFilterForRestrictions(Restrictions restrictions) {

		String filter = "";
		// We need to create a filter for the specific restriction
		filter += (restrictions.cubeNamePattern != null) ? " FILTER (?CUBE_NAME = <"
				+ restrictions.cubeNamePattern + ">) "
				: "";

		if (restrictions.dimensionUniqueName != null
				&& !restrictions.dimensionUniqueName.toString().equals(
						Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
			// filter += " filter("
			// + createConditionConsiderEquivalences(
			// restrictions.dimensionUniqueName, new Variable(
			// "DIMENSION_UNIQUE_NAME")) + ") ";
			filter += " filter(str(?DIMENSION_UNIQUE_NAME) = \""
					+ restrictions.dimensionUniqueName + "\") ";
		}

		if (restrictions.hierarchyUniqueName != null
				&& !restrictions.hierarchyUniqueName.toString().equals(
						Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {

			// This we do since ranges may be blank nodes, e.g., of ical:dtend
			// XXX: Workaround
			if (restrictions.hierarchyUniqueName.toString().startsWith("node")) {
				filter += "";
			} else {
				// filter += " filter("
				// + createConditionConsiderEquivalences(
				// restrictions.hierarchyUniqueName, new Variable(
				// "HIERARCHY_UNIQUE_NAME")) + ") ";
				filter += " filter(str(?HIERARCHY_UNIQUE_NAME) = \""
						+ restrictions.hierarchyUniqueName + "\") ";
			}

		}

		if (restrictions.levelUniqueName != null
				&& !restrictions.levelUniqueName.toString().equals(
						Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {

			// This we do since ranges may be blank nodes, e.g., of ical:dtend
			// XXX: Workaround
			if (restrictions.hierarchyUniqueName != null
					&& restrictions.hierarchyUniqueName.toString().startsWith(
							"node")) {
				filter += "";
			} else {

				// filter += " filter("
				// + createConditionConsiderEquivalences(
				// restrictions.levelUniqueName, new Variable(
				// "LEVEL_UNIQUE_NAME")) + ") ";
				filter += " filter(str(?LEVEL_UNIQUE_NAME) = \""
						+ restrictions.levelUniqueName + "\") ";

			}

		}

		if (restrictions.memberUniqueName != null
				&& !restrictions.memberUniqueName.toString().equals(
						Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {

			// filter += " filter("
			// + createConditionConsiderEquivalences(
			// restrictions.memberUniqueName, new Variable(
			// "MEMBER_UNIQUE_NAME")) + ") ";
			filter += " filter(str(?MEMBER_UNIQUE_NAME) = \""
					+ restrictions.memberUniqueName + "\") ";
		}

		return filter;
	}

	/**
	 * This method creates a filter string for a Resource with a specific
	 * variable for all equivalences.
	 * 
	 * @param canonicalResource
	 * @param variableName
	 * @return
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private String createConditionConsiderEquivalences(Node canonicalResource,
			Variable variable) {

		List<Node> equivalenceClass = getEquivalenceClassOfNode(canonicalResource);

		// Since we sometimes manually build member names, we have to check
		// on strings
		String[] filterString = new String[equivalenceClass.size()];
		for (int i = 0; i < filterString.length; i++) {
			filterString[i] = "str(?" + variable + ") = \""
					+ equivalenceClass.get(i) + "\"";
		}

		return "(" + Olap4ldLinkedDataUtil.implodeArray(filterString, " || ")
				+ ")";
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

	@Override
	public List<Node[]> executeOlapQuery(LogicalOlapQueryPlan queryplan)
			throws OlapException {
		// Log logical query plan

		Olap4ldUtil._log.config("Logical query plan: " + queryplan.toString());

		Olap4ldUtil._log
				.info("Execute logical query plan: Generate physical query plan.");
		long time = System.currentTimeMillis();

		// Create physical query plan
		this.execplan = createExecplan(queryplan);

		Olap4ldUtil._log
				.info("Execute logical query plan: Physical query plan: "
						+ execplan.toString());

		time = System.currentTimeMillis() - time;
		Olap4ldUtil._log
				.info("Execute logical query plan: Generate physical query plan finished in "
						+ time + "ms.");

		Olap4ldUtil._log
				.info("Execute logical query plan: Execute physical query plan.");
		time = System.currentTimeMillis();
		PhysicalOlapIterator resultIterator = this.execplan.getIterator();

		/*
		 * We create our own List<Node[]> result with every item
		 * 
		 * Every Node[] contains for each dimension in the dimension list of the
		 * metadata a member and for each measure in the measure list a value.
		 */
		List<Node[]> result = new ArrayList<Node[]>();
		while (resultIterator.hasNext()) {
			Object nextObject = resultIterator.next();
			// Will be Node[]
			Node[] node = (Node[]) nextObject;
			result.add(node);
		}

		time = System.currentTimeMillis() - time;
		Olap4ldUtil._log
				.info("Execute logical query plan: Execute physical query plan finished in "
						+ time + "ms.");

		return result;
	}

	@Override
	public List<Node[]> executeOlapQuery(Cube cube, List<Level> slicesrollups,
			List<Position> dices, List<Measure> projections)
			throws OlapException {
		throw new UnsupportedOperationException(
				"Only LogicalOlapQuery trees can be executed!");
	}

	/**
	 * Empties store and locationMap.
	 */
	public void rollback() {
		initialize();
	}

}
