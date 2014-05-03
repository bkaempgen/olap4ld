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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
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
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * The EmbeddedSesameEngine manages an embedded Sesame repository (triple store)
 * while executing metadata or olap queries.
 * 
 * @author b-kaempgen
 * 
 */
public class EmbeddedSesameEngine implements LinkedDataCubesEngine {

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

	/**
	 * The Sesame repository (triple store). Gets filled when asking for cubes.
	 */
	private SailRepository repo;

	private Integer MAX_LOAD_TRIPLE_SIZE = 10000000;

	private Integer MAX_COMPLEX_CONSTRAINTS_TRIPLE_SIZE = 5000;

	private Integer LOADED_TRIPLE_SIZE = 0;

	private PhysicalOlapQueryPlan execplan;

	private List<List<Node>> equivalenceList;

	public EmbeddedSesameEngine(URL serverUrlObject,
			List<String> datastructuredefinitions, List<String> datasets,
			String databasename) throws OlapException {

		// We actually do not need that.
		URL = serverUrlObject.toString();

		if (databasename.equals("EMBEDDEDSESAME")) {
			DATASOURCENAME = databasename;
			DATASOURCEVERSION = "1.0";
		}
		initialize();
	}

	private void createExecplan(LogicalOlapQueryPlan queryplan)
			throws OlapException {

		try {
			LogicalOlapOperatorQueryPlanVisitor r2a;
			// Heuristics of how to select the right visitor.
			if (queryplan._root instanceof DrillAcrossOp) {
				r2a = new OlapDrillAcross2SparqlSesameVisitor(repo);
			} else if (queryplan._root instanceof ConvertCubeOp) {
				// We create visitor to translate logical into physical
				r2a = new Olap2SparqlSesameDerivedDatasetVisitor(repo);
			} else {
				r2a = new Olap2SparqlSesameVisitor(repo);
			}

			PhysicalOlapIterator newRoot;
			// Transform into physical query plan
			newRoot = (PhysicalOlapIterator) queryplan.visitAll(r2a);
			PhysicalOlapQueryPlan execplan = new PhysicalOlapQueryPlan(newRoot);

			this.execplan = execplan;

		} catch (QueryException e) {
			Olap4ldUtil._log.warning("Olap query execution went wrong: "
					+ e.getMessage());
			throw new OlapException("Olap query execution went wrong: "
					+ e.getMessage());
		}
	}

	public PhysicalOlapQueryPlan getExecplan() {
		return this.execplan;
	}

	private void initialize() {

		// This seems to hold up a lot. I hope garbage collector works.
		// TODO: Hopefully, we do not need to close the repo explicitly.
		if (this.repo != null) {
			try {
				this.repo.initialize();
				// this.repo.shutDown();
			} catch (RepositoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			this.repo = new SailRepository(new MemoryStore());
			try {
				repo.initialize();

				// do something interesting with the values here...
				// con.close();
			} catch (RepositoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// LoadedMap
		loadedMap.clear();
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
	@SuppressWarnings("unused")
	private void preload() throws OlapException {

		try {
			// First, we load everything that Data-Fu can create
			// loadInStore(new URL(
			// "http://127.0.0.1:8080/Data-Fu-Engine/data-fu/gdp_per_capita_experiment/triples"));

			// Then, we load everything that Data-Fu cannot create
			// We load all cubes
			long time = System.currentTimeMillis();

			// load and validate dataset requires to load cube
			URL dataset;

			// ----------------
			// Load "GDP per capita - annual Data" ds and dsd
			// URL dataset = new URL(
			// "http://estatwrap.ontologycentral.com/id/nama_aux_gph#ds");
			// Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);
			// Shortcut
			// loadInStore(new
			// URL("http://localhost:8080/Data-Fu-Engine/datasets/gdp_per_capita_experiment_load_cubes_nama_aux_gph_estatwrap.n3"));

			// # Gross Domestic Product (GDP) per capita in Purchasing Power
			// Standards (PPS)
			// dataset = new URL(
			// "http://olap4ld.googlecode.com/git/OLAP4LD-trunk/tests/estatwrap/tec00114_ds.rdf#ds");
			// Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);

			// ----------------
			// Load "GDP and main components - Current prices [nama_gdp_c]" ds
			// and dsd
			// dataset = new URL(
			// "http://estatwrap.ontologycentral.com/id/nama_gdp_c#ds");
			// Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);
			// Shortcut
			// loadInStore(new
			// URL("http://localhost:8080/Data-Fu-Engine/datasets/gdp_per_capita_experiment_load_cubes_nama_gdp_c_estatwrap.n3"));

			// ----------------
			// # Regional gross domestic product by NUTS 2 regions [tgs00003]
			// (Estatwrap)
			// <http://estatwrap.ontologycentral.com/id/tgs00003#ds> rdf:type
			// qb:DataSet.
			dataset = new URL(
					"http://estatwrap.ontologycentral.com/id/tgs00003#ds");
			Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);

			// ----------------
			// # Regional gross domestic product by NUTS 2 regions [tgs00003]
			// (Eurostat LD)
			// <http://eurostat.linked-statistics.org/data/tgs00003> rdf:type
			// qb:DataSet.

			// Problem: Eurostat LD provides wrong link between dataset and dsd:
			// http://eurostat.linked-statistics.org/../dsd/tgs00003. Thus, dsd
			// and everything else cannot be crawled, properly. Solution: I
			// manually add the triple beforehand.

			// Problem: dcterms:date could not be resolved.

			String triples = "<http://eurostat.linked-statistics.org/data/tgs00003> <http://purl.org/linked-data/cube#structure> <http://eurostat.linked-statistics.org/dsd/tgs00003>. "
					+ "<http://eurostat.linked-statistics.org/dsd/tgs00003> <http://purl.org/linked-data/cube#component> _:comp. "
					+ "_:comp <http://purl.org/linked-data/cube#measure> <http://purl.org/linked-data/sdmx/2009/measure#obsValue>. "
					+ "<http://purl.org/dc/terms/date> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Literal>. ";

			// insertTriples(triples);

			dataset = new URL(
					"http://eurostat.linked-statistics.org/data/tgs00003");
			Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);

			// Problem: Wrong dsd has to be removed
			triples = "<http://eurostat.linked-statistics.org/data/tgs00003> <http://purl.org/linked-data/cube#structure> <http://eurostat.linked-statistics.org/../dsd/tgs00003>. ";

			// deleteTriples(triples);

			triples = "<http://eurostat.linked-statistics.org/dsd/tgs00003> <http://purl.org/linked-data/cube#component> ?comp. "
					+ "?comp <http://purl.org/linked-data/cube#dimension> <http://purl.org/linked-data/sdmx/2009/measure#obsValue>. ";
			String where = "?comp <http://purl.org/linked-data/cube#dimension> <http://purl.org/linked-data/sdmx/2009/measure#obsValue>. ";
			// deleteTriplesWhere(triples, where);

			// ----------------
			// # Population on 1 January by age and sex [demo_pjan] (Estatwrap)
			// <http://estatwrap.ontologycentral.com/id/demo_pjan#ds> rdf:type
			// qb:DataSet.

			// Problem: demo_pjan contains errors
			// loadInStore(new
			// URL("http://localhost:8080/Data-Fu-Engine/datasets/demo_pjan_ds_v3.rdf"));

			dataset = new URL(
					"http://estatwrap.ontologycentral.com/id/demo_pjan#ds");
			Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);
			// Shortcut
			// loadInStore(new
			// URL("http://localhost:8080/Data-Fu-Engine/datasets/gdp_per_capita_experiment_load_cubes_demo_pjan_estatwrap.n3"));

			// ----------------
			// # Population on 1 January by age and sex [demo_pjan] (Eurstat LD)
			// <http://eurostat.linked-statistics.org/data/demo_pjan> rdf:type
			// qb:DataSet.
			dataset = new URL(
					"http://eurostat.linked-statistics.org/data/demo_pjan");
			Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);
			// # Real GDP per Capita (real local currency units, various base
			// years)
			// <http://worldbank.270a.info/dataset/GDPPCKN> rdf:type qb:DataSet.

			// Problem: this dataset only is available in a GZIP file
			// loadInStore(new URL(
			// "http://localhost:8080/Data-Fu-Engine/datasets/GDPPCKN.rdf"));
			// loadInStore(new URL(
			// "http://worldbank.270a.info/dataset/world-bank-indicators/structure"));

			// dataset = new URL("http://worldbank.270a.info/dataset/GDPPCKN");
			Olap4ldUtil._log.info("Load dataset: " + dataset);
			// loadCube(dataset);

			// Load other metadata objects?
			time = System.currentTimeMillis() - time;
			Olap4ldUtil._log.info("Load dataset: loading "
					+ this.LOADED_TRIPLE_SIZE + " triples finished in " + time
					+ "ms.");

			// Olap4ldLinkedDataUtil.dumpRDF(repo,
			// "/media/84F01919F0191352/Projects/2014/paper/Link to paper-drill-across/Link to task-data-fu/drill-across-paper/gdp_per_capita_experiment_load_cubes.n3",
			// RDFFormat.NTRIPLES);

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("unused")
	private void insertTriples(String triples) {
		String query = "PREFIX olap4ld:<http://purl.org/olap4ld/> INSERT DATA { GRAPH <http://manually> { "
				+ triples + " } }";
		Olap4ldLinkedDataUtil.sparqlRepoUpdate(repo, query, false);
	}

	@SuppressWarnings("unused")
	private void deleteTriples(String triples) {
		String query = "PREFIX olap4ld:<http://purl.org/olap4ld/> DELETE DATA { "
				+ triples + " }";
		Olap4ldLinkedDataUtil.sparqlRepoUpdate(repo, query, false);
	}

	@SuppressWarnings("unused")
	private void deleteTriplesWhere(String triples, String where) {
		String query = "PREFIX olap4ld:<http://purl.org/olap4ld/> DELETE { "
				+ triples + " } where { " + where + "}";
		Olap4ldLinkedDataUtil.sparqlRepoUpdate(repo, query, false);
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
	private List<Node[]> sparql(String query, boolean caching) {

		Olap4ldUtil._log.config("SPARQL query: " + query);

		List<Node[]> myBindings = new ArrayList<Node[]>();

		try {
			RepositoryConnection con = repo.getConnection();

			ByteArrayOutputStream boas = new ByteArrayOutputStream();
			// FileOutputStream fos = new
			// FileOutputStream("/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/resources/result.srx");

			SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(
					boas);

			TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL,
					query);
			tupleQuery.evaluate(sparqlWriter);

			ByteArrayInputStream bais = new ByteArrayInputStream(
					boas.toByteArray());

			// String xmlwriterstreamString =
			// Olap4ldLinkedDataUtil.convertStreamToString(bais);
			// System.out.println(xmlwriterstreamString);
			// Transform sparql xml to nx
			InputStream nx = Olap4ldLinkedDataUtil.transformSparqlXmlToNx(bais);

			// Only if logging level accordingly
			if (Olap4ldUtil._isDebug) {
				String test2 = Olap4ldLinkedDataUtil.convertStreamToString(nx);
				Olap4ldUtil._log.config("NX output: " + test2);
				nx.reset();
			}

			NxParser nxp = new NxParser(nx);

			Node[] nxx;
			while (nxp.hasNext()) {
				try {
					nxx = nxp.next();
					myBindings.add(nxx);
				} catch (Exception e) {

					// Might happen often, therefore config only
					Olap4ldUtil._log
							.config("NxParser: Could not parse properly: "
									+ e.getMessage());
				}
				;
			}

			boas.close();
			con.close();
			// do something interesting with the values here...
			// con.close();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TupleQueryResultHandlerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return myBindings;
	}

	/**
	 * Loads resource in store if 1) URI and location of resource not already
	 * loaded 2) number of triples has not reached maximum.
	 * 
	 * @param location
	 * @throws OlapException
	 */
	private void loadInStore(URL resource) throws OlapException {
		SailRepositoryConnection con = null;

		// Check max loaded
		String query = "select (count(?s) as ?count) where {?s ?p ?o}";
		List<Node[]> result = sparql(query, false);
		this.LOADED_TRIPLE_SIZE = new Integer(result.get(1)[0].toString());
		Olap4ldUtil._log.config("Number of loaded triples before: "
				+ this.LOADED_TRIPLE_SIZE);

		if (this.LOADED_TRIPLE_SIZE > this.MAX_LOAD_TRIPLE_SIZE) {
			Olap4ldUtil._log
					.warning("Warning: We have reached the maximum number of triples to load!");
			throw new OlapException(
					"Warning: Maximum storage capacity reached! Dataset contains too many triples.");
		}

		if (loadedMap.get(resource.hashCode()) != null
				&& loadedMap.get(resource.hashCode()) == true) {
			// Already loaded
			return;
		}

		try {
			URL location = Olap4ldLinkedDataUtil.askForLocation(resource);
			if (loadedMap.get(location.hashCode()) != null
					&& loadedMap.get(location.hashCode()) == true) {
				// Already loaded
				return;
			}

			String locationstring = location.toString();
			Olap4ldUtil._log.config("Load in store: " + location);

			con = repo.getConnection();

			// Would not work since we cannot ask for the file size without
			// downloading the file
			// Check size and set size to have of heap space
			// URLConnection urlConnection = locationurl.openConnection();
			// urlConnection.connect();
			// // assuming both bytes: 1) file_size is byte 2)
			// int file_size = urlConnection.getContentLength();
			// // TODO: Apparently file size often wrong?
			// Olap4ldUtil._log.config("File size: " + file_size);
			// long memory_size = Olap4ldUtil.getFreeMemory();
			// Olap4ldUtil._log.config("Current memory size: " + memory_size);
			//
			// if (file_size > memory_size) {
			// con.close();
			// Olap4ldUtil._log.warning("Warning: File (" + location
			// + ") to load exceeds amount of heap space memory!");
			// throw new OlapException(
			// "Warning: Maximum storage capacity reached! Dataset too large.");
			// }

			// Workaround certain files are not loadable

			// XXX: remove
			if (locationstring
					.equals("http://worldbank.270a.info/property/indicator.rdf")) {
				return;
			}

			// Guess file format
			RDFFormat format = RDFFormat.forFileName(locationstring);
			if (format != null) {
				con.add(location, locationstring, format);
			} else {
				// Heuristics

				InputStream is;
				HttpURLConnection connection = (HttpURLConnection) location
						.openConnection();
				// We always try to get rdf/xml
				connection.setRequestProperty("Accept", "application/rdf+xml");
				format = RDFFormat.RDFXML;

				// Not acceptable time
				connection.setConnectTimeout(5000);
				int responsecode = connection.getResponseCode();

				// Not acceptable format?
				if (responsecode == 406) {
					connection.disconnect();
					connection = (HttpURLConnection) location.openConnection();
					connection.setRequestProperty("Accept", "text/turtle");
					format = RDFFormat.TURTLE;
					responsecode = connection.getResponseCode();
				}

				// Error
				if (responsecode >= 400) {
					is = connection.getErrorStream();

					BufferedReader rd = new BufferedReader(
							new InputStreamReader(is));

					String response = "";
					String line;
					while ((line = rd.readLine()) != null) {
						response += line;
					}
					Olap4ldUtil._log
							.warning("Warning: URL not possible to load: "
									+ response);
					rd.close();
					is.close();
				} else {
					// is = connection.getInputStream();
					//
					// BufferedReader in = new BufferedReader(
					// new InputStreamReader(is));
					//
					// // Check first line
					// String inputLine;
					// // Read first line only.
					// // while ((inputLine = in.readLine()) != null) {
					// // }
					// inputLine = in.readLine();
					// if (inputLine != null
					// && (inputLine
					// .startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
					// || inputLine
					// .startsWith("<?xml version=\"1.0\" encoding=\"utf-8\"?>")
					// || inputLine
					// .startsWith("<"))) {
					// Olap4ldUtil._log
					// .config("Had to guess format to be RDFXML: "
					// + location);
					// format = RDFFormat.RDFXML;
					//
					// } else if (inputLine != null
					// && (!inputLine
					// .startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
					// && !inputLine
					// .startsWith("<?xml version=\"1.0\" encoding=\"utf-8\"?>")
					// || inputLine
					// .startsWith("<"))) {
					// Olap4ldUtil._log
					// .config("Had to guess format to be Turtle: "
					// + location);
					// format = RDFFormat.TURTLE;
					// // con.add(location, locationstring,
					// // RDFFormat.TURTLE);
					//
					// } else {
					// Olap4ldUtil._log
					// .warning("Warning: URL not possible to load since input line is NULL.");
					// }
					// in.close();
					// is.close();
					try {
						InputStream inputstream = connection.getInputStream();
						con.add(inputstream, locationstring, format);
						connection.disconnect();

					} catch (RDFParseException e) {
						// Try to continue on next line?
						// int linenumber = e.getLineNumber();

						// Since it happens often, we just log it in config
						Olap4ldUtil._log.config("RDFParseException:"
								+ e.getMessage());

						if (e.getColumnNumber() == 1) {
							Olap4ldUtil._log
									.config("RDFParseException, but try afresh.");

							// Try with in-built loading functionality
							if (format == RDFFormat.RDFXML) {
								con.add(location, locationstring,
										RDFFormat.TURTLE);
							} else {
								con.add(location, locationstring,
										RDFFormat.RDFXML);
							}

						}
					}
				}

			}

			// Mark as loaded
			loadedMap.put(resource.hashCode(), true);
			loadedMap.put(location.hashCode(), true);

			// Log content only if log level accordingly
			if (Olap4ldUtil._isDebug) {

				query = "select * where {?s ?p ?o}";
				Olap4ldUtil._log.config("Check loaded data (10 triples): "
						+ query);
				sparql(query, false);
			}

			con.close();
			
			// Check max loaded
			query = "select (count(?s) as ?count) where {?s ?p ?o}";
			result = sparql(query, false);
			this.LOADED_TRIPLE_SIZE = new Integer(result.get(1)[0].toString());
			Olap4ldUtil._log.config("Number of loaded triples after: "
					+ this.LOADED_TRIPLE_SIZE);

		} catch (RepositoryException e) {
			throw new OlapException("Problem with repository: "
					+ e.getMessage());
		} catch (MalformedURLException e) {
			// If this happens, it is not so bad.
			e.printStackTrace();
		} catch (IOException e) {
			Olap4ldUtil._log.config("ConnectException:" + e.getMessage());
			e.printStackTrace();
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * We load all data for a cube. We also normalise and do integrity checks.
	 * 
	 * @param location
	 */
	private void loadCube(URL uri) throws OlapException {
		try {

			// If we have cube uri and location is not loaded, yet, we start
			// collecting all information

			loadInStore(uri);

			// For everything else: Check whether really cube
			RepositoryConnection con;
			con = repo.getConnection();

			// qb:structure is more robust than a qb:DataSet.
			String testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> ASK { ?CUBE_NAME qb:structure ?dsd. FILTER (?CUBE_NAME = <"
					+ uri + ">)}";
			BooleanQuery booleanQuery = con.prepareBooleanQuery(
					QueryLanguage.SPARQL, testquery);
			boolean isDataset = booleanQuery.evaluate();
			con.close();

			if (!isDataset) {
				throw new OlapException(
						"A cube should be a qb:DataSet and serve via qb:structure a qb:DataStructureDefinition, also this one "
								+ uri + "!");
			} else {

				// If loading ds, also load dsd. Ask for DSD URI and
				// load
				String query = "PREFIX qb: <http://purl.org/linked-data/cube#> SELECT ?dsd WHERE {<"
						+ uri + "> qb:structure ?dsd}";
				List<Node[]> dsd = sparql(query, true);
				// There should be a dsd
				// Note in spec:
				// "Every qb:DataSet has exactly one associated qb:DataStructureDefinition."
				if (dsd.size() <= 1) {
					throw new OlapException(
							"A cube should serve a data structure definition!");
				} else {
					// Get the second
					URL dsduri = new URL(dsd.get(1)[0].toString());

					loadInStore(dsduri);

				}

				// If loading ds, also load components
				query = "PREFIX qb: <http://purl.org/linked-data/cube#> SELECT ?comp WHERE {<"
						+ uri
						+ "> qb:structure ?dsd. ?dsd qb:component ?comp.}";
				List<Node[]> components = sparql(query, true);
				// There should be a dsd
				// Note in spec:
				// "Every qb:DataSet has exactly one associated qb:DataStructureDefinition."
				boolean first = true;
				for (Node[] nodes : components) {
					if (first) {
						first = false;
						continue;
					}
					try {
						URL componenturi = new URL(nodes[0].toString());
						loadInStore(componenturi);
					} catch (MalformedURLException e) {
					}
				}

				// If loading ds, also load measures
				query = "PREFIX qb: <http://purl.org/linked-data/cube#> SELECT ?measure WHERE {<"
						+ uri
						+ "> qb:structure ?dsd. ?dsd qb:component ?comp. ?comp qb:measure ?measure}";
				List<Node[]> measures = sparql(query, true);
				// There should be a dsd
				// Note in spec:
				// "Every qb:DataSet has exactly one associated qb:DataStructureDefinition."
				first = true;
				for (Node[] nodes : measures) {
					if (first) {
						first = false;
						continue;
					}

					URL measureuri = new URL(nodes[0].toString());
					loadInStore(measureuri);
				}

				// If loading ds, also load dimensions
				query = "PREFIX qb: <http://purl.org/linked-data/cube#> SELECT ?dimension WHERE {<"
						+ uri
						+ "> qb:structure ?dsd. ?dsd qb:component ?comp. ?comp qb:dimension ?dimension}";
				List<Node[]> dimensions = sparql(query, true);
				// There should be a dsd
				// Note in spec:
				// "Every qb:DataSet has exactly one associated qb:DataStructureDefinition."
				if (dimensions.size() <= 1) {
					throw new OlapException("A cube should serve a dimension!");
				} else {
					first = true;
					for (Node[] nodes : dimensions) {
						if (first) {
							first = false;
							continue;
						}
						URL dimensionuri = new URL(nodes[0].toString());

						loadInStore(dimensionuri);
					}
				}

				// If loading ds, also load codelists
				query = "PREFIX qb: <http://purl.org/linked-data/cube#> SELECT ?codelist WHERE {<"
						+ uri
						+ "> qb:structure ?dsd. ?dsd qb:component ?comp. ?comp qb:dimension ?dimension. ?dimension qb:codeList ?codelist}";
				List<Node[]> codelists = sparql(query, true);
				// There should be a dsd
				// Note in spec:
				// "Every qb:DataSet has exactly one associated qb:DataStructureDefinition."
				if (codelists.size() <= 1) {
					;
				} else {
					first = true;
					// So far, members are not crawled.
					for (Node[] nodes : codelists) {
						if (first) {
							first = false;
							continue;
						}

						URL codelisturi = new URL(nodes[0].toString());
						loadInStore(codelisturi);
					}
				}
			}

			// We need to materialise implicit information
			Olap4ldUtil._log.info("Run normalisation algorithm on datasets");

			long time = System.currentTimeMillis();
			runNormalizationAlgorithm();

			// Own normalization and inferencing.

			/*
			 * SKOS:
			 * 
			 * Since 1) skos:topConceptOf is a sub-property of skos:inScheme. 2)
			 * skos:topConceptOf is owl:inverseOf the property
			 * skos:hasTopConcept 3) The rdfs:domain of skos:hasTopConcept is
			 * the class skos:ConceptScheme.: ?conceptScheme skos:hasTopConcept
			 * ?concept. => ?concept skos:inScheme ?conceptScheme.
			 */
			// String updateQuery = TYPICALPREFIXES
			// +
			// " INSERT { ?concept skos:inScheme ?codelist.} WHERE { ?codelist skos:hasTopConcept ?concept }; ";
			// Update updateQueryQuery = con.prepareUpdate(
			// QueryLanguage.SPARQL, updateQuery);
			// updateQueryQuery.execute();

			time = System.currentTimeMillis() - time;
			Olap4ldUtil._log
					.info("Run normalisation algorithm on dataset: finished in "
							+ time + "ms.");

			// Now that we presumably have loaded all necessary
			// data, we check integrity constraints

			Olap4ldUtil._log.info("Check integrity constraints on datasets.");
			time = System.currentTimeMillis();
			checkIntegrityConstraints();

			// Own checks:
			con = repo.getConnection();

			String prefixbindings = "PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#> PREFIX skos:    <http://www.w3.org/2004/02/skos/core#> PREFIX qb:      <http://purl.org/linked-data/cube#> PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#> PREFIX owl:     <http://www.w3.org/2002/07/owl#> ";

			// Datasets should have at least one
			// observation
			testquery = prefixbindings
					+ "ASK { ?CUBE_NAME a qb:DataSet. FILTER NOT EXISTS { ?obs qb:dataSet ?CUBE_NAME. } }";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				throw new OlapException(
						"Failed own check: Dataset should have at least one observation. ");
			}

			// XXX Possible other checks
			// No dimensions
			// No aggregation function
			// Code list empty
			// No member

			time = System.currentTimeMillis() - time;
			Olap4ldUtil._log
					.info("Check integrity constraints on dataset: finished in "
							+ time + "ms.");

			// Important!

			con.close();
		} catch (RepositoryException e) {
			throw new OlapException("Problem with repository: "
					+ e.getMessage());
		} catch (QueryEvaluationException e) {
			throw new OlapException("Problem with query evaluation: "
					+ e.getMessage());
		} catch (MalformedQueryException e) {
			throw new OlapException("Problem with malformed query: "
					+ e.getMessage());
		} catch (MalformedURLException e) {
			throw new OlapException("Problem with malformed url: "
					+ e.getMessage());
		}
	}

	/**
	 * Check whether we query for "Measures".
	 * 
	 * @param dimensionUniqueName
	 * @param hierarchyUniqueName
	 * @param levelUniqueName
	 * @return
	 */
	private boolean isMeasureQueriedForExplicitly(String dimensionUniqueName,
			String hierarchyUniqueName, String levelUniqueName) {
		// If one is set, it should not be Measures, not.
		// Watch out: no square brackets are needed.
		boolean explicitlyStated = (dimensionUniqueName != null && dimensionUniqueName
				.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME))
				|| (hierarchyUniqueName != null && hierarchyUniqueName
						.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME))
				|| (levelUniqueName != null && levelUniqueName
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
		if (restrictions.cubeNamePattern != null
				&& restrictions.cubeNamePattern.contains(",")) {

			String[] datasets = restrictions.cubeNamePattern.split(",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				Restrictions newrestrictions = new Restrictions();
				newrestrictions.cubeNamePattern = dataset;

				List<Node[]> intermediaryresult = getCubesPerDataSet(newrestrictions);

				// Add to result
				boolean first = true;
				for (Node[] nodes : intermediaryresult) {
					// We do not want to have the single datasets returned.
					// result.add(nodes);
					if (first) {
						if (i == 0) {
							result.add(nodes);
						}
						first = false;
						continue;
					}
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

		// for now, we simply assume equivalence statements given

		List<Node[]> equivs = new ArrayList<Node[]>();

		equivs.add(new Node[] {
				new Resource(
						"http://lod.gesis.org/lodpilot/ALLBUS/vocab.rdf#geo"),
				new Resource(
						"http://ontologycentral.com/2009/01/eurostat/ns#geo") });
		
		// Hierarchy gesis-geo:list = estatwrap:geo
		equivs.add(new Node[] {
				new Resource("http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#list"),
				new Resource("http://ontologycentral.com/2009/01/eurostat/ns#geo") });

		// Could also for the olap
//		equivs.add(new Node[] {
//				new Resource("http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#00"),
//				new Resource("http://ontologycentral.com/dic/geo#DE") });
		
		equivs.add(new Node[] {
				new Resource("http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#00"),
				new Resource("http://olap4ld.googlecode.com/dic/geo#DE") });

		this.equivalenceList = createEquivalenceList(equivs);

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
				globalcubename += nodes[cubemap.get("?CUBE_NAME")];
			}

		} else {
			globalcubename = restrictions.cubeNamePattern;
		}

		Node[] virtualcube = new Node[] { new Literal(TABLE_CAT),
				new Literal(TABLE_SCHEM), new Literal(globalcubename),
				new Literal("CUBE"), new Literal("Global Cube"),
				new Literal("This is the global cube.") };
		result.add(virtualcube);

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
		// For now, we simply preload.
		if (restrictions.cubeNamePattern != null) {
			try {
				loadCube(new URL(restrictions.cubeNamePattern));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			Olap4ldUtil._log
					.config("In this situation, we cannot load and validate a dataset!");
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

		result = sparql(querytemplate, true);

		return result;
	}

	private void checkIntegrityConstraints() throws OlapException {

		// Check space for more complex integrity constraints

		boolean doComplexObservationIntegrityConstraints = (this.LOADED_TRIPLE_SIZE < this.MAX_COMPLEX_CONSTRAINTS_TRIPLE_SIZE);

		// Logging
		Olap4ldUtil._log.config("Run integrity constraints...");
		Olap4ldUtil._log.config("including complex integrity constraints: "
				+ doComplexObservationIntegrityConstraints + "...");

		try {
			// Now, we check the integrity constraints
			RepositoryConnection con;
			con = repo.getConnection();

			String testquery;
			BooleanQuery booleanQuery;

			boolean error = false;
			String overview = "";
			String status = "";

			// IC-1. Unique DataSet. Every qb:Observation
			// has exactly one associated qb:DataSet.
			// TODO: May take long since all observations tested

			// Since needs to go through all observations, only done if enough
			// memory
			if (doComplexObservationIntegrityConstraints) {

				testquery = TYPICALPREFIXES
						+ "ASK {  {        ?obs a qb:Observation .    FILTER NOT EXISTS { ?obs qb:dataSet ?dataset1 . } } UNION {        ?obs a qb:Observation ;       qb:dataSet ?dataset1, ?dataset2 .    FILTER (?dataset1 != ?dataset2)  }}";
				booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
						testquery);
				if (booleanQuery.evaluate() == true) {
					error = true;
					status = "Failed specification check: IC-1. Unique DataSet. Every qb:Observation has exactly one associated qb:DataSet.<br/>";
					Olap4ldUtil._log.config(status);
					overview += status;
				} else {
					status = "Successful specification check: IC-1. Unique DataSet. Every qb:Observation has exactly one associated qb:DataSet.<br/>";
					Olap4ldUtil._log.config(status);
					overview += status;
				}
			}

			// IC-2. Unique DSD. Every qb:DataSet has
			// exactly one associated
			// qb:DataStructureDefinition. <= tested before
			testquery = TYPICALPREFIXES
					+ "ASK {  {        ?dataset a qb:DataSet .    FILTER NOT EXISTS { ?dataset qb:structure ?dsd . }  } UNION {    ?dataset a qb:DataSet ;       qb:structure ?dsd1, ?dsd2 .    FILTER (?dsd1 != ?dsd2)  }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-2. Unique DSD. Every qb:DataSet has exactly one associated qb:DataStructureDefinition. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-2. Unique DSD. Every qb:DataSet has exactly one associated qb:DataStructureDefinition.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-3. DSD includes measure
			testquery = TYPICALPREFIXES
					+ "ASK {  ?dsd a qb:DataStructureDefinition .  FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty [a qb:MeasureProperty]] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-3. DSD includes measure. Every qb:DataStructureDefinition must include at least one declared measure.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-3. DSD includes measure. Every qb:DataStructureDefinition must include at least one declared measure.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-4. Dimensions have range
			testquery = TYPICALPREFIXES
					+ "ASK { ?dim a qb:DimensionProperty . FILTER NOT EXISTS { ?dim rdfs:range [] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-4. Dimensions have range. Every dimension declared in a qb:DataStructureDefinition must have a declared rdfs:range.\n";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-4. Dimensions have range. Every dimension declared in a qb:DataStructureDefinition must have a declared rdfs:range.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-5. Concept dimensions have code lists
			testquery = TYPICALPREFIXES
					+ "ASK { ?dim a qb:DimensionProperty ; rdfs:range skos:Concept . FILTER NOT EXISTS { ?dim qb:codeList [] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-5. Concept dimensions have code lists. Every dimension with range skos:Concept must have a qb:codeList. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-5. Concept dimensions have code lists. Every dimension with range skos:Concept must have a qb:codeList. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-6. Only attributes may be optional <= not
			// important right now. We do not regard
			// attributes.
			testquery = TYPICALPREFIXES
					+ "ASK {  ?dsd qb:component ?componentSpec .  ?componentSpec qb:componentRequired \"false\"^^xsd:boolean ;                 qb:componentProperty ?component .  FILTER NOT EXISTS { ?component a qb:AttributeProperty }} ";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-6. Only attributes may be optional. The only components of a qb:DataStructureDefinition that may be marked as optional, using qb:componentRequired are attributes. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-6. Only attributes may be optional. The only components of a qb:DataStructureDefinition that may be marked as optional, using qb:componentRequired are attributes.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-7. Slice Keys must be declared <= not
			// important right now. We do not regard slices.
			testquery = TYPICALPREFIXES
					+ "ASK {    ?sliceKey a qb:SliceKey .    FILTER NOT EXISTS { [a qb:DataStructureDefinition] qb:sliceKey ?sliceKey }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-7. Slice Keys must be declared. Every qb:SliceKey must be associated with a qb:DataStructureDefinition.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-7. Slice Keys must be declared. Every qb:SliceKey must be associated with a qb:DataStructureDefinition.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-8. Slice Keys consistent with DSD
			// Spelling error in spec fixed
			testquery = TYPICALPREFIXES
					+ "ASK {  ?sliceKey a qb:SliceKey;      qb:componentProperty ?prop .  ?dsd qb:sliceKey ?sliceKey .  FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty ?prop] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-8. Slice Keys consistent with DSD. Every qb:componentProperty on a qb:SliceKey must also be declared as a qb:component of the associated qb:DataStructureDefinition.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-8. Slice Keys consistent with DSD. Every qb:componentProperty on a qb:SliceKey must also be declared as a qb:component of the associated qb:DataStructureDefinition. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-9. Unique slice structure
			// Does not seem to work. Returns all slices. Therefore disabled.
			// String query =
			// "PREFIX qb: <http://purl.org/linked-data/cube#> select * where {  {    ?slice a qb:Slice .    FILTER NOT EXISTS { ?slice qb:sliceStructure ?key } } UNION {    ?slice a qb:Slice ;           qb:sliceStructure ?key1, ?key2;    FILTER (?key1 != ?key2)  }}";
			// List<Node[]> result = sparql(query, false);
			// testquery = TYPICALPREFIXES
			// +
			// "ASK {  {    ?slice a qb:Slice .    FILTER NOT EXISTS { ?slice qb:sliceStructure ?key } } UNION {    ?slice a qb:Slice ;           qb:sliceStructure ?key1, ?key2;    FILTER (?key1 != ?key2)  }}";
			// booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
			// testquery);
			// if (booleanQuery.evaluate() == true) {
			// error = true;
			// status =
			// "Failed specification check: IC-9. Unique slice structure. Each qb:Slice must have exactly one associated qb:sliceStructure. <br/>";
			// Olap4ldUtil._log.config(status);
			// overview += status;
			// } else {
			// status =
			// "Successful specification check: IC-9. Unique slice structure. Each qb:Slice must have exactly one associated qb:sliceStructure. <br/>";
			// Olap4ldUtil._log.config(status);
			// overview += status;
			// }

			// IC-10. Slice dimensions complete
			testquery = TYPICALPREFIXES
					+ "ASK {  ?slice qb:sliceStructure [qb:componentProperty ?dim] .  FILTER NOT EXISTS { ?slice ?dim [] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-10. Slice dimensions complete. Every qb:Slice must have a value for every dimension declared in its qb:sliceStructure.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-10. Slice dimensions complete. Every qb:Slice must have a value for every dimension declared in its qb:sliceStructure.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// Since needs to go through all observations, only done if enough
			// memory
			if (doComplexObservationIntegrityConstraints) {

				// IC-11. All dimensions required <= takes too
				// long
				testquery = TYPICALPREFIXES
						+ "ASK {    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .    ?dim a qb:DimensionProperty;    FILTER NOT EXISTS { ?obs ?dim [] }}";
				booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
						testquery);
				if (booleanQuery.evaluate() == true) {
					error = true;
					status = "Failed specification check: IC-11. All dimensions required. Every qb:Observation has a value for each dimension declared in its associated qb:DataStructureDefinition. <br/>";
					Olap4ldUtil._log.config(status);
					overview += status;
				} else {
					status = "Successful specification check: IC-11. All dimensions required. Every qb:Observation has a value for each dimension declared in its associated qb:DataStructureDefinition. <br/>";
					Olap4ldUtil._log.config(status);
					overview += status;
				}

				// IC-12. No duplicate observations <= takes especially
				// long, expensive quadratic check (IC-12) (see
				// http://lists.w3.org/Archives/Public/public-gld-wg/2013Jul/0017.html)
				// Dave Reynolds has implemented a linear time version of it
				// testquery = TYPICALPREFIXES
				// +
				// "ASK {  FILTER( ?allEqual )  {    SELECT (MIN(?equal) AS ?allEqual) WHERE {        ?obs1 qb:dataSet ?dataset .        ?obs2 qb:dataSet ?dataset .        FILTER (?obs1 != ?obs2)        ?dataset qb:structure/qb:component/qb:componentProperty ?dim .        ?dim a qb:DimensionProperty .        ?obs1 ?dim ?value1 .        ?obs2 ?dim ?value2 .        BIND( ?value1 = ?value2 AS ?equal)    } GROUP BY ?obs1 ?obs2  }}";
				// booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
				// testquery);
				// if (booleanQuery.evaluate() == true) {
				// error = true;
				// status =
				// "Failed specification check: IC-12. No duplicate observations. No two qb:Observations in the same qb:DataSet may have the same value for all dimensions.<br/>";
				// Olap4ldUtil._log.config(status);
				// overview += status;
				// } else {
				// status =
				// "Successful specification check: IC-12. No duplicate observations. No two qb:Observations in the same qb:DataSet may have the same value for all dimensions.<br/>";
				// Olap4ldUtil._log.config(status);
				// overview += status;
				// }

			}

			// IC-13. Required attributes <= We do not
			// regard attributes
			testquery = TYPICALPREFIXES
					+ "ASK { ?obs qb:dataSet/qb:structure/qb:component ?component .   ?component qb:componentRequired \"true\"^^xsd:boolean ;               qb:componentProperty ?attr .    FILTER NOT EXISTS { ?obs ?attr [] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-13. Required attributes. Every qb:Observation has a value for each declared attribute that is marked as required.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-13. Required attributes. Every qb:Observation has a value for each declared attribute that is marked as required. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-14. All measures present
			testquery = TYPICALPREFIXES
					+ "ASK { ?obs qb:dataSet/qb:structure ?dsd . FILTER NOT EXISTS { ?dsd qb:component/qb:componentProperty qb:measureType } ?dsd qb:component/qb:componentProperty ?measure . ?measure a qb:MeasureProperty; FILTER NOT EXISTS { ?obs ?measure [] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-14. All measures present. In a qb:DataSet which does not use a Measure dimension then each individual qb:Observation must have a value for every declared measure.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-14. All measures present. In a qb:DataSet which does not use a Measure dimension then each individual qb:Observation must have a value for every declared measure.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-15. Measure dimension consistent <= We do
			// not support measureType, yet.
			testquery = TYPICALPREFIXES
					+ "ASK {    ?obs qb:dataSet/qb:structure ?dsd ;         qb:measureType ?measure .    ?dsd qb:component/qb:componentProperty qb:measureType .    FILTER NOT EXISTS { ?obs ?measure [] }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-15. Measure dimension consistent. In a qb:DataSet which uses a Measure dimension then each qb:Observation must have a value for the measure corresponding to its given qb:measureType.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-15. Measure dimension consistent. In a qb:DataSet which uses a Measure dimension then each qb:Observation must have a value for the measure corresponding to its given qb:measureType.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-16. Single measure on measure dimension
			// observation
			testquery = TYPICALPREFIXES
					+ "ASK {    ?obs qb:dataSet/qb:structure ?dsd ;         qb:measureType ?measure ;         ?omeasure [] .    ?dsd qb:component/qb:componentProperty qb:measureType ;         qb:component/qb:componentProperty ?omeasure .    ?omeasure a qb:MeasureProperty .        FILTER (?omeasure != ?measure)}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-16. Single measure on measure dimension observation. In a qb:DataSet which uses a Measure dimension then each qb:Observation must only have a value for one measure (by IC-15 this will be the measure corresponding to its qb:measureType).<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-16. Single measure on measure dimension observation. In a qb:DataSet which uses a Measure dimension then each qb:Observation must only have a value for one measure (by IC-15 this will be the measure corresponding to its qb:measureType). <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-17. All measures present in measures dimension cube
			testquery = TYPICALPREFIXES
					+ "ASK { {      SELECT ?numMeasures (COUNT(?obs2) AS ?count) WHERE {         {             SELECT ?dsd (COUNT(?m) AS ?numMeasures) WHERE {                 ?dsd qb:component/qb:componentProperty ?m.                  ?m a qb:MeasureProperty .              } GROUP BY ?dsd          }                  ?obs1 qb:dataSet/qb:structure ?dsd;                qb:dataSet ?dataset ;                qb:measureType ?m1 .              ?obs2 qb:dataSet ?dataset ;                qb:measureType ?m2 .          FILTER NOT EXISTS {              ?dsd qb:component/qb:componentProperty ?dim .              FILTER (?dim != qb:measureType)              ?dim a qb:DimensionProperty .              ?obs1 ?dim ?v1 .              ?obs2 ?dim ?v2.              FILTER (?v1 != ?v2)          }                } GROUP BY ?obs1 ?numMeasures        HAVING (?count != ?numMeasures)  }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-17. All measures present in measures dimension cube. In a qb:DataSet which uses a Measure dimension then if there is a Observation for some combination of non-measure dimensions then there must be other Observations with the same non-measure dimension values for each of the declared measures.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-17. All measures present in measures dimension cube. In a qb:DataSet which uses a Measure dimension then if there is a Observation for some combination of non-measure dimensions then there must be other Observations with the same non-measure dimension values for each of the declared measures.<br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// IC-18. Consistent data set links
			testquery = TYPICALPREFIXES
					+ "ASK { ?dataset qb:slice ?slice . ?slice qb:observation ?obs .FILTER NOT EXISTS { ?obs qb:dataSet ?dataset . }}";
			booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
					testquery);
			if (booleanQuery.evaluate() == true) {
				error = true;
				status = "Failed specification check: IC-18. If a qb:DataSet D has a qb:slice S, and S has an qb:observation O, then the qb:dataSet corresponding to O must be D. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			} else {
				status = "Successful specification check: IC-18. If a qb:DataSet D has a qb:slice S, and S has an qb:observation O, then the qb:dataSet corresponding to O must be D. <br/>";
				Olap4ldUtil._log.config(status);
				overview += status;
			}

			// Since needs to go through all observations, only done if enough
			// memory
			if (doComplexObservationIntegrityConstraints) {

				// Watch out: skos:inScheme has to be used.
				// String query =
				// TYPICALPREFIXES+" select * { ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .    ?dim a qb:DimensionProperty ;        qb:codeList ?list .    ?list a skos:ConceptScheme .    ?obs ?dim ?v .    FILTER NOT EXISTS { ?v a skos:Concept ; skos:inScheme ?list }}";
				// List<Node[]> result = sparql(query, false);

				// IC-19. Codes from code list
				// Probably takes very long since involves property chain and
				// going through all observations.
				testquery = TYPICALPREFIXES
						+ "ASK { ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .    ?dim a qb:DimensionProperty ;        qb:codeList ?list .    ?list a skos:ConceptScheme .    ?obs ?dim ?v .    FILTER NOT EXISTS { ?v a skos:Concept ; skos:inScheme ?list }}";
				String testquery2 = TYPICALPREFIXES
						+ "ASK {   ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .    ?dim a qb:DimensionProperty ;        qb:codeList ?list .    ?list a skos:Collection .    ?obs ?dim ?v .    FILTER NOT EXISTS { ?v a skos:Concept . ?list skos:member+ ?v }}";
				booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
						testquery);
				BooleanQuery booleanQuery2 = con.prepareBooleanQuery(
						QueryLanguage.SPARQL, testquery2);
				if (booleanQuery.evaluate() == true
						|| booleanQuery2.evaluate() == true) {
					error = true;
					status = "Failed specification check: IC-19. If a dimension property has a qb:codeList, then the value of the dimension property on every qb:Observation must be in the code list.  <br/>";
					Olap4ldUtil._log.config(status);
					overview += status;
				} else {
					status = "Successful specification check: IC-19. If a dimension property has a qb:codeList, then the value of the dimension property on every qb:Observation must be in the code list.  <br/>";
					Olap4ldUtil._log.config(status);
					overview += status;
				}

			}

			// For the next two integrity constraints, we need instantiation
			// queries first.
			// XXX: Do them later.

			// IC-20. Codes from hierarchy
			// testquery = prefixbindings
			// +
			// "ASK {    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .    ?dim a qb:DimensionProperty ;        qb:codeList ?list .    ?list a qb:HierarchicalCodeList .    ?obs ?dim ?v .    FILTER NOT EXISTS { ?list qb:hierarchyRoot/<$p>* ?v }}";
			// booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
			// testquery);
			// if (booleanQuery.evaluate() == true) {
			// error = true;
			// status =
			// "Failed specification check: IC-20. If a dimension property has a qb:HierarchicalCodeList with a non-blank qb:parentChildProperty then the value of that dimension property on every qb:Observation must be reachable from a root of the hierarchy using zero or more hops along the qb:parentChildProperty links.  <br/>";
			// Olap4ldUtil._log.config(status);
			// overview += status;
			// } else {
			// status =
			// "Successful specification check: IC-20. If a dimension property has a qb:HierarchicalCodeList with a non-blank qb:parentChildProperty then the value of that dimension property on every qb:Observation must be reachable from a root of the hierarchy using zero or more hops along the qb:parentChildProperty links.  <br/>";
			// Olap4ldUtil._log.config(status);
			// overview += status;
			// }

			// IC-21. Codes from hierarchy (inverse)
			// testquery = prefixbindings
			// +
			// "ASK {    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .    ?dim a qb:DimensionProperty ;        qb:codeList ?list .    ?list a qb:HierarchicalCodeList .    ?obs ?dim ?v .    FILTER NOT EXISTS { ?list qb:hierarchyRoot/<$p>* ?v }}";
			// booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,
			// testquery);
			// if (booleanQuery.evaluate() == true) {
			// error = true;
			// status =
			// "Failed specification check: IC-21. If a dimension property has a qb:HierarchicalCodeList with an inverse qb:parentChildProperty then the value of that dimension property on every qb:Observation must be reachable from a root of the hierarchy using zero or more hops along the inverse qb:parentChildProperty links.  <br/>";
			// Olap4ldUtil._log.config(status);
			// overview += status;
			// } else {
			// status =
			// "Successful specification check: IC-21. If a dimension property has a qb:HierarchicalCodeList with an inverse qb:parentChildProperty then the value of that dimension property on every qb:Observation must be reachable from a root of the hierarchy using zero or more hops along the inverse qb:parentChildProperty links.  <br/>";
			// Olap4ldUtil._log.config(status);
			// overview += status;
			// }

			// Important!
			con.close();

			if (error) {
				Olap4ldUtil._log
						.warning("Integrity constraints failed: Integrity constraints overview: "
								+ overview);
				// XXX: OlapExceptions possible?
				throw new OlapException(
						"Integrity constraints failed: Integrity constraints overview:<br/>"
								+ overview);
			} else {
				// Logging
				Olap4ldUtil._log
						.config("Integrity constraints successful: Integrity constraints overview: "
								+ overview);
			}

		} catch (RepositoryException e) {
			throw new OlapException("Problem with repository: "
					+ e.getMessage());
		} catch (MalformedQueryException e) {
			throw new OlapException("Problem with malformed query: "
					+ e.getMessage());
		} catch (QueryEvaluationException e) {
			throw new OlapException("Problem with query evaluation: "
					+ e.getMessage());
		}
	}

	/**
	 * According to QB specification, a cube may be provided in abbreviated form
	 * so that inferences first have to be materialised to properly query a
	 * cube.
	 * 
	 * @throws OlapException
	 */
	private void runNormalizationAlgorithm() throws OlapException {

		// Logging
		Olap4ldUtil._log.config("Run normalization algorithm...");

		try {
			RepositoryConnection con;

			con = repo.getConnection();

			// First, we run normalization algorithm
			String updateQuery = "PREFIX rdf:            <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX qb:             <http://purl.org/linked-data/cube#> INSERT { ?o rdf:type qb:Observation .} WHERE {    [] qb:observation ?o .}; INSERT { ?o rdf:type qb:Observation .} WHERE { ?o qb:dataSet [] .}; INSERT {    ?s rdf:type qb:Slice . } WHERE {  [] qb:slice ?s.}; INSERT {    ?cs qb:componentProperty ?p .    ?p  rdf:type qb:DimensionProperty .} WHERE {    ?cs qb:dimension ?p .}; INSERT {    ?cs qb:componentProperty ?p .    ?p  rdf:type qb:MeasureProperty .} WHERE {    ?cs qb:measure ?p .};INSERT {    ?cs qb:componentProperty ?p .    ?p  rdf:type qb:AttributeProperty .} WHERE {    ?cs qb:attribute ?p .}";
			Update updateQueryQuery = con.prepareUpdate(QueryLanguage.SPARQL,
					updateQuery);
			updateQueryQuery.execute();

			// # Dataset attachments
			updateQuery = "PREFIX qb:             <http://purl.org/linked-data/cube#> INSERT {    ?obs  ?comp ?value} WHERE {    ?spec    qb:componentProperty ?comp ;            qb:componentAttachment qb:DataSet .    ?dataset qb:structure [qb:component ?spec];             ?comp ?value .    ?obs     qb:dataSet ?dataset.};";
			con.prepareUpdate(QueryLanguage.SPARQL, updateQuery);
			updateQueryQuery = con.prepareUpdate(QueryLanguage.SPARQL,
					updateQuery);
			updateQueryQuery.execute();

			// # Slice attachments
			updateQuery = "PREFIX qb:             <http://purl.org/linked-data/cube#> INSERT {    ?obs  ?comp ?value} WHERE {    ?spec    qb:componentProperty ?comp;             qb:componentAttachment qb:Slice .    ?dataset qb:structure [qb:component ?spec];             qb:slice ?slice .    ?slice ?comp ?value;           qb:observation ?obs .};";
			con.prepareUpdate(QueryLanguage.SPARQL, updateQuery);
			updateQueryQuery = con.prepareUpdate(QueryLanguage.SPARQL,
					updateQuery);
			updateQueryQuery.execute();

			// # Dimension values on slices
			updateQuery = "PREFIX qb:             <http://purl.org/linked-data/cube#> INSERT {    ?obs  ?comp ?value} WHERE {    ?spec    qb:componentProperty ?comp .    ?comp a  qb:DimensionProperty .    ?dataset qb:structure [qb:component ?spec];             qb:slice ?slice .    ?slice ?comp ?value;           qb:observation ?obs .}";
			con.prepareUpdate(QueryLanguage.SPARQL, updateQuery);
			updateQueryQuery = con.prepareUpdate(QueryLanguage.SPARQL,
					updateQuery);
			updateQueryQuery.execute();

			// Important!
			con.close();
		} catch (RepositoryException e) {
			throw new OlapException("Problem with repository: "
					+ e.getMessage());
		} catch (MalformedQueryException e) {
			throw new OlapException("Problem with malformed query: "
					+ e.getMessage());
		} catch (UpdateExecutionException e) {
			throw new OlapException("Problem with update execution: "
					+ e.getMessage());
		}
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

			String[] datasets = restrictions.cubeNamePattern.split(",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				String saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = dataset;

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
				newnode[dimensionmap.get("?CUBE_NAME")] = new Resource(
						restrictions.cubeNamePattern);
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

	private Node getCanonical(Node canonical) {
		for (List<Node> equivalenceClass : equivalenceList) {
			for (Node node : equivalenceClass) {

				if (node.equals(canonical)) {
					canonical = equivalenceClass.get(0);
					break;
				}
			}
		}
		return canonical;
	}

	/**
	 * 
	 * @param equivs
	 *            - equiv[0] first same as and equiv[1] second same as entity.
	 * @return
	 */
	private List<List<Node>> createEquivalenceList(List<Node[]> equivs) {
		List<List<Node>> equivalenceList = new ArrayList<List<Node>>();

		for (Node[] equiv : equivs) {

			Node A = equiv[0];
			Node B = equiv[1];

			// Store equiv
			List<Node> rA = null;
			for (List<Node> equivalenceClass : equivalenceList) {
				for (Node node : equivalenceClass) {
					if (node.equals(A)) {
						rA = equivalenceClass;
					}
				}
				if (rA != null) {
					break;
				}
			}
			List<Node> rB = null;
			for (List<Node> equivalenceClass : equivalenceList) {
				for (Node node : equivalenceClass) {
					if (node.equals(B)) {
						rB = equivalenceClass;
					}
				}
				if (rB != null) {
					break;
				}
			}

			if (rA == null && rB == null) {
				ArrayList<Node> newEquivalenceClass = new ArrayList<Node>();
				newEquivalenceClass.add(A);
				newEquivalenceClass.add(B);
				equivalenceList.add(newEquivalenceClass);
			} else if (rA != null && rB != null) {
				// merge rA and rB

				for (Node node : rB) {
					rA.add(node);
				}
				equivalenceList.remove(rB);

			} else if (rA != null) {
				// add B to rA

				rA.add(B);

			} else if (rB != null) {
				// add A to rB

				rB.add(A);
			}

		}

		return equivalenceList;
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

			List<Node[]> myresult = sparql(querytemplate, true);
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

			List<Node[]> myresult = sparql(querytemplate, true);

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
		result = replaceIdentifiersWithCanonical(result);

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
		if (restrictions.cubeNamePattern != null
				&& restrictions.cubeNamePattern.contains(",")) {

			String[] datasets = restrictions.cubeNamePattern.split(",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				String saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = dataset;

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
					newnode[map.get("?CUBE_NAME")] = new Resource(
							restrictions.cubeNamePattern);
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
				.readInQueryTemplate("sesame_getMeasures.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> result = sparql(querytemplate, true);

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

		List<Node[]> result2 = sparql(querytemplate, true);

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
		result = replaceIdentifiersWithCanonical(result);

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

			String[] datasets = restrictions.cubeNamePattern.split(",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				String saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = dataset;

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

					// We do not want to have the single datasets returned.
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
				newnode[hierarchymap.get("?CUBE_NAME")] = new Resource(
						restrictions.cubeNamePattern);
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
						.get("?HIERARCHY_UNIQUE_NAME")].toString().equals(
						newnode[hierarchymap.get("?HIERARCHY_UNIQUE_NAME")]
								.toString());
				boolean sameCube = aResult[hierarchymap.get("?CUBE_NAME")]
						.toString().equals(
								newnode[hierarchymap.get("?CUBE_NAME")]
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

			List<Node[]> myresult = sparql(querytemplate, true);

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

			List<Node[]> myresult = sparql(querytemplate, true);

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

			List<Node[]> myresult = sparql(querytemplate, true);

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
		result = replaceIdentifiersWithCanonical(result);

		return result;
	}

	private List<Node[]> replaceIdentifiersWithCanonical(List<Node[]> result) {

		List<Node[]> newresult = new ArrayList<Node[]>();

		for (Node[] anIntermediaryresult : result) {
			Node[] newnode = new Node[anIntermediaryresult.length];
			for (int i = 0; i < anIntermediaryresult.length; i++) {
				newnode[i] = getCanonical(anIntermediaryresult[i]);
			}
			newresult.add(newnode);
		}

		return newresult;
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
		if (restrictions.cubeNamePattern != null
				&& restrictions.cubeNamePattern.contains(",")) {

			String[] datasets = restrictions.cubeNamePattern.split(",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				String saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = dataset;

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
					newnode[levelmap.get("?CUBE_NAME")] = new Resource(
							restrictions.cubeNamePattern);
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
								.get("?LEVEL_UNIQUE_NAME")].toString().equals(
								newnode[levelmap.get("?LEVEL_UNIQUE_NAME")]
										.toString());
						boolean sameCube = aResult[levelmap.get("?CUBE_NAME")]
								.toString().equals(
										newnode[levelmap.get("?CUBE_NAME")]
												.toString());

						if (sameDimension && sameCube) {
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

			List<Node[]> myresult = sparql(querytemplate, true);
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

			myresult = sparql(querytemplate, true);

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

			List<Node[]> myresult = sparql(querytemplate, true);

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
			List<Node[]> myresult = sparql(querytemplate, true);

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
		result = replaceIdentifiersWithCanonical(result);

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
		if (restrictions.cubeNamePattern != null
				&& restrictions.cubeNamePattern.contains(",")) {

			String[] datasets = restrictions.cubeNamePattern.split(",");
			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];
				// Should make sure that the full restrictions are used.
				String saverestrictioncubePattern = restrictions.cubeNamePattern;
				restrictions.cubeNamePattern = dataset;

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
					newnode[membermap.get("?CUBE_NAME")] = new Resource(
							restrictions.cubeNamePattern);
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
								.get("?MEMBER_UNIQUE_NAME")].toString().equals(
								newnode[membermap.get("?MEMBER_UNIQUE_NAME")]
										.toString());
						boolean sameCube = aResult[membermap.get("?CUBE_NAME")]
								.toString().equals(
										newnode[membermap.get("?CUBE_NAME")]
												.toString());

						if (sameDimension && sameCube) {
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
			intermediaryresult = getMeasureMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		// Regular members
		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			intermediaryresult = getHasTopConceptMembers(restrictions);

			addToResult(intermediaryresult, result);
		}

		// Xkos members
		// Watch out: No square brackets
		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			intermediaryresult = getXkosMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		// If we still do not have members, then we might have degenerated
		// members
		if (!isMeasureQueriedForExplicitly(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {
			// Members without codeList
			intermediaryresult = getDegeneratedMembers(restrictions);

			addToResult(intermediaryresult, result);

		}

		// Use canonical identifier
		result = replaceIdentifiersWithCanonical(result);

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
				.readInQueryTemplate("sesame_getMembers_measure_members.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> memberUris2 = sparql(querytemplate, true);

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

		List<Node[]> memberUris2 = sparql(querytemplate, true);

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

			List<Node[]> memberUris = sparql(querytemplate, true);

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

		List<Node[]> memberUris1 = sparql(querytemplate, true);

		return memberUris1;

	}

	@SuppressWarnings("unused")
	private boolean isResourceAndNotLiteral(String resource) {
		return resource.startsWith("http:");
	}

	private String createFilterForRestrictions(Restrictions restrictions) {
		
		// We need to create a filter for the specific restriction
		String cubeNamePatternFilter = (restrictions.cubeNamePattern != null) ? " FILTER (?CUBE_NAME = <"
				+ restrictions.cubeNamePattern + ">) "
				: "";
		String dimensionUniqueNameFilter = createFilterConsiderEquivalences(restrictions.dimensionUniqueName, "DIMENSION_UNIQUE_NAME");
				
		String hierarchyUniqueNameFilter = createFilterConsiderEquivalences(restrictions.hierarchyUniqueName, "HIERARCHY_UNIQUE_NAME");
				
		String levelUniqueNameFilter = createFilterConsiderEquivalences(restrictions.levelUniqueName, "LEVEL_UNIQUE_NAME");
		
		String memberUniqueNameFilter = createFilterConsiderEquivalences(restrictions.memberUniqueName, "MEMBER_UNIQUE_NAME");

		return cubeNamePatternFilter + dimensionUniqueNameFilter
				+ hierarchyUniqueNameFilter + levelUniqueNameFilter
				+ memberUniqueNameFilter;
	}

	private String createFilterConsiderEquivalences(String uniqueName, String variableName) {
		if (uniqueName != null && !uniqueName
				.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
			
			List<Node> equivalenceClass = new ArrayList<Node>();
			equivalenceClass.add(new Resource(uniqueName));
			for (List<Node> iterable_element : this.equivalenceList) {
				for (Node node : iterable_element) {
					if (uniqueName.equals(node.toString())) {
						equivalenceClass = iterable_element;
						break;
					}
				}
			}
			
			// Since we sometimes manually build member names, we have to check
			// on strings
			String[] filterString = new String[equivalenceClass.size()];
			for (int i = 0; i < filterString.length; i++) {
				filterString[i] = "str(?"+variableName+") = \""+equivalenceClass.get(i)+"\"";
			}
			
			return "FILTER ("+Olap4ldLinkedDataUtil.implodeArray(filterString, "||")+")";
			
		} else {
			return "";
		}
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
		Olap4ldUtil._log.config("Execute OLAP query...");
		Olap4ldUtil._log.config("Logical query plan: " + queryplan.toString());

		long time = System.currentTimeMillis();

		// Create physical query plan
		createExecplan(queryplan);

		Olap4ldUtil._log.info("Create and execute physical query plan: "
				+ execplan.toString());

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
				.info("Create and execute physical query plan: finished in "
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
