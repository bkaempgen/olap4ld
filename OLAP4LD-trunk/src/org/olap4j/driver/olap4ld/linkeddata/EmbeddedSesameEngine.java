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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.olap4j.Position;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.driver.olap4ld.helper.Restrictions;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerException;
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
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * The EmbeddedSesameEngine manages an embedded Sesame repository (triple store)
 * while executing metadata or olap queries.
 * 
 * @author b-kaempgen
 * 
 */
public class EmbeddedSesameEngine implements LinkedDataEngine {

	// Meta data attributes
	private static final String DATASOURCEDESCRIPTION = "OLAP data from the statistical Linked Data cloud.";

	private static final String PROVIDERNAME = "The community.";

	private static String URL;

	private static final String DATASOURCEINFO = "Data following the Linked Data principles.";

	private static final String TABLE_CAT = "LdCatalogSchema";

	private static final String TABLE_SCHEM = "LdCatalogSchema";

	public String DATASOURCENAME;

	public String DATASOURCEVERSION;

	// Helper attributes

	/**
	 * Map of locations that have been loaded into the embedded triple store.
	 */
	private HashMap<Integer, Boolean> locationsMap = new HashMap<Integer, Boolean>();

	/**
	 * The Sesame repository (triple store).
	 */
	private SailRepository repo;

	public EmbeddedSesameEngine(URL serverUrlObject,
			ArrayList<String> datastructuredefinitions,
			ArrayList<String> datasets, String databasename) {

		// We actually do not need that.
		URL = serverUrlObject.toString();

		if (databasename.equals("EMBEDDEDSESAME")) {
			DATASOURCENAME = databasename;
			DATASOURCEVERSION = "1.0";
		}

		// Store
		this.repo = new SailRepository(new MemoryStore());
		try {
			repo.initialize();

			// do something interesting with the values here...
			// con.close();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Those given URIs we load and add to the location map
		for (String string : datastructuredefinitions) {
			try {
				String location = askForLocation(string);
				if (!isStored(location)) {
					loadInStore(location);
				}
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (String string : datasets) {
			try {
				String location = askForLocation(string);
				if (!isStored(location)) {
					loadInStore(location);
				}
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
	 * Helper Method for asking for location
	 * 
	 * @param uri
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private String askForLocation(String uri) throws MalformedURLException {

		Olap4ldUtil._log.info("Ask for location: " + uri + "...");

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
			// We should remove # uris
			if (uri.contains("#")) {
				int index = uri.lastIndexOf("#");
				uri = uri.substring(0, index);
			}
		} catch (IOException e) {
			throw new MalformedURLException(e.getMessage());
		}

		Olap4ldUtil._log.info("... result: " + uri);
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
	 *            (not used)
	 * @return
	 */
	private List<Node[]> sparql(String query, boolean caching) {

		Olap4ldUtil._log.info("SPARQL query: " + query);

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
			String test2 = Olap4ldLinkedDataUtil.convertStreamToString(nx);
			Olap4ldUtil._log.info("NX output: " + test2);
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

			con.close();
			boas.close();
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
	 * Loads location into store and mark it as stored.
	 * 
	 * @param location
	 */
	private void loadInStore(String location) {
		try {
			Olap4ldUtil._log.info("Load in store: " + location);

			SailRepositoryConnection con = repo.getConnection();
			URL url = new URL(location);

			if (location.endsWith(".rdf") || location.endsWith(".xml")) {
				con.add(url, url.toString(), RDFFormat.RDFXML);
			} else if (location.endsWith(".ttl")) {
				con.add(url, url.toString(), RDFFormat.TURTLE);
			} else {

				// Guess file format
				RDFFormat format = RDFFormat.forFileName(location);
				con.add(url, url.toString(), format);

				// throw new UnsupportedOperationException(
				// "How is the RDF encoded of location: " + location + "?");
			}
			locationsMap.put(location.hashCode(), true);
			con.close();

			// Log content
			String query = "select * where {?s ?p ?o} limit 100";
			// query =
			// "PREFIX dcterms: <http://purl.org/dc/terms/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dc: <http://purl.org/dc/elements/1.1/> PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#> PREFIX qb: <http://purl.org/linked-data/cube#> PREFIX xkos: <http://purl.org/linked-data/xkos#> PREFIX owl: <http://www.w3.org/2002/07/owl#> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> SELECT * WHERE { ?CUBE_NAME qb:structure ?dsd. ?dsd qb:component ?compSpec. ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME rdfs:range ?range FILTER (?range != skos:Concept). ?obs qb:dataSet ?CUBE_NAME. ?obs ?DIMENSION_UNIQUE_NAME ?MEMBER_UNIQUE_NAME.   } ";
			sparql(query, false);

		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * @param location
	 * @return boolean value say whether location already loaded
	 */
	private boolean isStored(String location) {
		return this.locationsMap.containsKey(location.hashCode());
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
	 * 
	 * 
	 * Get Cubes from the triple store.
	 * 
	 * Here, the restrictions are strict restrictions without patterns.
	 * 
	 * 
	 * 
	 * @return Node[]{}
	 */
	public List<Node[]> getCubes(Restrictions restrictions) {

		checkSufficientInformationGathered(restrictions);

		String additionalFilters = createFilterForRestrictions(restrictions);

		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("sesame_getCubes_regular.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> result = sparql(querytemplate, true);

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
	 * Depending on the restrictions, we check whether sufficient Linked Data
	 * has been gathered. If not, we gather more or throw exception.
	 * 
	 * @param restrictions
	 */
	private void checkSufficientInformationGathered(Restrictions restrictions) {
		// For now, if only cube is asked for, we load ds and dsd and run checks
		/*
		 * && restrictions.dimensionUniqueName == null &&
		 * restrictions.hierarchyUniqueName == null &&
		 * restrictions.levelUniqueName == null && restrictions.memberUniqueName
		 * == null
		 */
		if (restrictions.cubeNamePattern != null) {
			String uri = Olap4ldLinkedDataUtil
					.convertMDXtoURI(restrictions.cubeNamePattern);

			try {
				String location = askForLocation(uri);

				// Here, we always load, since we want to update.

				if (!isStored(location)) {
					loadInStore(location);

					// For everything else: Check whether really cube
					RepositoryConnection con;
					con = repo.getConnection();

					String testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> ASK { ?CUBE_NAME a qb:DataSet. FILTER (?CUBE_NAME = <"
							+ uri + ">)}";
					BooleanQuery booleanQuery = con.prepareBooleanQuery(
							QueryLanguage.SPARQL, testquery);
					boolean isDataset = booleanQuery.evaluate();
					con.close();

					if (isDataset) {

						// If loading ds, also load dsd. Ask for DSD URI and
						// load
						String query = "PREFIX qb: <http://purl.org/linked-data/cube#> SELECT ?dsd WHERE {<"
								+ uri + "> qb:structure ?dsd}";
						List<Node[]> dsd = sparql(query, true);
						// There should be a dsd
						// Note in spec:
						// "Every qb:DataSet has exactly one associated qb:DataStructureDefinition."
						if (dsd.size() <= 1) {
							throw new UnsupportedOperationException(
									"A cube should serve a data structure definition!");
						} else {
							String dsduri = dsd.get(1)[0].toString();

							location = askForLocation(dsduri);

							// Since ds and dsd location can be the same,
							// check
							// whether loaded already
							if (!isStored(location)) {
								loadInStore(location);
							}
						}

						// Now, we run checks.

						con = repo.getConnection();

						// IC-1. Unique DataSet. Every qb:Observation
						// has exactly one associated qb:DataSet. <=
						// takes too long since every observation tested

						// IC-2. Unique DSD. Every qb:DataSet has
						// exactly one associated
						// qb:DataStructureDefinition. <= tested before

						// IC-3. DSD includes measure
						// XXX: Not fully like in spec
						testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> ASK { ?dsd a qb:DataStructureDefinition . FILTER NOT EXISTS { ?dsd qb:component [qb:measure [a qb:MeasureProperty]] }}";
						booleanQuery = con.prepareBooleanQuery(
								QueryLanguage.SPARQL, testquery);
						if (booleanQuery.evaluate() == true) {
							throw new UnsupportedOperationException(
									"Failed specification check: IC-3. DSD includes measure. Every qb:DataStructureDefinition must include at least one declared measure. ");
						}

						// IC-4. Dimensions have range
						testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ASK { ?dim a qb:DimensionProperty . FILTER NOT EXISTS { ?dim rdfs:range [] }}";
						booleanQuery = con.prepareBooleanQuery(
								QueryLanguage.SPARQL, testquery);
						if (booleanQuery.evaluate() == true) {
							throw new UnsupportedOperationException(
									"Failed specification check: IC-4. Dimensions have range. Every dimension declared in a qb:DataStructureDefinition must have a declared rdfs:range.");
						}

						// IC-5. Concept dimensions have code lists
						testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> ASK { ?dim a qb:DimensionProperty ; rdfs:range skos:Concept . FILTER NOT EXISTS { ?dim qb:codeList [] }}";
						booleanQuery = con.prepareBooleanQuery(
								QueryLanguage.SPARQL, testquery);
						if (booleanQuery.evaluate() == true) {
							throw new UnsupportedOperationException(
									"Failed specification check: IC-14. All measures present. Every dimension with range skos:Concept must have a qb:codeList. ");
						}

						// IC-6. Only attributes may be optional <= not
						// important right now. We do not regard
						// attributes.

						// IC-7. Slice Keys must be declared <= not
						// important right now. We do not regard slices.
						// IC-8. Slice Keys consistent with DSD
						// IC-9. Unique slice structure
						// IC-10. Slice dimensions complete

						// IC-11. All dimensions required <= takes too
						// long
						// IC-12. No duplicate observations <= takes too
						// long

						// IC-13. Required attributes <= We do not
						// regard attributes

						// IC-14. All measures present
						testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> ASK { ?obs qb:dataSet/qb:structure ?dsd . FILTER NOT EXISTS { ?dsd qb:component/qb:componentProperty qb:measureType } ?dsd qb:component/qb:componentProperty ?measure . ?measure a qb:MeasureProperty; FILTER NOT EXISTS { ?obs ?measure [] }}";
						booleanQuery = con.prepareBooleanQuery(
								QueryLanguage.SPARQL, testquery);
						if (booleanQuery.evaluate() == true) {
							throw new UnsupportedOperationException(
									"Failed specification check: IC-14. In a qb:DataSet which does not use a Measure dimension then each individual qb:Observation must have a value for every declared measure. ");
						}

						// IC-15. Measure dimension consistent <= We do
						// not support measureType, yet.
						// IC-16. Single measure on measure dimension
						// observation

						// Own check: Dataset should have at least one
						// observation
						testquery = "PREFIX qb: <http://purl.org/linked-data/cube#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> ASK { ?obs qb:dataSet ?CUBE_NAME FILTER (?CUBE_NAME = <"
								+ uri + ">)}";
						booleanQuery = con.prepareBooleanQuery(
								QueryLanguage.SPARQL, testquery);
						if (booleanQuery.evaluate() == false) {
							throw new UnsupportedOperationException(
									"Failed own check: Dataset should have at least one observation. ");
						}

						// XXX Possible other checks
						// No dimensions
						// No aggregation function
						// Code list empty
						// No member

						// Important!
						con.close();

					}
				}

			} catch (RepositoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
	public List<Node[]> getDimensions(Restrictions restrictions) {

		checkSufficientInformationGathered(restrictions);

		String additionalFilters = createFilterForRestrictions(restrictions);

		// Get all dimensions
		String querytemplate = Olap4ldLinkedDataUtil
				.readInQueryTemplate("sesame_getDimensions_regular.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
				askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{FILTERS}}}",
				additionalFilters);

		List<Node[]> result = sparql(querytemplate, true);

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
			querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getDimensions_measure_dimension.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					specificFilters);

			List<Node[]> result2 = sparql(querytemplate, true);

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

		checkSufficientInformationGathered(restrictions);

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

		// List<Node[]> result = applyRestrictions(measureUris, restrictions);
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
	public List<Node[]> getHierarchies(Restrictions restrictions) {

		checkSufficientInformationGathered(restrictions);

		List<Node[]> result = new ArrayList<Node[]>();

		// Create header
		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?DIMENSION_UNIQUE_NAME"),
				new Variable("?HIERARCHY_UNIQUE_NAME"),
				new Variable("?HIERARCHY_NAME"),
				new Variable("?HIERARCHY_CAPTION"),
				new Variable("?DESCRIPTION") };
		result.add(header);

		if (!isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			String additionalFilters = createFilterForRestrictions(restrictions);

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
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getHierarchies_measure_dimension.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					specificFilters);

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
		if (isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)
				|| (restrictions.hierarchyUniqueName != null && !restrictions.hierarchyUniqueName
						.equals(restrictions.dimensionUniqueName))) {
			;
			// we do not need to do this
		} else {

			String additionalFilters = createFilterForRestrictions(restrictions);

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

		checkSufficientInformationGathered(restrictions);

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

		if (!isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)) {

			String additionalFilters = createFilterForRestrictions(restrictions);

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
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getLevels_measure_dimension.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					specificFilters);

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
		if (isMeasureQueriedFor(restrictions.dimensionUniqueName,
				restrictions.hierarchyUniqueName, restrictions.levelUniqueName)
				|| (restrictions.hierarchyUniqueName != null && !restrictions.hierarchyUniqueName
						.equals(restrictions.dimensionUniqueName))) {
			// we do not need to do this
			;
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

			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getLevels_without_codelist.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					specificFilters);

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

		checkSufficientInformationGathered(restrictions);

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

		// Regular members
		if (result.size() == 1) {

			intermediaryresult = getHasTopConceptMembers(restrictions);

			addToResult(intermediaryresult, result);
		}

		// Xkos members
		// Watch out: No square brackets
		if ((restrictions.dimensionUniqueName == null || !restrictions.dimensionUniqueName
				.equals("Measures"))
				|| (restrictions.hierarchyUniqueName == null || !restrictions.hierarchyUniqueName
						.equals("Measures"))
				|| (restrictions.levelUniqueName == null || !restrictions.levelUniqueName
						.equals("Measures"))) {

			intermediaryresult = getXkosMembers(restrictions);

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
				Olap4ldUtil._log.info("TreeOp:CHILDREN");

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.info("TreeOp:SIBLINGS");

				if (restrictions.cubeNamePattern != null) {
					additionalFilters += " FILTER (?CUBE_NAME = <"
							+ Olap4ldLinkedDataUtil
									.convertMDXtoURI(restrictions.cubeNamePattern)
							+ ">) ";
				}
			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.info("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.info("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.info("TreeOp:ANCESTORS");
			}

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.info("TreeOp:SELF");

			if (restrictions.cubeNamePattern != null) {
				additionalFilters += " FILTER (?CUBE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.cubeNamePattern)
						+ ">) ";
			}
			if (restrictions.memberUniqueName != null) {
				additionalFilters += " FILTER (?MEMBER_UNIQUE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.memberUniqueName)
						+ ">) ";
			}

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
				Olap4ldUtil._log.info("TreeOp:CHILDREN");

				// Here, we need a specific filter
				additionalFilters = " FILTER (?PARENT_UNIQUE_NAME = <"
						+ Olap4ldLinkedDataUtil
								.convertMDXtoURI(restrictions.memberUniqueName)
						+ ">) ";

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.info("TreeOp:SIBLINGS");

			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.info("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.info("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.info("TreeOp:ANCESTORS");
			}

			throw new UnsupportedOperationException(
					"TreeOp and getLevelMember failed.");

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.info("TreeOp:SELF");

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
				Olap4ldUtil._log.info("TreeOp:CHILDREN");

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.info("TreeOp:SIBLINGS");

			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.info("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.info("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.info("TreeOp:ANCESTORS");
			}

			throw new UnsupportedOperationException(
					"TreeOp and getLevelMember failed.");

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.info("TreeOp:SELF");

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
			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getMembers_topConcept.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					specificFilters);

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

		if (restrictions.tree != null && (restrictions.tree & 8) != 8) {

			// Assumption 1: Treeop only uses Member
			if (restrictions.memberUniqueName == null) {
				throw new UnsupportedOperationException(
						"If a treeMask is given, we should also have a unique member name!");
			}

			if ((restrictions.tree & 1) == 1) {
				// CHILDREN
				Olap4ldUtil._log.info("TreeOp:CHILDREN");

			}
			if ((restrictions.tree & 2) == 2) {
				// SIBLINGS
				Olap4ldUtil._log.info("TreeOp:SIBLINGS");

			}
			if ((restrictions.tree & 4) == 4) {
				// PARENT
				Olap4ldUtil._log.info("TreeOp:PARENT");
			}
			if ((restrictions.tree & 16) == 16) {
				// DESCENDANTS
				Olap4ldUtil._log.info("TreeOp:DESCENDANTS");

			}
			if ((restrictions.tree & 32) == 32) {
				// ANCESTORS
				Olap4ldUtil._log.info("TreeOp:ANCESTORS");
			}

			throw new UnsupportedOperationException(
					"TreeOp and getLevelMember failed.");

		} else {
			// TreeOp = Self or null
			Olap4ldUtil._log.info("TreeOp:SELF");

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

			if (dimensionWithoutHierarchies == null) {

				;

			} else {

				;

			}

			String querytemplate = Olap4ldLinkedDataUtil
					.readInQueryTemplate("sesame_getMembers_degenerated.txt");
			querytemplate = querytemplate.replace("{{{STANDARDFROM}}}",
					askForFrom(true));
			querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
			querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}",
					TABLE_SCHEM);
			querytemplate = querytemplate.replace("{{{FILTERS}}}",
					alteredadditionalFilters);

			memberUris1 = sparql(querytemplate, true);

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

	@Override
	public List<Node[]> getOlapResult(LogicalOlapQueryPlan queryplan) {
		// Log logical query plan
		Olap4ldUtil._log.info("Logical query plan to string: "
				+ queryplan.toString());

		LogicalOlap2PhysicalOlap r2a = new LogicalOlap2PhysicalOlap(repo);

		ExecIterator newRoot;
		try {
			newRoot = (ExecIterator) queryplan.visitAll(r2a);

			Olap4ldUtil._log.info("bytes iterator " + newRoot);

			ExecPlan ap = new ExecPlan(newRoot);

			ExecIterator resultIterator = ap.getIterator();

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
	public List<Node[]> getOlapResult(Cube cube, List<Level> slicesrollups,
			List<Position> dices, List<Measure> projections) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Empties store and locationMap.
	 */
	public void rollback() {

		// Store
		try {
			// Rely on garbage collector
			// this.repo.initialize();
			// this.repo.shutDown();
			this.repo = new SailRepository(new MemoryStore());
			repo.initialize();

			// do something interesting with the values here...
			// con.close();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		locationsMap.clear();
	}

}
