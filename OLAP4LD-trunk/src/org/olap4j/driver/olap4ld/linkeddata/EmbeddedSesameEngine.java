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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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
 * The EmbeddedSesameEngine manages an embedded Sesame repository (triple store) while
 * executing metadata or olap queries. 
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

	private static final String TABLE_CAT = "LdCatalog";

	private static final String TABLE_SCHEM = "LdSchema";

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
		
		//We actually do not need that.
		URL = serverUrlObject.toString();
		
		//Those given URIs we load and add to the location map
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
	 * @param caching (not used)
	 * @return
	 */
	private List<Node[]> sparql(String query, boolean caching) {

		Olap4ldUtil._log.info("SPARQL query: " + query);

		List<Node[]> myBindings = new ArrayList<Node[]>();

		try {
			RepositoryConnection con = repo.getConnection();
			
			ByteArrayOutputStream boas =   new ByteArrayOutputStream();
			
			SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(
					boas);

			TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL,
					query);
			tupleQuery.evaluate(sparqlWriter);
			
			ByteArrayInputStream bais = new ByteArrayInputStream(boas.toByteArray());
			
			//String xmlwriterstreamString = Olap4ldLinkedDataUtil.convertStreamToString(bais);
			//System.out.println(xmlwriterstreamString);
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

		// If specific data cube asked for, get URI
		if (restrictions.cubeNamePattern != null) {
			String uri = Olap4ldLinkedDataUtil
					.convertMDXtoURI(restrictions.cubeNamePattern);
			try {
				String location = askForLocation(uri);

				// If not in store: load location
				if (!isStored(location)) {
					loadInStore(location);
				}

			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		String additionalFilters = createFilterForRestrictions(restrictions);
		
		String querytemplate = Olap4ldLinkedDataUtil.readInQueryTemplate("sesame_getCubes_regular.txt");
		querytemplate = querytemplate.replace("{{{STANDARDFROM}}}", askForFrom(true));
		querytemplate = querytemplate.replace("{{{TABLE_CAT}}}", TABLE_CAT);
		querytemplate = querytemplate.replace("{{{TABLE_SCHEM}}}", TABLE_SCHEM);
		querytemplate = querytemplate.replace("{{{ADDITIONALFILTERS}}}", additionalFilters);

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
	 * Loads location into store and mark it as stored.
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
				throw new UnsupportedOperationException("How is the RDF encoded of location: "+location+"?");
			}
			locationsMap.put(location.hashCode(), true);
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
		return this.locationsMap.containsValue(location.hashCode());
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
				Olap4ldUtil._log.info("TreeOp:CHILDREN");

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
			String query = null;
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

	@Override
	public List<Node[]> getOlapResult(LogicalOlapQueryPlan queryplan) {
		// Log logical query plan
		Olap4ldUtil._log.finer(queryplan.toString());

		LogicalOlap2PhysicalOlap r2a = new LogicalOlap2PhysicalOlap();

		ExecIterator newRoot;
		try {
			newRoot = (ExecIterator) queryplan.visitAll(r2a);

			Olap4ldUtil._log.finer("bytes iterator " + newRoot);

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

}
