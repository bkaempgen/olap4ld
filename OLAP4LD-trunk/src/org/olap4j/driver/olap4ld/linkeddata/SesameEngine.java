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
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Implements methods of XmlaOlap4jDatabaseMetadata, returning the specified
 * columns as nodes from a sparql endpoint.
 * 
 * @author b-kaempgen
 * 
 */
public class SesameEngine implements LinkedDataCubesEngine {

	/**
	 * The type of SPARQL endpoint should be found out automatically and with
	 * the server string
	 */
	private static String SPARQLSERVERURL;

	private static final String STANDARDPREFIX = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix skos: <http://www.w3.org/2004/02/skos/core#> PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>\n PREFIX dcterms: <http://purl.org/dc/terms/>\n PREFIX eus: <http://ontologycentral.com/2009/01/eurostat/ns#>\n PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n PREFIX qb: <http://purl.org/linked-data/cube#>\n PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n PREFIX dc: <http://purl.org/dc/elements/1.1/>\n PREFIX skosclass: <http://ddialliance.org/ontologies/skosclass#>\n ";

	private static final String DATASOURCEDESCRIPTION = "OLAP data from the statistical Linked Data cloud.";

	private static final String PROVIDERNAME = "The community.";

	private static String URL;

	private static final String DATASOURCEINFO = "Data following the Linked Data principles.";

	private static final String CATALOG_NAME = "LdCatalog";

	private static final String DESCRIPTION = "Catalog of Linked Data.";

	private static final String SCHEMA_NAME = "LdSchema";

	/*
	 * Array of datasets of the database/catalog/schema that shall be integrated
	 */
	public String DATASOURCENAME;
	public String DATASOURCEVERSION;
	private ArrayList<String> datastructuredefinitions;
	private ArrayList<String> datasets;

	private HashMap<Integer, List<Node[]>> sparqlResultMap = new HashMap<Integer, List<Node[]>>();

	public SesameEngine(URL serverUrlObject,
			ArrayList<String> datastructuredefinitions,
			ArrayList<String> datasets, String databasename) {
		URL = serverUrlObject.toString();
		this.datastructuredefinitions = datastructuredefinitions;
		this.datasets = datasets;

		// TODO: I could add another version with a specific rule set for open
		// virtuoso
		if (databasename.equals("SESAME")) {
			this.DATASOURCENAME = databasename;
			this.DATASOURCEVERSION = "1.0";
		}
		// Set URL
		SPARQLSERVERURL = serverUrlObject.toString();
	}

	/**
	 * Returns from String in order to retrieve information about these URIs
	 * 
	 * 
	 * Properly access the triple store: For DSD, we query a specific named
	 * graph that we put the DSDs in (could be much better). For data, we query
	 * the default graph.
	 * 
	 * @param uris
	 * @return fromResult
	 */
	private String askForFrom(boolean isDsdQuery) {

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
			}
		}
		return fromResult;
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
		Integer hash = null;
		if (caching) {
			hash = query.hashCode();
		}
		if (caching) {
			if (this.sparqlResultMap.containsKey(hash)) {
				return this.sparqlResultMap.get(hash);
			}
		}

		Olap4ldUtil._log.info("SPARQL query: " + query);
		List<Node[]> result;

		result = sparql(query);

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



	private List<Node[]> sparql(String query) {
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
			con.setRequestProperty("Accept", "application/sparql-results+xml, */*;q=0.5");
			con.setRequestMethod("POST");

			 InputStream inputStream = con.getInputStream();
			 String test = Olap4ldLinkedDataUtil.convertStreamToString(inputStream);
			 Olap4ldUtil._log.info("XML output: " + test);
			
			if (con.getResponseCode() != 200) {
				throw new RuntimeException("lookup on " + fullurl
						+ " resulted HTTP in status code "
						+ con.getResponseCode());
			}

			// Transform sparql xml to nx
			InputStream nx = Olap4ldLinkedDataUtil.transformSparqlXmlToNx(con
					.getInputStream());
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

	private List<Node[]> applyRestrictions(List<Node[]> result,
			Restrictions restrictions) {
		
		/*
		 * We go through all the nodes. For each node, if it does not fulfill
		 * one of the restrictions, we remove that node from the result (lets
		 * see how that works).
		 * 
		 * Removing is bad, since this result may come from a cache. Therefore I
		 * create an add list and then return this.
		 */
		boolean isFirst = true;
		Map<String, Integer> mapFields = null;
		ArrayList<Node[]> addList = new ArrayList<Node[]>();
//		for (Node[] node : result) {
//			boolean isToRemove = false;
//			if (isFirst) {
//				mapFields = Olap4ldLinkedDataUtil.getNodeResultFields(node);
//				isFirst = false;
//			} else {
//				// restrictions are structured: %2=0 restriction name, %2=1
//				// restriction value
//				for (int i = 0; i <= restrictions.length - 1; i = i + 2) {
//					// TREE_OP is special, needs to be handled differently
//					if (restrictions[i].toString().equals("TREE_OP")) {
//
//						// TODO: In this case, we should prioritize treeOp
//
//						int tree = new Integer(restrictions[i + 1].toString());
//						if ((tree & 1) == 1) {
//							// CHILDREN
//							Olap4ldUtil._log.info("TreeOp:CHILDREN");
//						} else {
//							// Remove all children
//							// TODO: So far, we do not support children.
//						}
//						if ((tree & 2) == 2) {
//							// SIBLINGS
//							Olap4ldUtil._log.info("TreeOp:SIBLINGS");
//
//						} else {
//							// How to remove all siblings?
//							/*
//							 * We can assume that we are at a specific cube,
//							 * dim, hier, level
//							 */
//						}
//						if ((tree & 4) == 4) {
//							// PARENT
//							Olap4ldUtil._log.info("TreeOp:PARENT");
//						} else {
//							// Remove all parents
//							// TODO: So far, we do not support parents.
//						}
//						if ((tree & 8) == 8) {
//							// SELF
//							Olap4ldUtil._log.info("TreeOp:SELF");
//						} else {
//							// How can we remove only oneself?
//
//						}
//						if ((tree & 16) == 16) {
//							// DESCENDANTS
//							Olap4ldUtil._log.info("TreeOp:DESCENDANTS");
//
//						} else {
//							// Remove all descendants
//							// TODO: So far, we do not support descendants.
//						}
//						if ((tree & 32) == 32) {
//							// ANCESTORS
//							Olap4ldUtil._log.info("TreeOp:ANCESTORS");
//						} else {
//							// Remove all ancestors
//							// TODO: So far, we do not support ancestors.
//						}
//
//					} else {
//
//						String restriction = restrictions[i + 1].toString();
//						String value = Olap4ldLinkedDataUtil
//								.convertNodeToMDX(node[mapFields.get("?"
//										+ restrictions[i].toString())]);
//
//						if (restriction == null || restriction.equals(value)) {
//							// fine
//						} else {
//							isToRemove = true;
//							// TODO does it jump out of the whole thing?
//							// We need to go to the next node
//							break;
//						}
//					}
//				}
//			}
//			// If node is marked as delete, nothing, else add it to addList
//			if (!isToRemove) {
//				addList.add(node);
//			}
//		}
		return addList;
	}

	/**
	 * 
	 * @return
	 */
	public List<Node[]> getDatabases() {

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

	public List<Node[]> getCatalogs() {
		/*
		 * DBSCHEMA_CATALOGS( new MetadataColumn("CATALOG_NAME"), new
		 * MetadataColumn( "DESCRIPTION"), new MetadataColumn("ROLES"), new
		 * MetadataColumn("DATE_MODIFIED"))
		 */
		List<Node[]> results = new ArrayList<Node[]>();

		Node[] bindingNames = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?DESCRIPTION"), new Variable("?ROLES"),
				new Variable("?DATE_MODIFIED") };
		results.add(bindingNames);

		Node[] triple = new Node[] { new Literal(CATALOG_NAME),
				new Literal(DESCRIPTION),
				// No role and date modified
				new Literal(""), new Literal(""), };
		results.add(triple);

		return results;
	}

	/**
	 * 
	 * @return
	 */
	public List<Node[]> getSchemas() {
		List<Node[]> results = new ArrayList<Node[]>();
		/*
		 * DBSCHEMA_SCHEMATA(new MetadataColumn( "CATALOG_NAME"), new
		 * MetadataColumn("SCHEMA_NAME"), new MetadataColumn("SCHEMA_OWNER"))
		 */
		Node[] bindingNames = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?SCHEMA_OWNER") };
		results.add(bindingNames);

		Node[] triple = new Node[] { new Literal(CATALOG_NAME),
				new Literal(SCHEMA_NAME),
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
	public List<Node[]> getCubes(Restrictions restrictions) throws OlapException {

		
		// If new cube is created, I empty the cache of the Linked Data Engine
		this.emptySparqlResultCache();
		
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

		// TODO: For now, we only use the non-language-tag CAPTION and
		// DESCRIPTION
		String query = STANDARDPREFIX
				+ "select ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?CUBE_TYPE ?DESCRIPTION ?CUBE_CAPTION "
				+ askForFrom(true)
				+ "where { BIND('"+CATALOG_NAME+"' as ?CATALOG_NAME). BIND('"+SCHEMA_NAME+"' as ?SCHEMA_NAME). BIND('CUBE' as ?CUBE_TYPE). ?CUBE_NAME a qb:DataStructureDefinition. OPTIONAL {?CUBE_NAME rdfs:label ?CUBE_CAPTION FILTER ( lang(?CUBE_CAPTION) = \"\" )} OPTIONAL {?CUBE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"\" )} }"
				+ "order by ?CUBE_NAME ";

		List<Node[]> cubeUris = sparql(query, true);

		/*
		 * Check on restrictions that the interface makes:
		 * 
		 * Restrictions are strong restrictions, no fuzzy, since those wild
		 * cards have been eliminated before.
		 */
		List<Node[]> result = applyRestrictions(cubeUris, restrictions);
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
	public List<Node[]> getDimensions(Restrictions restrictions) throws OlapException {

		// Get all dimensions
		String query = "";
		query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME as ?DIMENSION_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_CAPTION ?DESCRIPTION \"0\" as ?DIMENSION_TYPE \"0\" as ?DIMENSION_ORDINAL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:label ?DIMENSION_CAPTION FILTER ( lang(?DIMENSION_CAPTION) = \"\" )} OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"\" )} } "
				+ "order by ?CUBE_NAME ?DIMENSION_NAME ";

		List<Node[]> dimensionUris = sparql(query, true);

		/*
		 * Get all measures: We query for all cubes and simply add measure
		 * information
		 */

		List<Node[]> result = applyRestrictions(dimensionUris, restrictions);

		query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?DIMENSION_CAPTION \"Measures\" as ?DESCRIPTION \"2\" as ?DIMENSION_TYPE \"0\" as ?DIMENSION_ORDINAL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:measure ?DIMENSION_UNIQUE_NAME. OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:label ?DIMENSION_CAPTION FILTER ( lang(?DIMENSION_CAPTION) = \"\" )} } "
				+ "order by ?CUBE_NAME ?DIMENSION_NAME limit 1";

		List<Node[]> memberUris2 = sparql(query, true);

		List<Node[]> result2 = applyRestrictions(memberUris2, restrictions);

		// Add all of result2 to result
		boolean first = true;
		for (Node[] nodes : result2) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
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
	public List<Node[]> getMeasures(Restrictions restrictions) throws OlapException {

		// ///////////QUERY//////////////////////////
		/*
		 * TODO: How to consider equal measures?
		 */

		// Boolean values need to be returned as "true" or "false".
		// Get all measures
		String query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME  ?DIMENSION_UNIQUE_NAME as ?MEASURE_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?MEASURE_NAME ?DIMENSION_UNIQUE_NAME as ?MEASURE_CAPTION \"5\" as ?DATA_TYPE \"true\" as ?MEASURE_IS_VISIBLE \"5\" as ?MEASURE_AGGREGATOR "
				+ askForFrom(true)
				// Important: Only measures are queried
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:measure ?DIMENSION_UNIQUE_NAME. OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:label ?MEASURE_CAPTION FILTER ( lang(?MEASURE_CAPTION) = \"\" )} OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:comment ?DESCRIPTION FILTER ( lang(?DESCRIPTION) = \"\" )} } "
				+ "order by ?CUBE_NAME ?MEASURE_NAME ";
		List<Node[]> measureUris = sparql(query, true);

		List<Node[]> result = applyRestrictions(measureUris, restrictions);
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
	public List<Node[]> getHierarchies(Restrictions restrictions) throws OlapException {

		// Get all hierarchies
		String query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?HIERARCHY_NAME ?HIERARCHY_CAPTION ?DESCRIPTION "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. OPTIONAL {?HIERARCHY_UNIQUE_NAME rdfs:label ?HIERARCHY_CAPTION FILTER ( lang(?HIERARCHY_CAPTION) = \"\" ) } OPTIONAL {?HIERARCHY_UNIQUE_NAME rdfs:comment ?DESCRIPTION } } "
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_NAME ";
		List<Node[]> hierarchyResults = sparql(query, true);

		// TODO: No sorting done, yet

		// TODO: How about dimensions without a hierarchy?

		List<Node[]> result = applyRestrictions(hierarchyResults, restrictions);

		// Get measure dimensions
		query = STANDARDPREFIX
				+ "select distinct \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?HIERARCHY_UNIQUE_NAME \"Measures\" as ?HIERARCHY_NAME \"Measures\" as ?HIERARCHY_CAPTION \"Measures\" as ?DESCRIPTION "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:measure ?DIMENSION_UNIQUE_NAME. "
				+ "} order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_NAME  limit 1";

		List<Node[]> memberUris2 = sparql(query, true);

		List<Node[]> result2 = applyRestrictions(memberUris2, restrictions);

		// Add all of result2 to result
		boolean first = true;
		for (Node[] nodes : result2) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		// Get dimension hierarchies without codeList
		query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?HIERARCHY_NAME ?HIERARCHY_CAPTION ?DESCRIPTION "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. FILTER NOT EXISTS { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. } OPTIONAL { ?DIMENSION_UNIQUE_NAME rdfs:label ?HIERARCHY_CAPTION } OPTIONAL {?DIMENSION_UNIQUE_NAME rdfs:comment ?DESCRIPTION } } "
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ";

		List<Node[]> memberUris3 = sparql(query, true);

		List<Node[]> result3 = applyRestrictions(memberUris3, restrictions);

		// Add all of result2 to result
		first = true;
		for (Node[] nodes : result3) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
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
	public List<Node[]> getLevels(Restrictions restrictions) throws OlapException {

		// Get all levels
		String query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?LEVEL_CAPTION \"Root-Level\" as ?LEVEL_NAME \"N/A\" as ?DESCRIPTION \"0\" as ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. }"
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_NUMBER ";
		List<Node[]> levelResults = sparql(query, true);

		List<Node[]> result = applyRestrictions(levelResults, restrictions);

		// TODO: No sorting done, yet

		// TODO: Add properly modeled level

		// Add measures levels
		query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?HIERARCHY_UNIQUE_NAME \"Measures\" as ?LEVEL_UNIQUE_NAME \"Measures\" as ?LEVEL_CAPTION \"Measures\" as ?LEVEL_NAME \"Measures\" as ?DESCRIPTION \"0\" as ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:measure ?DIMENSION_UNIQUE_NAME. } "
				+ "order by ?DIMENSION_UNIQUE_NAME  limit 1";

		// Second, ask for the measures (which are also members)
		List<Node[]> memberUris2 = sparql(query, true);

		List<Node[]> result2 = applyRestrictions(memberUris2, restrictions);

		// Add all of result2 to result
		boolean first = true;
		for (Node[] nodes : result2) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		// Add levels for dimensions without codelist
		query = STANDARDPREFIX
				+ "select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?LEVEL_CAPTION ?DIMENSION_UNIQUE_NAME as ?LEVEL_NAME \"N/A\" as ?DESCRIPTION \"0\" as ?LEVEL_NUMBER \"0\" as ?LEVEL_CARDINALITY \"0x0000\" as ?LEVEL_TYPE "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec "
				+ ". ?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. FILTER NOT EXISTS { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. } } "
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?LEVEL_NUMBER ";

		// Second, ask for the measures (which are also members)
		List<Node[]> memberUris3 = sparql(query, true);

		List<Node[]> result3 = applyRestrictions(memberUris3, restrictions);

		// Add all of result3 to result
		first = true;
		for (Node[] nodes : result3) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
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
	 * For caption of members, we use
	 * http://www.w3.org/2004/02/skos/core#notation skos:notation, since members
	 * are in rdf represented as skos:Concept and this is the proper way to give
	 * them a representation.
	 * 
	 * @return Node[]{?memberURI ?name}
	 * @throws MalformedURLException
	 */
	public List<Node[]> getMembers(Restrictions restrictions) throws OlapException {
		/*
		 * For each dimension, get the possible members
		 */

		// TODO: Ordering misses ORDINAL (the most important one) - Ad-hoc
		// workaround we replace it by Member unique name
		// TODO: Currently, no special lang filtered for in caption: FILTER (
		// lang(?MEMBER_CAPTION) = \"\")

		// First, ask for all members

		// TODO: Add members of properly modeled levels
		// query = STANDARDPREFIX
		// + "Select \""
		// + CATALOG_NAME
		// + "\" as ?CATALOG_NAME \""
		// + SCHEMA_NAME
		// +
		// "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_NAME ?MEMBER_UNIQUE_NAME ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE ?PARENT_UNIQUE_NAME ?PARENT_LEVEL "
		// + askForFrom(true)
		// + " where { ?CUBE_NAME qb:component ?compSpec. "
		// +
		// "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. { { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?MEMBER_UNIQUE_NAME skos:member ?LEVEL_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME  skos:inScheme ?HIERARCHY_UNIQUE_NAME. ?LEVEL_UNIQUE_NAME skos:depth ?LEVEL_NUMBER. } UNION { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?HIERARCHY_UNIQUE_NAME skos:hasTopConcept ?MEMBER_UNIQUE_NAME. }}"
		// +
		// " OPTIONAL{  ?relationship    rdf:type  skos:ConceptAssociation. ?relationship    skos:inScheme ?HIERARCHY_UNIQUE_NAME. ?relationship    skos:hasDomainConcept ?MEMBER_UNIQUE_NAME. ?relationship    rdf:predicate skos:broaderTransitive. ?relationship    skos:hasRangeConcept ?PARENT_UNIQUE_NAME. ?PARENT_UNIQUE_NAME skos:member ?PARENT_LEVEL_UNIQUE_NAME. ?PARENT_LEVEL_UNIQUE_NAME skos:depth ?PARENT_LEVEL. } "
		// +
		// " OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION FILTER ( lang(?MEMBER_CAPTION) = \"\" )} } "
		// +
		// "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

		// TODO: Quickfix distinct (!)
		String query = STANDARDPREFIX
				+ "Select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"null\" as ?PARENT_LEVEL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec. "
				+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. ?HIERARCHY_UNIQUE_NAME skos:hasTopConcept ?MEMBER_UNIQUE_NAME. "
				+ " OPTIONAL {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION } } "
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

		List<Node[]> memberUris = sparql(query, true);

		List<Node[]> result = applyRestrictions(memberUris, restrictions);

		// Second, ask for the measures (which are also members)
		query = STANDARDPREFIX
				+ "Select \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME \"Measures\" as ?DIMENSION_UNIQUE_NAME \"Measures\" as ?HIERARCHY_UNIQUE_NAME \"Measures\" as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?DIMENSION_UNIQUE_NAME as ?MEMBER_NAME ?DIMENSION_UNIQUE_NAME as ?MEMBER_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?MEMBER_CAPTION \"3\" as ?MEMBER_TYPE \"\" as ?PARENT_UNIQUE_NAME \"\" as ?PARENT_LEVEL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec. "
				+ "?compSpec qb:measure ?DIMENSION_UNIQUE_NAME. } "
				+ "order by ?DIMENSION_UNIQUE_NAME";

		List<Node[]> memberUris2 = sparql(query, true);

		List<Node[]> result2 = applyRestrictions(memberUris2, restrictions);

		// Add all of result2 to result
		boolean first = true;
		for (Node[] nodes : result2) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		/*
		 * Third, ask for members of all dimensions that do not define a
		 * codeList. For now, I use separate queries for skos:Concepts and
		 * Literal values
		 */
		query = STANDARDPREFIX
				+ "Select distinct \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"null\" as ?PARENT_LEVEL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec. "
				+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. FILTER NOT EXISTS { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. } "
				+ "?obs qb:dataSet ?ds. ?ds qb:structure ?CUBE_NAME. ?obs ?DIMENSION_UNIQUE_NAME ?MEMBER_UNIQUE_NAME."
				+ " ?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION } "
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

		List<Node[]> memberUris3 = sparql(query, true);

		List<Node[]> result3 = applyRestrictions(memberUris3, restrictions);

		// Add all of result3 to result
		first = true;
		for (Node[] nodes : result3) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		/*
		 * ... and Literal values / values that do not serve a skos:notation.
		 */
		query = STANDARDPREFIX
				+ "Select distinct \""
				+ CATALOG_NAME
				+ "\" as ?CATALOG_NAME \""
				+ SCHEMA_NAME
				+ "\" as ?SCHEMA_NAME ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?HIERARCHY_UNIQUE_NAME ?DIMENSION_UNIQUE_NAME as ?LEVEL_UNIQUE_NAME \"0\" as ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME as ?MEMBER_NAME ?MEMBER_UNIQUE_NAME ?MEMBER_UNIQUE_NAME as ?MEMBER_CAPTION \"1\" as ?MEMBER_TYPE \"null\" as ?PARENT_UNIQUE_NAME \"null\" as ?PARENT_LEVEL "
				+ askForFrom(true)
				+ " where { ?CUBE_NAME qb:component ?compSpec. "
				+ "?compSpec qb:dimension ?DIMENSION_UNIQUE_NAME. FILTER NOT EXISTS { ?DIMENSION_UNIQUE_NAME qb:codeList ?HIERARCHY_UNIQUE_NAME. } "
				+ "?obs qb:dataSet ?ds. ?ds qb:structure ?CUBE_NAME. ?obs ?DIMENSION_UNIQUE_NAME ?MEMBER_UNIQUE_NAME."
				+ " FILTER NOT EXISTS {?MEMBER_UNIQUE_NAME skos:notation ?MEMBER_CAPTION } } "
				+ "order by ?CUBE_NAME ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME";

		List<Node[]> memberUris31 = sparql(query, true);

		List<Node[]> result31 = applyRestrictions(memberUris31, restrictions);

		// Add all of result3 to result
		first = true;
		for (Node[] nodes : result31) {
			if (first) {
				first = false;
				continue;
			}
			result.add(nodes);
		}

		return result;
	}

	public List<Node[]> getSets(Restrictions restrictions) throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Node[]> getDatabases(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getCatalogs(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getSchemas(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> executeOlapQuery(Cube cube, List<Level> slicesrollups,
			List<Position> dices, List<Measure> projections)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> executeOlapQuery(LogicalOlapQueryPlan queryplan)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	// /**
	// * Creates a SPARQL result for one specific cell.
	// *
	// * Example SPARQL query: PREFIX sdmx-measure:
	// * <http://purl.org/linked-data/sdmx/2009/measure#> PREFIX dcterms:
	// * <http://purl.org/dc/terms/> PREFIX eus:
	// * <http://ontologycentral.com/2009/01/eurostat/ns#> PREFIX rdf:
	// * <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX qb:
	// * <http://purl.org/linked-data/cube#> PREFIX rdfs:
	// * <http://www.w3.org/2000/01/rdf-schema#> prefix owl:
	// * <http://www.w3.org/2002/07/owl#> prefix skos:
	// * <http://www.w3.org/2004/02/skos/core#>
	// *
	// * select distinct * where { ?obs qb:dataSet ?ds. ?ds qb:structure
	// * <http://edgarwrap
	// * .ontologycentral.com/archive/921590/0001002105-11-000241#dsd>. ?obs
	// * <http://edgarwrap.ontologycentral.com/vocab/edgar#segment>
	// * ?segmentMember. ?segmentMemberConcept skos:member
	// * <http://edgarwrap.ontologycentral.com/vocab/edgar#segmentRootLevel>.
	// * ?segmentMemberConcept skos:notation ?segmentMember. ?obs
	// * <http://purl.org/dc/terms/date> ?dateMember. ?dateMemberConcept
	// * skos:member <http://purl.org/dc/terms/dateRootLevel>.
	// ?dateMemberConcept
	// * skos:notation ?dateMember. ?obs
	// * <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?obsValue. }
	// * ORDER BY ?segmentMember ?dateMember
	// *
	// * Issues: How to map the CellSetMetadata and the CellSet? Given we ask
	// for
	// * each value either with ordinal, coordinates, or positions. Our aim
	// should
	// * be to have through the SPARQL query for each cell in the CellSet simply
	// * one value (or, we can have a bunch of them, but then we need to capture
	// * the provenance information). In OLAP4J, getCell return a Cell: A Cell
	// can
	// * have an object value and object properties which we could look into
	// * later. For now, we want to return at least one formattedValue. Of
	// course,
	// * it would be nice to fill several values at once, but maybe for now, we
	// * just query each value separately? We can pre-fetch/calculate and store
	// * values, later. In order to query for a value, we need: The Cube (dsd)
	// The
	// * Dimensions and Members on each axis (dimension property, member concept
	// /
	// * actual values in the data) If no measure is included, the default
	// measure
	// * (aggregation function, measure property) If we query a triple store,
	// what
	// * named graphs to query?
	// *
	// * @param xmlaOlap4jCellSet
	// * @param pos
	// *
	// * @param cube
	// * @param members
	// * @param filtermembers
	// * - at the moment, we assume that this is a flat list of members
	// * that needs to aggregated over
	// * @param measure
	// * @return
	// */
	// public Cell getOlapCellResult(LdOlap4jCellSet xmlaOlap4jCellSet, int pos,
	// LdOlap4jCube cube, List<Member> members,
	// List<Member> filtermembers, Measure measure) {
	//
	// // Note: The cube is surrounded by square brackets.
	// String dsd = LdOlap4jUtil.decodeUriForMdx(cube.getUniqueName()
	// .substring(1, cube.getUniqueName().length() - 1));
	//
	// /*
	// * TODO: Some observations (blank nodes) are returned several time,
	// * therefore the obs are counted and distincted.
	// */
	// String query = STANDARDPREFIX + "select distinct ?obs ?obsValue " + " "
	// + "where { ";
	//
	// String measureProperty = LdOlap4jUtil.decodeUriForMdx(measure
	// .getUniqueName());
	//
	// String memberPart = "";
	// int i = 0;
	// for (Member member : members) {
	// String dimensionProperty = LdOlap4jUtil.decodeUriForMdx(member
	// .getDimension().getUniqueName());
	// String levelConcept = LdOlap4jUtil.decodeUriForMdx(member
	// .getLevel().getUniqueName());
	// String dimensionValueConcept = LdOlap4jUtil.decodeUriForMdx(member
	// .getUniqueName());
	// // It can only be either literal or resource: notion, or
	// // exactMatch
	// String representation = LdOlap4jUtil
	// .getSkosPropertyOfDimension(dimensionProperty);
	//
	// memberPart += "?obs <" + dimensionProperty + "> ?member" + i + "."
	// + "<" + dimensionValueConcept + "> skos:member <"
	// + levelConcept + ">. " + "<" + dimensionValueConcept + "> "
	// + representation + " ?member" + i + " .";
	// i++;
	// }
	//
	// String filterpart = "";
	// // Depending on the size of the filter, we need to do querying.
	// // TODO: Magic number 3 needed?
	// if (filtermembers.size() == 0 || filtermembers.size() >= 3) {
	// // Add the structure
	// filterpart = "?obs qb:dataSet ?ds." + "?ds qb:structure <" + dsd
	// + ">." + memberPart;
	// // Add the measure
	// filterpart += "?obs <" + measureProperty + "> ?obsValue.";
	//
	// } else if (filtermembers.size() == 1) {
	// // If filterpart is 1, then just add it as a next member
	// String dimensionProperty = LdOlap4jUtil
	// .decodeUriForMdx(filtermembers.get(0).getDimension()
	// .getUniqueName());
	// String levelConcept = LdOlap4jUtil.decodeUriForMdx(filtermembers
	// .get(0).getLevel().getUniqueName());
	// String dimensionValue = LdOlap4jUtil.decodeUriForMdx(filtermembers
	// .get(0).getUniqueName());
	//
	// // Add the structure
	// filterpart = "?obs qb:dataSet ?ds." + "?ds qb:structure <" + dsd
	// + ">." + memberPart;
	//
	// // It can only be either literal or resource: notion, or
	// // exactMatch
	// // To make process faster, we ask the skos property.
	// String representation = LdOlap4jUtil
	// .getSkosPropertyOfDimension(dimensionProperty);
	//
	// filterpart += "?obs <" + dimensionProperty + "> ?member" + i + "."
	// + "<" + dimensionValue + "> skos:member <" + levelConcept
	// + ">. " + "<" + dimensionValue + "> " + representation
	// + " ?member" + i + " .";
	//
	// // Add the measure
	// filterpart += "?obs <" + measureProperty + "> ?obsValue.";
	//
	// i++;
	// } else if (filtermembers.size() > 1) {
	// // If filterpart is >1, then UNION it
	// boolean first = true;
	// for (Member filtermember : filtermembers) {
	//
	// String dimensionProperty = LdOlap4jUtil
	// .decodeUriForMdx(filtermember.getDimension()
	// .getUniqueName());
	// String levelConcept = LdOlap4jUtil.decodeUriForMdx(filtermember
	// .getLevel().getUniqueName());
	// String dimensionValue = LdOlap4jUtil
	// .decodeUriForMdx(filtermember.getUniqueName());
	//
	// // It can only be either literal or resource: notion, or
	// // exactMatch
	// String union;
	// if (first) {
	// union = "";
	// } else {
	// union = " UNION ";
	// }
	// first = false;
	//
	// /*
	// * if first union = ""
	// *
	// * { + dataset + memberpart + filtermemberpart + measurepart}
	// *
	// * else
	// *
	// * UNION { + dataset + memberpart + filtermemberpart +
	// * measurepart}
	// */
	//
	// // Only skos properties
	// // To make process faster, we ask the skos property.
	// String representation = LdOlap4jUtil
	// .getSkosPropertyOfDimension(dimensionProperty);
	//
	// // Add the union part and the structure
	// filterpart += union + " { " + " ?obs qb:dataSet ?ds."
	// + "?ds qb:structure <" + dsd + ">." + memberPart;
	//
	// filterpart += " ?obs <" + dimensionProperty + "> ?member" + i
	// + "." + "<" + dimensionValue + "> skos:member <"
	// + levelConcept + ">. " + "<" + dimensionValue + "> "
	// + representation + " ?member .";
	// // Add the measure
	// filterpart += "?obs <" + measureProperty + "> ?obsValue.";
	// filterpart += " } ";
	//
	// i++;
	// }
	// }
	//
	// query += filterpart;
	//
	// query += "}";
	//
	// // These kinds of queries are too often, therefore no cache.
	// List<Node[]> cellNodes = sparql(query, false);
	//
	// if (cellNodes.size() == 2) {
	// // 0 = obs 1 = value
	// return new LdOlap4jCell(xmlaOlap4jCellSet, pos, null,
	// cellNodes.get(1)[1].toString(),
	// Collections.<Property, Object> emptyMap());
	// } else {
	// return new LdOlap4jCell(xmlaOlap4jCellSet, pos, null,
	// (cellNodes.size() - 1) + " values",
	// Collections.<Property, Object> emptyMap());
	// }
	// }

	// /**
	// * In order to have a quick estimation of how many values we will have, we
	// * create a map of all possible dimension members of the levels of the
	// * mentioned members to the number of observations. Here, we do not take
	// * filter into account, therefore, we can have a possible occurrence, and
	// * later, after filtering, we will actually have zero value.
	// *
	// * @param xmlaOlap4jCellSet
	// * @param pos
	// * @param cube
	// * @param members
	// * @param filtermembers
	// * @param measure
	// * @return
	// */
	// public Map<Integer, Integer> getOlapResultEstimation(
	// LdOlap4jCellSet xmlaOlap4jCellSet, int pos, LdOlap4jCube cube,
	// List<Member> members, List<Member> filtermembers, Measure measure) {
	//
	// // Note: The cube is surrounded by square brackets.
	// String dsd = LdOlap4jUtil.decodeUriForMdx(cube.getUniqueName()
	// .substring(1, cube.getUniqueName().length() - 1));
	//
	// String memberPart = "";
	// String conceptSelects = "";
	// int i = 0;
	// for (Member member : members) {
	//
	// String dimensionProperty = LdOlap4jUtil.decodeUriForMdx(member
	// .getDimension().getUniqueName());
	// String levelConcept = LdOlap4jUtil.decodeUriForMdx(member
	// .getLevel().getUniqueName());
	//
	// // To make process faster, we ask the skos property.
	// String representation = LdOlap4jUtil
	// .getSkosPropertyOfDimension(dimensionProperty);
	//
	// memberPart += "?obs <" + dimensionProperty + "> ?member" + i + "."
	// + "?concept" + i + " skos:member <" + levelConcept
	// + ">. ?concept" + i + " " + representation + " ?member" + i
	// + " .";
	//
	// conceptSelects += " ?concept" + i;
	// i++;
	// }
	//
	// /*
	// * TODO: Some observations (blank nodes) are returned several time,
	// * therefore the obs are counted and distincted.
	// */
	// String query = STANDARDPREFIX + "select " + conceptSelects
	// + " count(distinct ?obs) " + " " + "where { ";
	//
	// query += "?obs qb:dataSet ?ds." + "?ds qb:structure <" + dsd + ">.";
	//
	// query += memberPart;
	//
	// String measureProperty = LdOlap4jUtil.decodeUriForMdx(measure
	// .getUniqueName());
	//
	// query += "?obs <" + measureProperty + "> ?obsValue.";
	//
	// query += "} ";
	//
	// List<Node[]> cellNodes = sparql(query, true);
	//
	// HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
	// boolean first = true;
	// for (Node[] nodes : cellNodes) {
	// if (first) {
	// // The first is not interesting
	// first = false;
	// continue;
	// }
	// // Take the number of hierarchies
	// String memberConcat = "";
	// for (int j = 0; j < nodes.length - 1; j++) {
	// memberConcat += nodes[j].toString();
	// }
	// // Measure also concat!
	// memberConcat += measure.getUniqueName();
	// // Value
	// String value = nodes[nodes.length - 1].toString();
	// if (value == null || value.equals("null")) {
	// map.put(memberConcat.hashCode(), 0);
	// } else {
	// map.put(memberConcat.hashCode(), new Integer(value));
	// }
	// }
	//
	// return map;
	// }

}