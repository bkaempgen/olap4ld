package org.olap4j.driver.olap4ld.linkeddata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Slice operator
 * 
 * @author benedikt
 * 
 */
public class ConvertSparqlDerivedDatasetIterator implements
		PhysicalOlapIterator {

	// The different metadata parts
	List<Node[]> cubes;
	List<Node[]> measures;
	List<Node[]> dimensions;
	List<Node[]> hierarchies;
	List<Node[]> levels;
	List<Node[]> members;

	Map<String, Integer> cubemap = null;
	Map<String, Integer> dimensionmap = null;
	Map<String, Integer> measuremap = null;
	Map<String, Integer> hierarchymap = null;
	Map<String, Integer> levelmap = null;
	Map<String, Integer> membermap = null;

	private Iterator<Node[]> iterator;
	private PhysicalOlapIterator inputiterator1;
	private PhysicalOlapIterator inputiterator2;
	private String conversionfunction;

	private SailRepository repo;
	private String triples;
	private String domainUri;
	private String newdataset;
	private String dataset1;
	private String dataset2;

	public ConvertSparqlDerivedDatasetIterator(SailRepository repo,
			PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2, String conversionfunction,
			String domainUri) {

		this.repo = repo;
		this.inputiterator1 = inputiterator1;
		this.inputiterator2 = inputiterator2;
		this.domainUri = domainUri;

		// Prepare conversion function

		this.conversionfunction = conversionfunction;

		List<List<Node[]>> conversionfunction_body_head = null;
		try {
			conversionfunction_body_head = Olap4ldLinkedDataUtil
					.splitandparseN3rule(this.conversionfunction);
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		List<Node[]> bodypatterns = conversionfunction_body_head.get(0);
		List<Node[]> headpatterns = conversionfunction_body_head.get(1);

		// We need two queries, one for the metadata and one for the data.

		// We assume that convert context does not change the metadata and thus
		// root1 returns the metadata.

		/*
		 * However, this is not enough, we need to at least change the cube
		 * name. I.e., we go through all those lists copy everything apart from
		 * cube name.
		 * 
		 * Thus, right at the beginning, we create the new ds name and adapt the
		 * metadata.
		 * 
		 * For now, we assume that the cube has the same metadata as the
		 * previous (first) cube.
		 */

		// Metadata

		prepareMetadata();

		// Data

		prepareData(bodypatterns, headpatterns);

		executeSelectQueryForOutput(bodypatterns, headpatterns);
	}

	private void prepareData(List<Node[]> bodypatterns,
			List<Node[]> headpatterns) {
		try {

			// We assume one or two cubes, only.

			RepositoryConnection con = this.repo.getConnection();

			// Construct query to create triples
			String constructquery = "";

			// ** PREFIXES
			String prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
					+ "PREFIX olap4ld:<http://purl.org/olap4ld/>";
			// ** construct {
			// ** Head:
			// String headold = "_:obs qb:dataSet <"
			// + newdataset
			// + ">. "
			// +
			// "_:obs <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg_eur. "
			// +
			// "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
			// +
			// "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#unit> <http://estatwrap.ontologycentral.com/dic/unit#EUR>. "
			// +
			// "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
			// + "_:obs <http://purl.org/dc/terms/date> ?time.";
			String head = "";
			// ** Add head from Data-Fu program
			for (Node[] headpattern : headpatterns) {
				String headgraphpatternstring = headpattern[0].toN3() + " "
						+ headpattern[1].toN3() + " " + headpattern[2].toN3()
						+ ". \n";
				head += headgraphpatternstring;
			}

			// ** For output cube, add dataset graph patterns (_:obs qb:dataSet
			// <newdataset>.)

			// We assume that always only one new dataset is created

			// Heuristically, the variables for the cubes, we get from the
			// pattern
			// subject. The first from the first, the second by going through
			// (apart from qrl:bindas)
			Node obsvariable = headpatterns.get(0)[0];

			head += obsvariable.toN3() + " qb:dataSet <" + newdataset + ">. \n";

			// DSD. For now, assume same dsd as other dataset.
			// Dsd seems not to be given to new datasets. Therefore, the
			// construct query does not work. Instead, we add the dsd separately
			// (adding triples).
			// Solution: Has to be propagated in select (!)
			head += "<" + newdataset + ">" + " qb:structure ?dsd1. \n";

			// ** For each dimension add dataset graph pattern if not already
			// contained in Data-Fu program or Measure Dimension.
			boolean first = true;
			for (Node[] dimension : dimensions) {
				if (first) {
					first = false;
					continue;
				}

				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					continue;
				}

				if (!isPatternContained(bodypatterns, null,
						dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
						null)) {
					head += obsvariable.toN3()
							+ " "
							+ dimension[dimensionmap
									.get("?DIMENSION_UNIQUE_NAME")].toN3()
							+ " "
							+ Olap4ldLinkedDataUtil.makeUriToVariable(
									dimension[dimensionmap
											.get("?DIMENSION_UNIQUE_NAME")]
											.toString()).toN3() + ". \n";

				}
			}
			// ** For each measure add dataset graph pattern if not already
			// contained in Data-Fu program and not implicit measure
			first = true;
			for (Node[] measure : measures) {
				if (first) {
					first = false;
					continue;
				}
				if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.contains("AGGFUNC")) {
					continue;
				}

				if (!isPatternContained(bodypatterns, null,
						measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
					head += obsvariable.toN3()
							+ " "
							+ measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
									.toN3()
							+ " "
							+ Olap4ldLinkedDataUtil.makeUriToVariable(
									measure[measuremap
											.get("?MEASURE_UNIQUE_NAME")]
											.toString()).toN3() + ". \n";
				}
			}
			// ** }
			// ** where { {
			// ** select
			// ** Select:
			// String select =
			// "?geo ?unit1 ?indic_na1 ?time ((?boneg * 1000000) as ?boneg_eur) ";
			// ** Add select variables for every dimension and measure which is
			// not
			// mentioned as predicate in body.
			String select = "";

			// Add dsd
			select += " ?dsd1 ?dsd2 ";

			// Build hashmap from dimension or measure node and variable node.
			HashMap<Node, Node> selectbodymap = new HashMap<Node, Node>();

			first = true;
			for (Node[] dimension : dimensions) {
				if (first) {
					first = false;
					continue;
				}
				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					continue;
				}

				if (!isPatternContained(bodypatterns, null,
						dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
						null)) {
					// Create variable and add
					Node dimensionvariable = Olap4ldLinkedDataUtil
							.makeUriToVariable(dimension[dimensionmap
									.get("?DIMENSION_UNIQUE_NAME")].toString());
					// What to do with it? Add to a map
					Resource dimensionresource = new Resource(
							dimension[dimensionmap
									.get("?DIMENSION_UNIQUE_NAME")].toString());
					selectbodymap.put(dimensionresource, dimensionvariable);
				}
			}
			// ** For each measure add dataset graph pattern if not already
			// contained in Data-Fu program
			first = true;
			for (Node[] measure : measures) {
				if (first) {
					first = false;
					continue;
				}
				if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.contains("AGGFUNC")) {
					continue;
				}

				if (!isPatternContained(bodypatterns, null,
						measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
					// Create variable and add
					Node measurevariable = Olap4ldLinkedDataUtil
							.makeUriToVariable(measure[measuremap
									.get("?MEASURE_UNIQUE_NAME")].toString());
					// What to do with it? Add to a map
					Resource measureresource = new Resource(
							measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
									.toString());
					selectbodymap.put(measureresource, measurevariable);
				}
			}

			// ** Add all variables from objects in body which are not constants

			for (Node[] bodypattern : bodypatterns) {
				// We add variable to map if no constant
				if (bodypattern[2].toN3().startsWith("?")) {
					selectbodymap.put(bodypattern[1], bodypattern[2]);
				}
			}

			// add bindas variables: Look for qrl:bindas; use subject as
			// variable to select; create select
			// http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas
			for (Node[] bodypattern : bodypatterns) {
				if (bodypattern[1]
						.toN3()
						.equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")) {
					// Add directly to select
					select += " (" + bodypattern[2].toString() + " as "
							+ bodypattern[0].toN3() + ") ";
				}
			}

			// Create full select
			Set<Entry<Node, Node>> selectbodyset = selectbodymap.entrySet();
			for (Entry<Node, Node> entry : selectbodyset) {
				select += " " + entry.getValue().toN3() + " ";
			}

			// ** where {
			// ** Body:
			// String bodyold = "?obs1 qb:dataSet <"
			// + dataset
			// + ">. "
			// +
			// "?obs1 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg. "
			// +
			// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
			// +
			// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#unit> ?unit1. "
			// +
			// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
			// + "?obs1 <http://purl.org/dc/terms/date> ?time. "
			// +
			// "FILTER (?unit1 = <http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR>)";
			String body = "";

			// ** Add body from Data-Fu program apart from "bindas"
			for (Node[] bodypattern : bodypatterns) {
				// Have to check on prefixed version, also?
				if (bodypattern[1]
						.toN3()
						.equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")) {
					continue;
				}
				String bodygraphpattern = bodypattern[0].toN3() + " "
						+ bodypattern[1].toN3() + " " + bodypattern[2].toN3()
						+ ". \n";
				body += bodygraphpattern;
			}

			// Heuristically, the variable we get from the first pattern
			// subject.
			// For now, we assume only one cube
			// XXX: and use index if there are several.
			obsvariable = bodypatterns.get(0)[0];

			Node obsvariable2 = null;
			if (dataset2 != null) {
				// Then we should find one
				for (Node[] bodypattern : bodypatterns) {
					if (!bodypattern[1]
							.toN3()
							.equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")
							&& !bodypattern[0].equals(bodypatterns.get(0)[0])) {
						obsvariable2 = bodypattern[0];
					}
				}
			}

			body += obsvariable.toN3() + " qb:dataSet <" + dataset1 + ">. \n";

			// DSD. Use ?dsd1 for 1, ?dsd2 for 2.
			// Dsd seems not to be given to new datasets. Therefore, the
			// construct query does not work. Instead, we add the dsd separately
			// (adding triples).
			body += "<" + dataset1 + ">" + " qb:structure ?dsd1. \n";

			if (dataset2 != null) {
				body += obsvariable2.toN3() + " qb:dataSet <" + dataset2
						+ ">. \n";

				// DSD.
				body += "<" + dataset2 + ">" + " qb:structure ?dsd2. \n";
			}

			// ** For each dimension add dataset graph pattern if of
			// specific dataset and not already contained in Data-Fu program
			first = true;
			for (Node[] dimension : dimensions) {
				if (first) {
					first = false;
					continue;
				}
				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					continue;
				}

				if (!isPatternContained(bodypatterns, null,
						dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
						null)) {
					body += obsvariable.toN3()
							+ " "
							+ dimension[dimensionmap
									.get("?DIMENSION_UNIQUE_NAME")].toN3()
							+ " "
							+ Olap4ldLinkedDataUtil.makeUriToVariable(
									dimension[dimensionmap
											.get("?DIMENSION_UNIQUE_NAME")]
											.toString()).toN3() + ". \n";
					if (dataset2 != null) {
						body += obsvariable2.toN3()
								+ " "
								+ dimension[dimensionmap
										.get("?DIMENSION_UNIQUE_NAME")].toN3()
								+ " "
								+ Olap4ldLinkedDataUtil.makeUriToVariable(
										dimension[dimensionmap
												.get("?DIMENSION_UNIQUE_NAME")]
												.toString()).toN3() + ". \n";
					}
				}
			}
			// ** For each measure add dataset graph pattern if not already
			// contained in Data-Fu program
			first = true;
			for (Node[] measure : measures) {
				if (first) {
					first = false;
					continue;
				}
				if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.contains("AGGFUNC")) {
					continue;
				}

				if (!isPatternContained(bodypatterns, null,
						measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
					body += obsvariable.toN3()
							+ " "
							+ measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
									.toN3()
							+ " "
							+ Olap4ldLinkedDataUtil.makeUriToVariable(
									measure[measuremap
											.get("?MEASURE_UNIQUE_NAME")]
											.toString()).toN3() + ". \n";
					if (dataset2 != null) {
						body += obsvariable2.toN3()
								+ " "
								+ measure[measuremap
										.get("?MEASURE_UNIQUE_NAME")].toN3()
								+ " "
								+ Olap4ldLinkedDataUtil.makeUriToVariable(
										measure[measuremap
												.get("?MEASURE_UNIQUE_NAME")]
												.toString()).toN3() + ". \n";
					}
				}
			}
			// ** } } }

			constructquery = prefixes + "construct { " + head
					+ " } where { { select " + select + " where {" + body
					+ " } } }";

			Olap4ldUtil._log.config("SPARQL query: " + constructquery);

			GraphQuery graphquery = con.prepareGraphQuery(
					org.openrdf.query.QueryLanguage.SPARQL, constructquery);

			StringWriter stringout = new StringWriter();
			RDFWriter w = Rio.createWriter(RDFFormat.RDFXML, stringout);
			graphquery.evaluate(w);

			this.triples = stringout.toString();
			
			if (Olap4ldUtil._isDebug) {

				Olap4ldUtil._log.config("Loaded triples: " + triples);

			}
			// Insert query to load triples
			// String insertquery =
			// "PREFIX olap4ld:<http://purl.org/olap4ld/> INSERT DATA { GRAPH <http://manually> { "
			// + triples + " } }";
			//
			// Olap4ldUtil._log.config("SPARQL query: " + insertquery);
			//
			// Update updateQuery = con.prepareUpdate(QueryLanguage.SPARQL,
			// insertquery);
			// updateQuery.execute();

			// Would not work: prolog error
			// ByteArrayInputStream inputstream = new
			// ByteArrayInputStream(w.toString().getBytes());

			// UTF-8 encoding seems important
			InputStream stream = new ByteArrayInputStream(
					triples.getBytes("UTF-8"));

			// Add to triple store
			con.add(stream, "", RDFFormat.RDFXML);

			// Loaded really?
			if (Olap4ldUtil._isDebug) {
				String filename = "dataset" + (dataset1 + dataset2).hashCode()
						+ "-conversionfunction" + conversionfunction.hashCode();

				Olap4ldLinkedDataUtil
						.dumpRDF(
								repo,
								"/media/84F01919F0191352/Projects/2014/paper/paper-macro-modelling/experiments/"
										+ filename + ".n3", RDFFormat.NTRIPLES);
			}

			con.close();

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
		} catch (RDFHandlerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (RDFParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private void prepareMetadata() {
		Restrictions restrictions = new Restrictions();

		this.dataset1 = null;
		this.dataset2 = null;
		// How to decide for the uri?
		this.newdataset = null;

		try {
			Restrictions restriction = new Restrictions();

			// First dataset(s) to convert
			cubemap = Olap4ldLinkedDataUtil.getNodeResultFields(inputiterator1
					.getCubes(restrictions).get(0));

			try {
				dataset1 = inputiterator1.getCubes(restrictions).get(1)[cubemap
						.get("?CUBE_NAME")].toString();
				if (inputiterator2 != null) {
					dataset2 = inputiterator2.getCubes(restrictions).get(1)[cubemap
							.get("?CUBE_NAME")].toString();
				}
			} catch (OlapException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}

			// We use hash of combination conversionfunction + datasets
			newdataset = domainUri + "dataset"
					+ (dataset1 + dataset2).hashCode() + "/conversionfunction"
					+ conversionfunction.hashCode();

			this.cubes = new ArrayList<Node[]>();
			boolean first = true;
			for (Node[] cube : inputiterator1.getCubes(restriction)) {
				if (first) {
					first = false;
					this.cubes.add(cube);
					continue;
				}
				// ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?CUBE_TYPE
				// ?CUBE_CAPTION ?DESCRIPTION

				Node[] newCube = new Node[6];
				newCube[cubemap.get("?CATALOG_NAME")] = cube[cubemap
						.get("?CATALOG_NAME")];
				newCube[cubemap.get("?SCHEMA_NAME")] = cube[cubemap
						.get("?SCHEMA_NAME")];
				newCube[cubemap.get("?CUBE_NAME")] = new Resource(newdataset);
				newCube[cubemap.get("?CUBE_TYPE")] = cube[cubemap
						.get("?CUBE_TYPE")];
				newCube[cubemap.get("?CUBE_CAPTION")] = cube[cubemap
						.get("?CUBE_CAPTION")];
				newCube[cubemap.get("?DESCRIPTION")] = cube[cubemap
						.get("?DESCRIPTION")];

				this.cubes.add(newCube);

			}
			this.measures = new ArrayList<Node[]>();
			measuremap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getMeasures(
							restrictions).get(0));
			first = true;
			for (Node[] node : inputiterator1.getMeasures(restriction)) {
				if (first) {
					first = false;
					this.measures.add(node);
					continue;
				}

				// Schema: (From XMLA/Linked Data Engine)
				// Measures: ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME
				// ?MEASURE_UNIQUE_NAME ?MEASURE_NAME ?MEASURE_CAPTION
				// ?DATA_TYPE ?MEASURE_IS_VISIBLE ?MEASURE_AGGREGATOR
				// ?EXPRESSION

				Node[] newnode = new Node[10];
				newnode[measuremap.get("?CATALOG_NAME")] = node[measuremap
						.get("?CATALOG_NAME")];
				newnode[measuremap.get("?SCHEMA_NAME")] = node[measuremap
						.get("?SCHEMA_NAME")];
				newnode[measuremap.get("?CUBE_NAME")] = new Resource(newdataset);
				newnode[measuremap.get("?MEASURE_UNIQUE_NAME")] = node[measuremap
						.get("?MEASURE_UNIQUE_NAME")];
				newnode[measuremap.get("?MEASURE_NAME")] = node[measuremap
						.get("?MEASURE_NAME")];
				newnode[measuremap.get("?MEASURE_CAPTION")] = node[measuremap
						.get("?MEASURE_CAPTION")];
				newnode[measuremap.get("?DATA_TYPE")] = node[measuremap
						.get("?DATA_TYPE")];
				newnode[measuremap.get("?MEASURE_IS_VISIBLE")] = node[measuremap
						.get("?MEASURE_IS_VISIBLE")];
				newnode[measuremap.get("?MEASURE_AGGREGATOR")] = node[measuremap
						.get("?MEASURE_AGGREGATOR")];
				newnode[measuremap.get("?EXPRESSION")] = node[measuremap
						.get("?EXPRESSION")];

				this.measures.add(newnode);
			}
			this.dimensions = new ArrayList<Node[]>();
			dimensionmap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getDimensions(
							restrictions).get(0));
			first = true;
			for (Node[] node : inputiterator1.getDimensions(restriction)) {
				if (first) {
					first = false;
					this.dimensions.add(node);
					continue;
				}
				// Schema: (From XMLA/Linked Data Engine)
				// Dimensions: ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME
				// ?DIMENSION_NAME ?DIMENSION_UNIQUE_NAME ?DIMENSION_CAPTION
				// ?DIMENSION_ORDINAL ?DIMENSION_TYPE ?DESCRIPTION

				Node[] newnode = new Node[9];
				newnode[dimensionmap.get("?CATALOG_NAME")] = node[dimensionmap
						.get("?CATALOG_NAME")];
				newnode[dimensionmap.get("?SCHEMA_NAME")] = node[dimensionmap
						.get("?SCHEMA_NAME")];
				newnode[dimensionmap.get("?CUBE_NAME")] = new Resource(
						newdataset);
				newnode[dimensionmap.get("?DIMENSION_NAME")] = node[dimensionmap
						.get("?DIMENSION_NAME")];
				newnode[dimensionmap.get("?DIMENSION_UNIQUE_NAME")] = node[dimensionmap
						.get("?DIMENSION_UNIQUE_NAME")];
				newnode[dimensionmap.get("?DIMENSION_CAPTION")] = node[dimensionmap
						.get("?DIMENSION_CAPTION")];
				newnode[dimensionmap.get("?DIMENSION_ORDINAL")] = node[dimensionmap
						.get("?DIMENSION_ORDINAL")];
				newnode[dimensionmap.get("?DIMENSION_TYPE")] = node[dimensionmap
						.get("?DIMENSION_TYPE")];
				newnode[dimensionmap.get("?DESCRIPTION")] = node[dimensionmap
						.get("?DESCRIPTION")];

				this.dimensions.add(newnode);
			}
			hierarchymap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getHierarchies(
							restrictions).get(0));
			this.hierarchies = new ArrayList<Node[]>();
			first = true;
			for (Node[] node : inputiterator1.getHierarchies(restriction)) {
				if (first) {
					first = false;
					this.hierarchies.add(node);
					continue;
				}
				Node[] newnode = new Node[9];
				// Schema: (From XMLA/Linked Data Engine)
				// Hierarchies: ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME
				// ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME ?HIERARCHY_NAME
				// ?HIERARCHY_CAPTION ?DESCRIPTION ?HIERARCHY_MAX_LEVEL_NUMBER

				newnode[hierarchymap.get("?CATALOG_NAME")] = node[hierarchymap
						.get("?CATALOG_NAME")];
				newnode[hierarchymap.get("?SCHEMA_NAME")] = node[hierarchymap
						.get("?SCHEMA_NAME")];
				newnode[hierarchymap.get("?CUBE_NAME")] = new Resource(
						newdataset);
				newnode[hierarchymap.get("?DIMENSION_UNIQUE_NAME")] = node[hierarchymap
						.get("?DIMENSION_UNIQUE_NAME")];
				newnode[hierarchymap.get("?HIERARCHY_UNIQUE_NAME")] = node[hierarchymap
						.get("?HIERARCHY_UNIQUE_NAME")];
				newnode[hierarchymap.get("?HIERARCHY_NAME")] = node[hierarchymap
						.get("?HIERARCHY_NAME")];
				newnode[hierarchymap.get("?HIERARCHY_CAPTION")] = node[hierarchymap
						.get("?HIERARCHY_CAPTION")];
				newnode[hierarchymap.get("?DESCRIPTION")] = node[hierarchymap
						.get("?DESCRIPTION")];
				newnode[hierarchymap.get("?HIERARCHY_MAX_LEVEL_NUMBER")] = node[hierarchymap
						.get("?HIERARCHY_MAX_LEVEL_NUMBER")];

				this.hierarchies.add(newnode);
			}
			levelmap = Olap4ldLinkedDataUtil.getNodeResultFields(inputiterator1
					.getLevels(restrictions).get(0));

			this.levels = new ArrayList<Node[]>();
			first = true;
			for (Node[] node : inputiterator1.getLevels(restriction)) {
				if (first) {
					first = false;
					this.levels.add(node);
					continue;
				}
				// Schema: (From XMLA/Linked Data Engine)
				// Levels: ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME
				// ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME
				// ?LEVEL_UNIQUE_NAME ?LEVEL_CAPTION ?LEVEL_NAME ?DESCRIPTION
				// ?LEVEL_NUMBER ?LEVEL_CARDINALITY ?LEVEL_TYPE

				Node[] newnode = new Node[12];
				newnode[levelmap.get("?CATALOG_NAME")] = node[levelmap
						.get("?CATALOG_NAME")];
				newnode[levelmap.get("?SCHEMA_NAME")] = node[levelmap
						.get("?SCHEMA_NAME")];
				newnode[levelmap.get("?CUBE_NAME")] = new Resource(newdataset);
				newnode[levelmap.get("?DIMENSION_UNIQUE_NAME")] = node[levelmap
						.get("?DIMENSION_UNIQUE_NAME")];
				newnode[levelmap.get("?HIERARCHY_UNIQUE_NAME")] = node[levelmap
						.get("?HIERARCHY_UNIQUE_NAME")];
				newnode[levelmap.get("?LEVEL_UNIQUE_NAME")] = node[levelmap
						.get("?LEVEL_UNIQUE_NAME")];
				newnode[levelmap.get("?LEVEL_CAPTION")] = node[levelmap
						.get("?LEVEL_CAPTION")];
				newnode[levelmap.get("?LEVEL_NAME")] = node[levelmap
						.get("?LEVEL_NAME")];
				newnode[levelmap.get("?DESCRIPTION")] = node[levelmap
						.get("?DESCRIPTION")];
				newnode[levelmap.get("?LEVEL_NUMBER")] = node[levelmap
						.get("?LEVEL_NUMBER")];
				newnode[levelmap.get("?LEVEL_CARDINALITY")] = node[levelmap
						.get("?LEVEL_CARDINALITY")];
				newnode[levelmap.get("?LEVEL_TYPE")] = node[levelmap
						.get("?LEVEL_TYPE")];
				this.levels.add(newnode);
			}
			membermap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1
							.getMembers(restrictions).get(0));
			this.members = new ArrayList<Node[]>();
			first = true;
			for (Node[] node : inputiterator1.getMembers(restriction)) {
				if (first) {
					first = false;
					this.members.add(node);
					continue;
				}
				// Schema: (From XMLA/Linked Data Engine)
				// Members: ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME
				// ?DIMENSION_UNIQUE_NAME ?HIERARCHY_UNIQUE_NAME
				// ?LEVEL_UNIQUE_NAME ?LEVEL_NUMBER ?MEMBER_UNIQUE_NAME
				// ?MEMBER_NAME ?MEMBER_CAPTION ?MEMBER_TYPE ?PARENT_UNIQUE_NAME
				// ?PARENT_LEVEL

				Node[] newnode = new Node[13];
				newnode[membermap.get("?CATALOG_NAME")] = node[membermap
						.get("?CATALOG_NAME")];
				newnode[membermap.get("?SCHEMA_NAME")] = node[membermap
						.get("?SCHEMA_NAME")];
				newnode[membermap.get("?CUBE_NAME")] = new Resource(newdataset);
				newnode[membermap.get("?DIMENSION_UNIQUE_NAME")] = node[membermap
						.get("?DIMENSION_UNIQUE_NAME")];
				newnode[membermap.get("?HIERARCHY_UNIQUE_NAME")] = node[membermap
						.get("?HIERARCHY_UNIQUE_NAME")];
				newnode[membermap.get("?LEVEL_UNIQUE_NAME")] = node[membermap
						.get("?LEVEL_UNIQUE_NAME")];
				newnode[membermap.get("?LEVEL_NUMBER")] = node[membermap
						.get("?LEVEL_NUMBER")];
				newnode[membermap.get("?MEMBER_UNIQUE_NAME")] = node[membermap
						.get("?MEMBER_UNIQUE_NAME")];
				newnode[membermap.get("?MEMBER_NAME")] = node[membermap
						.get("?MEMBER_NAME")];
				newnode[membermap.get("?MEMBER_CAPTION")] = node[membermap
						.get("?MEMBER_CAPTION")];
				newnode[membermap.get("?MEMBER_TYPE")] = node[membermap
						.get("?MEMBER_TYPE")];
				newnode[membermap.get("?PARENT_UNIQUE_NAME")] = node[membermap
						.get("?PARENT_UNIQUE_NAME")];
				newnode[membermap.get("?PARENT_LEVEL")] = node[membermap
						.get("?PARENT_LEVEL")];

				this.members.add(newnode);

			}
		} catch (OlapException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}

	private void executeSelectQueryForOutput(List<Node[]> bodypatterns,
			List<Node[]> headpatterns) {

		List<Node[]> myBindings = new ArrayList<Node[]>();
		try {

			RepositoryConnection con = this.repo.getConnection();
			// Select query for the output

			Node newobsvariable = bodypatterns.get(0)[0];

			/*
			 * Select: 1) Add variables for each dimension and measure. Where:
			 * 1) Add graph pattern for specific new dataset. 2) Add graph
			 * patterns for each dimension and measure
			 */

			String prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
					+ "PREFIX olap4ld:<http://purl.org/olap4ld/>";

			String select = "";

			boolean first = true;
			for (Node[] dimension : dimensions) {
				if (first) {
					first = false;
					continue;
				}
				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					continue;
				}

				// Create variable and add
				Node dimensionvariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(dimension[dimensionmap
								.get("?DIMENSION_UNIQUE_NAME")].toString());
				// What to do with it? Add to a map
				select += " " + dimensionvariable.toN3() + " ";
			}
			// ** For each measure add dataset graph pattern if not already
			// contained in Data-Fu program
			first = true;
			for (Node[] measure : measures) {
				if (first) {
					first = false;
					continue;
				}
				if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.contains("AGGFUNC")) {
					continue;
				}

				// Create variable and add
				Node measurevariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(measure[measuremap
								.get("?MEASURE_UNIQUE_NAME")].toString());
				// What to do with it? Add to a map
				select += " " + measurevariable.toN3() + " ";
			}

			// String where = "?obs1 qb:dataSet <"
			// + newdataset
			// + ">. "
			// +
			// "?obs1 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg_eur. "
			// +
			// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
			// +
			// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#unit> ?unit1. "
			// +
			// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
			// + "?obs1 <http://purl.org/dc/terms/date> ?time. \n";
			String where = "";

			// * 1) Add graph pattern for specific new dataset.

			where += newobsvariable.toN3() + " qb:dataSet <" + newdataset
					+ ">. \n";

			// * 2) Add graph patterns for each dimension and measure

			first = true;
			for (Node[] dimension : dimensions) {
				if (first) {
					first = false;
					continue;
				}
				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					continue;
				}

				// Create variable and add
				Node dimensionvariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(dimension[dimensionmap
								.get("?DIMENSION_UNIQUE_NAME")].toString());
				// What to do with it? Add to a map
				Resource dimensionresource = new Resource(
						dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
								.toString());
				where += newobsvariable.toN3() + " " + dimensionresource.toN3()
						+ " " + dimensionvariable.toN3() + ". \n";
			}
			// ** For each measure add dataset graph pattern if not already
			// contained in Data-Fu program
			first = true;
			for (Node[] measure : measures) {
				if (first) {
					first = false;
					continue;
				}
				if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.contains("AGGFUNC")) {
					continue;
				}

				// Create variable and add
				Node measurevariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(measure[measuremap
								.get("?MEASURE_UNIQUE_NAME")].toString());
				// What to do with it? Add to a map
				Resource measureresource = new Resource(
						measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
								.toString());
				where += newobsvariable.toN3() + " " + measureresource.toN3()
						+ " " + measurevariable.toN3() + ". \n";

			}

			String observationquery = prefixes + " select " + select
					+ " where {" + where + " }";

			Olap4ldUtil._log.config("SPARQL query: " + observationquery);
			ByteArrayOutputStream boas = new ByteArrayOutputStream();
			// FileOutputStream fos = new
			// FileOutputStream("/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/resources/result.srx");

			SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(
					boas);

			TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL,
					observationquery);
			tupleQuery.evaluate(sparqlWriter);

			ByteArrayInputStream bais = new ByteArrayInputStream(
					boas.toByteArray());

			// String xmlwriterstreamString =
			// Olap4ldLinkedDataUtil.convertStreamToString(bais);
			// System.out.println(xmlwriterstreamString);
			// Transform sparql xml to nx
			InputStream nx = Olap4ldLinkedDataUtil.transformSparqlXmlToNx(bais);

			// Only log if needed
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

		this.iterator = myBindings.iterator();
	}

	/**
	 * 
	 * @param bodypatterns
	 * @param object
	 * @param string
	 * @param object2
	 * @return
	 */
	private boolean isPatternContained(List<Node[]> patterns, Node subject,
			Node predicate, Node object) {

		for (Node[] bodypattern : patterns) {
			boolean subjectcontained = true;
			if (subject != null && !subject.equals(bodypattern[0])) {
				subjectcontained = false;
			}
			boolean predicatecontained = true;
			if (predicate != null && !predicate.equals(bodypattern[1])) {
				predicatecontained = false;
			}
			boolean objectcontained = true;
			if (object != null && !object.equals(bodypattern[2])) {
				objectcontained = false;
			}
			if (subjectcontained && predicatecontained && objectcontained) {
				return true;
			}
		}
		return false;
	}

	public String dumpRDF() {
		return this.triples;
	}

	/**
	 * Always true.
	 */
	public boolean hasNext() {
		return iterator.hasNext();
	}

	public Object next() {
		// No metadata is propagated, let us see whether our operators work
		// still
		// List<List<Node[]>> metadata = (ArrayList<List<Node[]>>) inputiterator
		// .next();
		//
		// // Remove from dimensions all those that are sliced
		// Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
		// .getNodeResultFields(slicedDimensions.get(0));
		// List<Node[]> dimensions = new ArrayList<Node[]>();
		// List<Node[]> inputdimensions = metadata.get(2);
		// for (Node[] inputdimension : inputdimensions) {
		// boolean add = true;
		// for (Node[] sliceddimension : slicedDimensions) {
		//
		// if (inputdimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
		// .equals(sliceddimension[dimensionmap
		// .get("?DIMENSION_UNIQUE_NAME")])) {
		// add = false;
		// }
		// }
		// if (add) {
		// dimensions.add(inputdimension);
		// }
		// }
		// metadata.set(2, dimensions);

		return iterator.next();
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Node[]> getCubes(Restrictions restrictions)
			throws OlapException {
		return cubes;
	}

	@Override
	public List<Node[]> getDimensions(Restrictions restrictions)
			throws OlapException {
		return dimensions;
	}

	@Override
	public List<Node[]> getMeasures(Restrictions restrictions)
			throws OlapException {
		return measures;
	}

	@Override
	public List<Node[]> getHierarchies(Restrictions restrictions)
			throws OlapException {
		return hierarchies;
	}

	@Override
	public List<Node[]> getLevels(Restrictions restrictions)
			throws OlapException {
		return levels;
	}

	@Override
	public List<Node[]> getMembers(Restrictions restrictions)
			throws OlapException {
		return members;
	}

}
