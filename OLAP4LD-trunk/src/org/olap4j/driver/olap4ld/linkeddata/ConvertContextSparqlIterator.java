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
public class ConvertContextSparqlIterator implements PhysicalOlapIterator {

	// The different metadata parts
	List<Node[]> cubes;
	List<Node[]> measures;
	List<Node[]> dimensions;
	List<Node[]> hierarchies;
	List<Node[]> levels;
	List<Node[]> members;

	private Iterator<Node[]> iterator;
	private String conversionfunction;

	private SailRepository repo;
	private String triples;

	public ConvertContextSparqlIterator(SailRepository repo,
			PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2, String conversionfunction,
			String domainuri) {

		this.repo = repo;

		// We assume that convert context does not change the metadata and thus
		// root1 returns the metadata.

		try {
			Restrictions restriction = new Restrictions();
			this.cubes = inputiterator1.getCubes(restriction);
			this.measures = inputiterator1.getMeasures(restriction);
			this.dimensions = inputiterator1.getDimensions(restriction);
			this.hierarchies = inputiterator1.getHierarchies(restriction);
			this.levels = inputiterator1.getLevels(restriction);
			this.members = inputiterator1.getMembers(restriction);
		} catch (OlapException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		this.conversionfunction = conversionfunction;

		List<Node[]> myBindings = new ArrayList<Node[]>();

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

		Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(cubes.get(0));
		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(dimensions.get(0));
		Map<String, Integer> measuremap = Olap4ldLinkedDataUtil
				.getNodeResultFields(measures.get(0));

		// First dataset to convert

		String datasetcombination = "";
		for (Node[] cube : cubes) {
			datasetcombination += cube[cubemap.get("?CUBE_NAME")].toString();
		}

		// How to decide for the uri?
		String newdataset = null;
		// We use hash of combination conversionfunction + datasets
		newdataset = domainuri + "dataset" + datasetcombination.hashCode()
				+ "/conversionfunction" + conversionfunction.hashCode();

		// We need two queries, one for the metadata and one for the data.

		// Metadata

		// Data

		try {

			// First try: We assume one cube, only.

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

			// ** For each input cube add dataset graph patterns.
			Node[] cube = cubes.get(1);

			// Heuristically, the variable we get from the first pattern
			// subject.
			// For now, we assume only one cube
			// XXX: and use index if there are several.
			String obsvariable = headpatterns.get(0)[0].toN3();

			head += obsvariable + " qb:dataSet <" + newdataset + ">. \n";

			// DSD. For now, assume same dsd as other dataset.
			head += "<" + newdataset + ">" + " qb:structure ?dsd. \n";

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
					head += obsvariable
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
					head += obsvariable
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

			// ** For each input cube add dataset graph patterns.
			cube = cubes.get(1);

			// Heuristically, the variable we get from the first pattern
			// subject.
			// For now, we assume only one cube
			// XXX: and use index if there are several.
			obsvariable = bodypatterns.get(0)[0].toN3();

			body += obsvariable + " qb:dataSet <"
					+ cube[cubemap.get("?CUBE_NAME")].toString() + ">. \n";

			// DSD.
			body += "<" + cube[cubemap.get("?CUBE_NAME")].toString() + ">"
					+ " qb:structure ?dsd. \n";

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
					body += obsvariable
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
					body += obsvariable
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

			Olap4ldUtil._log.config("Loaded triples: " + triples);
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

			// Select query for the output

			obsvariable = bodypatterns.get(0)[0].toN3();

			/*
			 * Select: 1) Add variables for each dimension and measure. Where:
			 * 1) Add graph pattern for specific new dataset. 2) Add graph
			 * patterns for each dimension and measure
			 */

			prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
					+ "PREFIX olap4ld:<http://purl.org/olap4ld/>";

			select = "";

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

			where += obsvariable + " qb:dataSet <" + newdataset + ">. \n";

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
				where += obsvariable + " " + dimensionresource.toN3() + " "
						+ dimensionvariable.toN3() + ". \n";
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
				where += obsvariable + " " + measureresource.toN3() + " "
						+ measurevariable.toN3() + ". \n";

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
		} catch (RDFHandlerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (RDFParseException e1) {
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
