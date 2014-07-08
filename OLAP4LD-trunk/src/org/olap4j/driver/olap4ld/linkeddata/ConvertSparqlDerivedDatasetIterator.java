package org.olap4j.driver.olap4ld.linkeddata;

import java.io.ByteArrayOutputStream;
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
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

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

	private Iterator<Node[]> outputiterator;
	private PhysicalOlapIterator inputiterator1;
	private PhysicalOlapIterator inputiterator2;
	// private String conversionfunction;
	private ReconciliationCorrespondence conversioncorrespondence;

	private EmbeddedSesameEngine engine;
	private String triples;
	private String domainUri;
	private String newdataset;
	private String dataset1;
	private String dataset2;
	private DataFuProgram dataFuProgram;
	private List<Node[]> results;

	public ConvertSparqlDerivedDatasetIterator(EmbeddedSesameEngine engine,
			PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2,
			ReconciliationCorrespondence conversioncorrespondence,
			String domainUri) {

		this.engine = engine;
		this.inputiterator1 = inputiterator1;
		this.inputiterator2 = inputiterator2;
		this.domainUri = domainUri;

		this.conversioncorrespondence = conversioncorrespondence;

		// Basics

		try {

			Restrictions restriction = new Restrictions();
			cubemap = Olap4ldLinkedDataUtil.getNodeResultFields(inputiterator1
					.getCubes(restriction).get(0));
			// Assume cube to be index 1
			dataset1 = inputiterator1.getCubes(restriction).get(1)[cubemap
					.get("?CUBE_NAME")].toString();
			if (inputiterator2 != null) {
				dataset2 = inputiterator2.getCubes(restriction).get(1)[cubemap
						.get("?CUBE_NAME")].toString();
			}
		} catch (OlapException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		// We use hash of combination conversionfunction + datasets
		newdataset = this.domainUri + "dataset" + (dataset1 + dataset2).hashCode()
				+ "/conversionfunction"
				+ conversioncorrespondence.getname().hashCode();

		// Metadata

		prepareMetadata();

		// Data

		prepareData();

	}

	private void prepareData() {

		this.dataFuProgram = createDataFuProgram();

	}

	/**
	 * 
	 * @param conversioncorrespondence2
	 * @return Array with first element the body, second the head.
	 */
	private DataFuProgram createDataFuProgram() {
		// We assume one or two cubes, only.

		List<Node[]> bodypatterns = createBody();

		List<Node[]> headpatterns = createHead();

		DataFuProgram dataFuProgramObject = new DataFuProgram(bodypatterns,
				headpatterns);

		return dataFuProgramObject;
	}

	private List<Node[]> createBody() {
		/*
		 * The bodypatterns should contain: All graph patterns for c1. All
		 * bindas patterns. Dataset and structure graph patterns. All missing
		 * dimensions/measures graph patterns.
		 */
		List<Node[]> bodypatterns = new ArrayList<Node[]>();

		// Dataset Triples

		// Heuristically, the variable we get from the first pattern
		// subject.
		Node obsvariable = new Variable("inputcube1");

		Node obsvariable2 = null;
		if (dataset2 != null) {
			// The second variable we get from the next variable.
			obsvariable2 = new Variable("inputcube2");
		}

		bodypatterns.add(new Node[] { obsvariable,
				new Resource("http://purl.org/linked-data/cube#dataSet"),
				new Resource(dataset1) });

		// DSD. Use ?dsd1 for 1, ?dsd2 for 2.
		// Dsd seems not to be given to new datasets. Therefore, the
		// construct query does not work. Instead, we add the dsd separately
		// (adding triples).
		bodypatterns.add(new Node[] { new Resource(dataset1),
				new Resource("http://purl.org/linked-data/cube#structure"),
				new Variable("dsd1") });

		if (dataset2 != null) {
			bodypatterns.add(new Node[] { obsvariable2,
					new Resource("http://purl.org/linked-data/cube#dataSet"),
					new Resource(dataset2) });

			// DSD.
			bodypatterns.add(new Node[] { new Resource(dataset2),
					new Resource("http://purl.org/linked-data/cube#structure"),
					new Variable("dsd2") });
		}

		// Inputmembers Triples

		List<Node[]> inputmembers1 = conversioncorrespondence
				.getInputmembers1();

		for (Node[] nodes : inputmembers1) {
			bodypatterns.add(new Node[] { obsvariable, nodes[0], nodes[1] });
		}

		if (dataset2 != null) {
			List<Node[]> inputmembers2 = conversioncorrespondence
					.getInputmembers2();

			for (Node[] nodes : inputmembers2) {
				bodypatterns
						.add(new Node[] { obsvariable2, nodes[0], nodes[1] });
			}
		}

		// Dimension Triples:

		// All missing dimensions graph patterns for c1

		// For each dimension add dataset graph pattern if of
		// specific dataset and not already contained in atoms.
		/*
		 * for now, we assume that every dimension used in the second cube is
		 * fixed since otherwise the metadata of the result cube would not be
		 * correct
		 */
		Boolean first = true;
		for (Node[] dimension : dimensions) {
			if (first) {
				first = false;
				continue;
			}
			// Since we do not know how measure is encoded, compare strings.
			if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
					.toString().equals(
							Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
				continue;
			}

			if (!isPatternContained(bodypatterns, obsvariable,
					dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")], null)) {
				bodypatterns.add(new Node[] {
						obsvariable,
						dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
						Olap4ldLinkedDataUtil
								.makeUriToVariable(dimension[dimensionmap
										.get("?DIMENSION_UNIQUE_NAME")]) });

				if (dataset2 != null) {
					bodypatterns.add(new Node[] {
							obsvariable2,
							dimension[dimensionmap
									.get("?DIMENSION_UNIQUE_NAME")],
							Olap4ldLinkedDataUtil
									.makeUriToVariable(dimension[dimensionmap
											.get("?DIMENSION_UNIQUE_NAME")]) });
				}
			}
		}

		// Measure Triples:

		// First is header
		for (int i = 1; i < measures.size(); i++) {
			Node[] measure = measures.get(i);

			/*
			 * Here we deal with the problem that cubes coming in will possibly
			 * have many "implicit" measures. For now, we assume those to be
			 * disregarded.
			 */

			if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
					.contains("AGGFUNC")) {
				continue;
			}

			if (!isPatternContained(bodypatterns, obsvariable,
					measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {

				if (dataset2 == null) {
					bodypatterns.add(new Node[] { obsvariable,
							measure[measuremap.get("?MEASURE_UNIQUE_NAME")],
							new Variable("inputvalue" + i) });

				} else {

					/*
					 * Weakness: We assume the second cube to have the same
					 * measures as the first cube.
					 */

					bodypatterns.add(new Node[] { obsvariable,
							measure[measuremap.get("?MEASURE_UNIQUE_NAME")],
							new Variable("input1value" + i) });

					bodypatterns.add(new Node[] { obsvariable2,
							measure[measuremap.get("?MEASURE_UNIQUE_NAME")],
							new Variable("input2value" + i) });
				}
			}
		}

		// Function Triples:

		// First is header
		for (int i = 1; i < measures.size(); i++) {
			Node[] measure = measures.get(i);

			// No extended measures. This means, currently, we are not
			// considering aggregations.
			if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
					.contains("AGGFUNC")) {
				continue;
			}

			// Replace variable in function
			String function = conversioncorrespondence.getFunction();
			if (dataset2 == null) {
				function = function.replace("x", "?inputvalue" + i);
			} else {
				function = function.replace("x1", "?input1value" + i).replace(
						"x2", "?input2value" + i);
			}

			bodypatterns
					.add(new Node[] {
							new Variable("outputvalue" + i),
							new Resource(
									"http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas"),
							new Literal(function) });
		}

		return bodypatterns;
	}

	private List<Node[]> createHead() {
		List<Node[]> headpatterns = new ArrayList<Node[]>();

		// Heuristically, the variable we create ourselves
		Node outputblanknodevariable = new BNode("_:outputcube");

		// Dataset Triples:

		// Dataset and structure graph patterns.

		headpatterns.add(new Node[] { outputblanknodevariable,
				new Resource("http://purl.org/linked-data/cube#dataSet"),
				new Resource(newdataset) });

		// DSD. For now, assume same dsd as other dataset.
		// Dsd seems not to be given to new datasets. Therefore, the
		// construct query does not work. Instead, we add the dsd separately
		// (adding triples).
		// Solution: Has to be propagated in select (!)
		headpatterns.add(new Node[] { new Resource(newdataset),
				new Resource("http://purl.org/linked-data/cube#structure"),
				new Variable("dsd1") });

		// Outputmembers Triples:

		List<Node[]> outputmembers = conversioncorrespondence
				.getOutputmembers();

		for (Node[] nodes : outputmembers) {
			headpatterns.add(new Node[] { outputblanknodevariable, nodes[0],
					nodes[1] });
		}

		// Dimension Triples:

		// All missing dimensions/measures graph patterns.
		// XXX: A cleaner way would be to specifically refer to the metadata of
		// the first input iterator
		Restrictions restrictions = new Restrictions();

		// We assume dimensions and measures that we go through coming from
		// first cube (otherwise metadata may not make sense anyway, sometimes).
		List<Node[]> dimensions = null;

		try {
			dimensions = this.inputiterator1.getDimensions(restrictions);
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		boolean first = true;
		for (Node[] dimension : dimensions) {
			if (first) {
				first = false;
				continue;
			}

			// disregard measure dimension
			if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
					.toString().equals(
							Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
				continue;
			}

			// This would mean, every dim in inputmembers also has to be in
			// outputmembers
			if (!isPatternContained(headpatterns, outputblanknodevariable,
					dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")], null)) {
				headpatterns.add(new Node[] {
						outputblanknodevariable,
						dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
						Olap4ldLinkedDataUtil
								.makeUriToVariable(dimension[dimensionmap
										.get("?DIMENSION_UNIQUE_NAME")]) });

			}
		}

		// Measure Triples:

		List<Node[]> measures = null;

		try {
			measures = this.inputiterator1.getMeasures(restrictions);
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// ** For each measure add dataset graph pattern if not already
		// contained in Data-Fu program and not implicit measure
		// First is header
		for (int i = 1; i < measures.size(); i++) {
			Node[] measure = measures.get(i);

			// No extended measures. This means, currently, we are not
			// considering aggregations.
			if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
					.contains("AGGFUNC")) {
				continue;
			}

			if (!isPatternContained(headpatterns, outputblanknodevariable,
					measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
				headpatterns.add(new Node[] { outputblanknodevariable,
						measure[measuremap.get("?MEASURE_UNIQUE_NAME")],
						new Variable("outputvalue" + i) });
			}
		}

		return headpatterns;
	}

	private void executeSPARQLSelectQuery() {

		// Select query for the output

		Node newobsvariable = new Variable("c1");

		/*
		 * Select: 1) Add variables for each dimension and measure. Where: 1)
		 * Add graph pattern for specific new dataset.
		 * 
		 * 2) Add graph patterns for each dimension and measure
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
							.get("?DIMENSION_UNIQUE_NAME")]);
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
							.get("?MEASURE_UNIQUE_NAME")]);
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

		where += newobsvariable.toN3() + " qb:dataSet <" + newdataset + ">. \n";

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
							.get("?DIMENSION_UNIQUE_NAME")]);
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
							.get("?MEASURE_UNIQUE_NAME")]);
			// What to do with it? Add to a map
			Resource measureresource = new Resource(
					measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString());
			where += newobsvariable.toN3() + " " + measureresource.toN3() + " "
					+ measurevariable.toN3() + ". \n";

		}

		String observationquery = prefixes + " select " + select + " where {"
				+ where + " }";

		Olap4ldUtil._log.config("SPARQL SELECT query: " + observationquery);

		this.results = this.engine.sparql(observationquery, false);
	}

	private void executeSPARQLConstructQuery() {

		List<Node[]> bodypatterns = dataFuProgram.bodypatterns;
		List<Node[]> headpatterns = dataFuProgram.headpatterns;

		// Construct query to create triples
		String constructquery = "";

		// ** PREFIXES
		String prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
				+ "PREFIX olap4ld:<http://purl.org/olap4ld/>";

		// ** where { {
		// ** select
		// ** Select:
		// String select =
		// "?geo ?unit1 ?indic_na1 ?time ((?boneg * 1000000) as ?boneg_eur) ";
		// ** Add select variables for every dimension and measure which is
		// not
		// mentioned as predicate in body.
		String select = "";

		// body head
		String body = "";

		for (Node[] bodypattern : bodypatterns) {

			// Only if not bindas.
			if (!bodypattern[1]
					.toN3()
					.equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")) {
				body += bodypattern[0].toN3() + " " + bodypattern[1].toN3()
						+ " " + bodypattern[2].toN3() + " . \n";
			}
		}

		String head = "";

		for (Node[] nodes : headpatterns) {
			head += nodes[0].toN3() + " " + nodes[1].toN3() + " "
					+ nodes[2].toN3() + " . \n";
		}

		// Add dsd
		select += " ?dsd1 ?dsd2 ";

		// Build hashmap from dimension or measure node and variable node.
		HashMap<Node, Node> selectbodymap = new HashMap<Node, Node>();

		// Do I not simply need to go through bodypatterns and add all
		// variables?

		for (Node[] nodes : bodypatterns) {
			if (nodes[2] instanceof Variable) {
				selectbodymap.put(nodes[1], nodes[2]);
			}
		}

		// add bindas variables: Look for qrl:bindas; use subject as
		// variable to select; create select
		// http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas
		for (Node[] bodypattern : bodypatterns) {
			if (bodypattern[1]
					.toN3()
					.equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")) {
				// Add directly to select without quotes (toString())
				select += " (" + bodypattern[2].toString() + " as "
						+ bodypattern[0].toN3() + ") ";
			}
		}

		// Create full select
		Set<Entry<Node, Node>> selectbodyset = selectbodymap.entrySet();
		for (Entry<Node, Node> entry : selectbodyset) {
			select += " " + entry.getValue().toN3() + " ";
		}

		constructquery = prefixes + "construct { " + head
				+ " } where { { select " + select + " where {" + body
				+ " } } }";

		Olap4ldUtil._log.config("SPARQL CONSTRUCT query: " + constructquery);

		this.engine.executeCONSTRUCTQuery(constructquery);
	}

	// Old
	// private void prepareData(List<Node[]> bodypatterns,
	// List<Node[]> headpatterns) {
	// try {
	//
	// // We assume one or two cubes, only.
	//
	// RepositoryConnection con = this.repo.getConnection();
	//
	// // Construct query to create triples
	// String constructquery = "";
	//
	// // ** PREFIXES
	// String prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
	// + "PREFIX olap4ld:<http://purl.org/olap4ld/>";
	// // ** construct {
	// // ** Head:
	// // String headold = "_:obs qb:dataSet <"
	// // + newdataset
	// // + ">. "
	// // +
	// //
	// "_:obs <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg_eur. "
	// // +
	// // "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
	// // +
	// //
	// "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#unit> <http://estatwrap.ontologycentral.com/dic/unit#EUR>. "
	// // +
	// //
	// "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
	// // + "_:obs <http://purl.org/dc/terms/date> ?time.";
	// String head = "";
	// // ** Add head from Data-Fu program
	// for (Node[] headpattern : headpatterns) {
	// String headgraphpatternstring = headpattern[0].toN3() + " "
	// + headpattern[1].toN3() + " " + headpattern[2].toN3()
	// + ". \n";
	// head += headgraphpatternstring;
	// }
	//
	// // ** For output cube, add dataset graph patterns (_:obs qb:dataSet
	// // <newdataset>.)
	//
	// // We assume that always only one new dataset is created
	//
	// // Heuristically, the variables for the cubes, we get from the
	// // pattern
	// // subject. The first from the first, the second by going through
	// // (apart from qrl:bindas)
	// Node obsvariable = headpatterns.get(0)[0];
	//
	// head += obsvariable.toN3() + " qb:dataSet <" + newdataset + ">. \n";
	//
	// // DSD. For now, assume same dsd as other dataset.
	// // Dsd seems not to be given to new datasets. Therefore, the
	// // construct query does not work. Instead, we add the dsd separately
	// // (adding triples).
	// // Solution: Has to be propagated in select (!)
	// head += "<" + newdataset + ">" + " qb:structure ?dsd1. \n";
	//
	// // ** For each dimension add dataset graph pattern if not already
	// // contained in Data-Fu program or Measure Dimension.
	//
	// // XXX: A cleaner way would be to specifically refer to the metadata
	// // of the first input iterator
	// Restrictions restrictions = new Restrictions();
	//
	// // We assume dimensions and measures that we go through coming from
	// // first cube (otherwise metadata may not make sense anyway,
	// // sometimes).
	// List<Node[]> dimensions = null;
	// List<Node[]> measures = null;
	// try {
	// dimensions = this.inputiterator1.getDimensions(restrictions);
	// measures = this.inputiterator1.getMeasures(restrictions);
	// } catch (OlapException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	//
	// boolean first = true;
	// for (Node[] dimension : dimensions) {
	// if (first) {
	// first = false;
	// continue;
	// }
	//
	// if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
	// .toString().equals(
	// Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
	// continue;
	// }
	//
	// /*
	// * Although we create the headpatterns, since it does not make
	// * sense to add graph patterns for dimensions to head for which
	// * there is not pendant in body, we go through bodypatterns.
	// */
	// if (!isPatternContained(bodypatterns, null,
	// dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
	// null)) {
	// head += obsvariable.toN3()
	// + " "
	// + dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")].toN3()
	// + " "
	// + Olap4ldLinkedDataUtil.makeUriToVariable(
	// dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")]
	// ).toN3() + ". \n";
	//
	// }
	// }
	// // ** For each measure add dataset graph pattern if not already
	// // contained in Data-Fu program and not implicit measure
	// first = true;
	// for (Node[] measure : measures) {
	// if (first) {
	// first = false;
	// continue;
	// }
	// // No extended measures. This means, currently, we are not
	// // considering aggregations.
	// if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
	// .contains("AGGFUNC")) {
	// continue;
	// }
	//
	// if (!isPatternContained(bodypatterns, null,
	// measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
	// head += obsvariable.toN3()
	// + " "
	// + measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
	// .toN3()
	// + " "
	// + Olap4ldLinkedDataUtil.makeUriToVariable(
	// measure[measuremap
	// .get("?MEASURE_UNIQUE_NAME")]
	// ).toN3() + ". \n";
	// }
	// }
	// // ** }
	// // ** where { {
	// // ** select
	// // ** Select:
	// // String select =
	// // "?geo ?unit1 ?indic_na1 ?time ((?boneg * 1000000) as ?boneg_eur) ";
	// // ** Add select variables for every dimension and measure which is
	// // not
	// // mentioned as predicate in body.
	// String select = "";
	//
	// // Add dsd
	// select += " ?dsd1 ?dsd2 ";
	//
	// // Build hashmap from dimension or measure node and variable node.
	// HashMap<Node, Node> selectbodymap = new HashMap<Node, Node>();
	//
	// first = true;
	// for (Node[] dimension : dimensions) {
	// if (first) {
	// first = false;
	// continue;
	// }
	// if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
	// .toString().equals(
	// Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
	// continue;
	// }
	//
	// if (!isPatternContained(bodypatterns, null,
	// dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
	// null)) {
	// // Create variable and add
	// Node dimensionvariable = Olap4ldLinkedDataUtil
	// .makeUriToVariable(dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")]);
	// // What to do with it? Add to a map
	// Resource dimensionresource = new Resource(
	// dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")].toString());
	// selectbodymap.put(dimensionresource, dimensionvariable);
	// }
	// }
	// // ** For each measure add dataset graph pattern if not already
	// // contained in Data-Fu program
	// first = true;
	// for (Node[] measure : measures) {
	// if (first) {
	// first = false;
	// continue;
	// }
	// if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
	// .contains("AGGFUNC")) {
	// continue;
	// }
	//
	// if (!isPatternContained(bodypatterns, null,
	// measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
	// // Create variable and add
	// Node measurevariable = Olap4ldLinkedDataUtil
	// .makeUriToVariable(measure[measuremap
	// .get("?MEASURE_UNIQUE_NAME")]);
	// // What to do with it? Add to a map
	// Resource measureresource = new Resource(
	// measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
	// .toString());
	// selectbodymap.put(measureresource, measurevariable);
	// }
	// }
	//
	// // ** Add all variables from objects in body which are not constants
	//
	// for (Node[] bodypattern : bodypatterns) {
	// // We add variable to map if no constant
	// if (bodypattern[2].toN3().startsWith("?")) {
	// selectbodymap.put(bodypattern[1], bodypattern[2]);
	// }
	// }
	//
	// // add bindas variables: Look for qrl:bindas; use subject as
	// // variable to select; create select
	// // http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas
	// for (Node[] bodypattern : bodypatterns) {
	// if (bodypattern[1]
	// .toN3()
	// .equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")) {
	// // Add directly to select
	// select += " (" + bodypattern[2].toString() + " as "
	// + bodypattern[0].toN3() + ") ";
	// }
	// }
	//
	// // Create full select
	// Set<Entry<Node, Node>> selectbodyset = selectbodymap.entrySet();
	// for (Entry<Node, Node> entry : selectbodyset) {
	// select += " " + entry.getValue().toN3() + " ";
	// }
	//
	// // ** where {
	// // ** Body:
	// // String bodyold = "?obs1 qb:dataSet <"
	// // + dataset
	// // + ">. "
	// // +
	// //
	// "?obs1 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg. "
	// // +
	// // "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
	// // +
	// // "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#unit> ?unit1. "
	// // +
	// //
	// "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
	// // + "?obs1 <http://purl.org/dc/terms/date> ?time. "
	// // +
	// //
	// "FILTER (?unit1 = <http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR>)";
	// String body = "";
	//
	// // ** Add body from Data-Fu program apart from "bindas"
	// for (Node[] bodypattern : bodypatterns) {
	// // Have to check on prefixed version, also?
	// if (bodypattern[1]
	// .toN3()
	// .equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")) {
	// continue;
	// }
	// String bodygraphpattern = bodypattern[0].toN3() + " "
	// + bodypattern[1].toN3() + " " + bodypattern[2].toN3()
	// + ". \n";
	// body += bodygraphpattern;
	// }
	//
	// // Heuristically, the variable we get from the first pattern
	// // subject.
	// obsvariable = bodypatterns.get(0)[0];
	//
	// // The second variable we get from the next variable.
	// Node obsvariable2 = null;
	//
	// if (dataset2 != null) {
	// // Then we should find one additional apart from those using
	// // specific properties.
	// for (Node[] bodypattern : bodypatterns) {
	// if (!bodypattern[1]
	// .toN3()
	// .equals("<http://www.aifb.kit.edu/project/ld-retriever/qrl#bindas>")
	// && !bodypattern[0].equals(bodypatterns.get(0)[0])) {
	// obsvariable2 = bodypattern[0];
	// }
	// }
	// }
	//
	// body += obsvariable.toN3() + " qb:dataSet <" + dataset1 + ">. \n";
	//
	// // DSD. Use ?dsd1 for 1, ?dsd2 for 2.
	// // Dsd seems not to be given to new datasets. Therefore, the
	// // construct query does not work. Instead, we add the dsd separately
	// // (adding triples).
	// body += "<" + dataset1 + ">" + " qb:structure ?dsd1. \n";
	//
	// if (dataset2 != null) {
	// body += obsvariable2.toN3() + " qb:dataSet <" + dataset2
	// + ">. \n";
	//
	// // DSD.
	// body += "<" + dataset2 + ">" + " qb:structure ?dsd2. \n";
	// }
	//
	// // ** For each dimension add dataset graph pattern if of
	// // specific dataset and not already contained in Data-Fu program
	// /*
	// * for now, we assume that every dimension used in the second cube
	// * is fixed since otherwise the metadata of the result cube would
	// * not be correct
	// */
	// first = true;
	// for (Node[] dimension : dimensions) {
	// if (first) {
	// first = false;
	// continue;
	// }
	// if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
	// .toString().equals(
	// Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
	// continue;
	// }
	//
	// if (!isPatternContained(bodypatterns, null,
	// dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")],
	// null)) {
	// body += obsvariable.toN3()
	// + " "
	// + dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")].toN3()
	// + " "
	// + Olap4ldLinkedDataUtil.makeUriToVariable(
	// dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")]
	// ).toN3() + ". \n";
	// if (dataset2 != null) {
	// body += obsvariable2.toN3()
	// + " "
	// + dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")].toN3()
	// + " "
	// + Olap4ldLinkedDataUtil.makeUriToVariable(
	// dimension[dimensionmap
	// .get("?DIMENSION_UNIQUE_NAME")]
	// ).toN3() + ". \n";
	// }
	// }
	// }
	// // ** For each measure add dataset graph pattern if not already
	// // contained in Data-Fu program
	// first = true;
	// for (Node[] measure : measures) {
	// if (first) {
	// first = false;
	// continue;
	// }
	// if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
	// .contains("AGGFUNC")) {
	// continue;
	// }
	//
	// if (!isPatternContained(bodypatterns, null,
	// measure[measuremap.get("?MEASURE_UNIQUE_NAME")], null)) {
	// body += obsvariable.toN3()
	// + " "
	// + measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
	// .toN3()
	// + " "
	// + Olap4ldLinkedDataUtil.makeUriToVariable(
	// measure[measuremap
	// .get("?MEASURE_UNIQUE_NAME")]
	// ).toN3() + ". \n";
	// if (dataset2 != null) {
	// body += obsvariable2.toN3()
	// + " "
	// + measure[measuremap
	// .get("?MEASURE_UNIQUE_NAME")].toN3()
	// + " "
	// + Olap4ldLinkedDataUtil.makeUriToVariable(
	// measure[measuremap
	// .get("?MEASURE_UNIQUE_NAME")]
	// ).toN3() + ". \n";
	// }
	// }
	// }
	// // ** } } }
	//
	// constructquery = prefixes + "construct { " + head
	// + " } where { { select " + select + " where {" + body
	// + " } } }";
	//
	// Olap4ldUtil._log.config("SPARQL query: " + constructquery);
	//
	// GraphQuery graphquery = con.prepareGraphQuery(
	// org.openrdf.query.QueryLanguage.SPARQL, constructquery);
	//
	// StringWriter stringout = new StringWriter();
	// RDFWriter w = Rio.createWriter(RDFFormat.RDFXML, stringout);
	// graphquery.evaluate(w);
	//
	// this.triples = stringout.toString();
	//
	// if (Olap4ldUtil._isDebug) {
	//
	// Olap4ldUtil._log.config("Loaded triples: " + triples);
	//
	// }
	// // Insert query to load triples
	// // String insertquery =
	// //
	// "PREFIX olap4ld:<http://purl.org/olap4ld/> INSERT DATA { GRAPH <http://manually> { "
	// // + triples + " } }";
	// //
	// // Olap4ldUtil._log.config("SPARQL query: " + insertquery);
	// //
	// // Update updateQuery = con.prepareUpdate(QueryLanguage.SPARQL,
	// // insertquery);
	// // updateQuery.execute();
	//
	// // Would not work: prolog error
	// // ByteArrayInputStream inputstream = new
	// // ByteArrayInputStream(w.toString().getBytes());
	//
	// // UTF-8 encoding seems important
	// InputStream stream = new ByteArrayInputStream(
	// triples.getBytes("UTF-8"));
	//
	// // Add to triple store
	// con.add(stream, "", RDFFormat.RDFXML);
	//
	// // Loaded really?
	// if (Olap4ldUtil._isDebug) {
	// String filename = "dataset" + (dataset1 + dataset2).hashCode()
	// + "-conversionfunction" + this.conversioncorrespondence;
	//
	// Olap4ldLinkedDataUtil
	// .dumpRDF(
	// repo,
	// "/media/84F01919F0191352/Projects/2014/paper/paper-macro-modelling/experiments/"
	// + filename + ".n3", RDFFormat.NTRIPLES);
	// }
	//
	// con.close();
	//
	// } catch (RepositoryException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (MalformedURLException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (MalformedQueryException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (QueryEvaluationException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (RDFHandlerException e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// } catch (RDFParseException e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	// }

	private void prepareMetadata() {

		try {
			Restrictions restriction = new Restrictions();

			// Cubes will be new cube
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

			// Measures will be all measures of first cube
			this.measures = new ArrayList<Node[]>();
			measuremap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1
							.getMeasures(restriction).get(0));
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

			// Dimensions will be all dimensions from first cube
			this.dimensions = new ArrayList<Node[]>();
			dimensionmap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getDimensions(
							restriction).get(0));
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

			// Hierarchies will be all hierarchies from first cube
			this.hierarchies = new ArrayList<Node[]>();
			hierarchymap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getHierarchies(
							restriction).get(0));
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

			// Levels will be all levels from first cube
			this.levels = new ArrayList<Node[]>();
			levelmap = Olap4ldLinkedDataUtil.getNodeResultFields(inputiterator1
					.getLevels(restriction).get(0));
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

			// Members will be all members of first cube
			this.members = new ArrayList<Node[]>();
			membermap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getMembers(restriction)
							.get(0));
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

		// Select query for the output

		Node newobsvariable = bodypatterns.get(0)[0];

		/*
		 * Select: 1) Add variables for each dimension and measure. Where: 1)
		 * Add graph pattern for specific new dataset. 2) Add graph patterns for
		 * each dimension and measure
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
							.get("?DIMENSION_UNIQUE_NAME")]);
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
							.get("?MEASURE_UNIQUE_NAME")]);
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

		where += newobsvariable.toN3() + " qb:dataSet <" + newdataset + ">. \n";

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
							.get("?DIMENSION_UNIQUE_NAME")]);
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
							.get("?MEASURE_UNIQUE_NAME")]);
			// What to do with it? Add to a map
			Resource measureresource = new Resource(
					measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString());
			where += newobsvariable.toN3() + " " + measureresource.toN3() + " "
					+ measurevariable.toN3() + ". \n";

		}

		String observationquery = prefixes + " select " + select + " where {"
				+ where + " }";

		Olap4ldUtil._log.config("SPARQL query: " + observationquery);
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		// FileOutputStream fos = new
		// FileOutputStream("/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/resources/result.srx");

		this.outputiterator = this.engine.sparql(observationquery, false)
				.iterator();
	}

	/**
	 * This basically is an ASK implementation for specific triple patterns.
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
		if (this.results == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return outputiterator.hasNext();
	}

	public Object next() {
		if (this.results == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return outputiterator.next();
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public void init() throws Exception {
		// From bodypatterns and headpatterns of data-fu program, create SPARQL
		// query.

		// Should not be run every time again.
		if (this.results == null) {

			// Need to make sure that I also init the input operators.
			inputiterator1.init();
			if (inputiterator2 != null) {
				inputiterator2.init();
			}

			Olap4ldUtil._log
					.info("Execute logical query plan: Create and load derived dataset.");
			long time = System.currentTimeMillis();

			executeSPARQLConstructQuery();

			time = System.currentTimeMillis() - time;

			Olap4ldUtil._log
					.info("Execute logical query plan: Create and load derived dataset finished in "
							+ time + "ms.");

			executeSPARQLSelectQuery();

		}
		this.outputiterator = this.results.iterator();
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	public String toString() {
		if (inputiterator2 == null) {
			return "Convert-Cube (" + inputiterator1.toString() + ", "
					+ dataFuProgram.toString() + ")";
		} else {
			return "Merge-Cubes (" + inputiterator1.toString() + ", "
					+ inputiterator2.toString() + ", "
					+ dataFuProgram.toString() + ")";
		}
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

		// Convert-Cube would only take the non-aggregated measures
		List<Node[]> newmeasures = new ArrayList<Node[]>();
		newmeasures.add(measures.get(0));
		for (int i = 1; i < measures.size(); i++) {
			Node[] measure = measures.get(i);

			/*
			 * Here we deal with the problem that cubes coming in will possibly
			 * have many "implicit" measures. For now, we assume those to be
			 * disregarded.
			 */

			if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
					.contains("AGGFUNC")) {
				continue;
			}

			newmeasures.add(measure);
		}

		return newmeasures;
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

	/**
	 * Represents data fu program with bodypatterns and headpatterns.
	 * 
	 * @author benedikt
	 * 
	 */
	class DataFuProgram {
		public List<Node[]> bodypatterns;
		public List<Node[]> headpatterns;

		public DataFuProgram(List<Node[]> bodypatterns,
				List<Node[]> headpatterns) {
			this.bodypatterns = bodypatterns;
			this.headpatterns = headpatterns;
		}

		public String toString() {

			// Return Data-Fu query
			String dataFuProgram = "Convert-Cube: { \n";
			for (Node[] nodes : bodypatterns) {
				dataFuProgram += nodes[0].toN3() + " " + nodes[1].toN3() + " "
						+ nodes[2].toN3() + " . \n";
			}
			dataFuProgram += " } => { \n";
			for (Node[] nodes : headpatterns) {
				dataFuProgram += nodes[0].toN3() + " " + nodes[1].toN3() + " "
						+ nodes[2].toN3() + " . \n";
			}
			dataFuProgram += "} ";

			Olap4ldUtil._log.config("Linked-Data-Fu-Program: " + dataFuProgram);

			return dataFuProgram;
		}
	}

}
