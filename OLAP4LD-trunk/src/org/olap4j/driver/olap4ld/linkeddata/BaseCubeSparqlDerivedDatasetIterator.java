package org.olap4j.driver.olap4ld.linkeddata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * 
 * @author benedikt
 * 
 */
public class BaseCubeSparqlDerivedDatasetIterator implements
		PhysicalOlapIterator {

	public List<Node[]> cubes;
	public List<Node[]> measures;
	public List<Node[]> dimensions;
	public List<Node[]> hierarchies;
	public List<Node[]> levels;
	public List<Node[]> members;

	private SailRepository repo;
	private Iterator<Node[]> outputiterator;
	private String query;

	public BaseCubeSparqlDerivedDatasetIterator(SailRepository repo,
			List<Node[]> cubes, List<Node[]> measures, List<Node[]> dimensions,
			List<Node[]> hierarchies, List<Node[]> levels, List<Node[]> members) {
		// We assume that basecube has a repo with populated according to
		// metadata
		this.repo = repo;
		// We assume that cubes to BaseCubeOp always refer to one single cube
		assert cubes.size() <= 2;

		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		this.members = members;

		// Instead of simply returning the metadata, we can return the
		// List of List of nodes representing the observations.
		// We create a SPARQL query asking for all observations
		// with all dimensions and measures.

		// We collect necessary parts of the SPARQL query.
		String selectClause = " ";
		String whereClause = " ";

		// 1. Get cube add triple patterns
		Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(cubes.get(0));

		// Cube gives us the URI of the dataset that we want to resolve
		String ds = cubes.get(1)[cubemap.get("?CUBE_NAME")].toString();

		// We could also make obs a variable.
		whereClause += "?obs qb:dataSet <" + ds + ">. ";

		// 2. Dimension triple patterns

		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(dimensions.get(0));
		// First is header
		for (int i = 1; i < dimensions.size(); i++) {
			Node dimensionProperty = dimensions.get(i)[dimensionmap
					.get("?DIMENSION_UNIQUE_NAME")];

			// As always disregard measure dimension.
			// Since we do not know how Measure Dimension is encoded as Node, we
			// compare the strings.
			if (dimensionProperty.toString().equals(
					Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
				continue;
			}

			Node dimensionPropertyVariable = Olap4ldLinkedDataUtil
					.makeUriToVariable(dimensionProperty);
			whereClause += " ?obs <" + dimensionProperty + "> "
					+ dimensionPropertyVariable.toN3() + ". ";
			selectClause += " ?" + dimensionPropertyVariable;
		}

		// 3. Measure triple patterns

		Map<String, Integer> measuremap = Olap4ldLinkedDataUtil
				.getNodeResultFields(measures.get(0));
		// First is header
		for (int i = 1; i < measures.size(); i++) {
			Node[] measure = measures.get(i);
			// As always, remove Aggregation Function from Measure Name
			String measureProperty = measure[measuremap
					.get("?MEASURE_UNIQUE_NAME")].toString().replace(
					"AGGFUNC"
							+ measure[measuremap.get("?MEASURE_AGGREGATOR")]
									.toString()
									.replace("http://purl.org/olap#", "")
									.toUpperCase(), "");
			
			// XXX: Will not work, currently, since measure names are defined by: measureProperty, dataset, Aggregation function. See OLAP-2-SPARQL algorithm iterator.

			// We also remove aggregation function from Measure Property
			// Variable so
			// that the same property is not selected twice.
			Variable measurePropertyVariable = Olap4ldLinkedDataUtil
					.makeUriToVariable(new Resource(measureProperty));

			// Unique name for variable
			// Node uniqueMeasurePropertyVariable = Olap4ldLinkedDataUtil
			// .makeUriToVariable(measure[measuremap
			// .get("?MEASURE_UNIQUE_NAME")].toString());

			// We take the aggregator from the measure
			// Since we use OPTIONAL, there might be empty columns,
			// which is why we need
			// to convert them to decimal.
			// selectClause += " ("
			// + measure[measuremap.get("?MEASURE_AGGREGATOR")].toString()
			// .replace("http://purl.org/olap#", "") + "(?"
			// + measurePropertyVariable + ") as ?"
			// + uniqueMeasurePropertyVariable + ")";

			selectClause += measurePropertyVariable.toN3() + " ";

			whereClause += "?obs <" + measureProperty + "> "
					+ measurePropertyVariable.toN3() + ".";
		}

		this.query = Olap4ldLinkedDataUtil.getStandardPrefixes() + "select "
				+ selectClause + " where { " + whereClause + " } ";

	}

	/**
	 * We only return always the same thing.
	 */
	public boolean hasNext() {
		if (this.outputiterator == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return outputiterator.hasNext();
	}

	@Override
	public Object next() {
		if (this.outputiterator == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return outputiterator.next();
	}

	/**
	 * Simply copied over from embedded sesame.
	 * 
	 * @param query
	 * @param caching
	 * @return
	 */
	private List<Node[]> sparql(String query) {

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

		return myBindings;
	}

	@Override
	public void remove() {
		// nothing to do?
	}

	public String toString() {
		Map<String, Integer> map = Olap4ldLinkedDataUtil
				.getNodeResultFields(cubes.get(0));

		Node[] cubeNodes = cubes.get(1);
		int index = map.get("?CUBE_NAME");
		String cubename = cubeNodes[index].toString();

		return "BaseCube (" + cubename + "," + query + ")";
	}

	@Override
	public void init() throws Exception {
		
		// Does not have input operators, therefore no other init necessary.
		
		this.outputiterator = sparql(this.query).iterator();
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
