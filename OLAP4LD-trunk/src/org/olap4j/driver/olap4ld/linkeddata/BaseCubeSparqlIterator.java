package org.olap4j.driver.olap4ld.linkeddata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.semanticweb.yars.nx.parser.NxParser;

public class BaseCubeSparqlIterator implements PhysicalOlapIterator {
	
	private List<Node[]> cubes;
	private List<Node[]> measures;
	private List<Node[]> dimensions;
	private List<Node[]> hierarchies;
	private List<Node[]> levels;
	private List<Node[]> members;
	private SailRepository repo;
	
	public BaseCubeSparqlIterator(SailRepository repo, List<Node[]> cubes, List<Node[]> measures,
			List<Node[]> dimensions, List<Node[]> hierarchies,
			List<Node[]> levels, List<Node[]> members) {
		// We assume that basecube has a repo with populated according to metadata
		this.repo = repo;
		//We assume that cubes to BaseCubeOp always refer to one single cube
		assert cubes.size() <= 2;
		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		this.members = members;
	}
	
	/**
	 * We only return always the same thing.
	 */
	public boolean hasNext() {
		
		return true;
	}

	@Override
	public Object next() {
		List<List<Node[]>> metadata = new ArrayList<List<Node[]>>();
		metadata.add(cubes);
		metadata.add(measures);
		metadata.add(dimensions);
		metadata.add(hierarchies);
		metadata.add(levels);
		metadata.add(members);
		
		// Instead of simply returning the metadata, we can return the
		// List of List of nodes representing the observations.
		// We create a SPARQL query asking for all observations 
		// with all dimensions and measures.
		
		// We collect necessary parts of the SPARQL query.
		String selectClause = " ";
		String whereClause = " ";
		String groupByClause = " group by ";
		String orderByClause = " order by ";
		
		// 1. Get cube add triple patterns
		Map<String, Integer> cubemap = Olap4ldLinkedDataUtil.getNodeResultFields(cubes.get(0));

		// Cube gives us the URI of the dataset that we want to resolve
		String ds = cubes.get(1)[cubemap.get("?CUBE_NAME")].toString();

		// We could also make obs a variable.
		whereClause += "?obs qb:dataSet" + ds + ". ";
		
		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil.getNodeResultFields(dimensions.get(0));
		for (int i = 1; i < dimensions.size(); i++) {
			String dimensionProperty = dimensions.get(i)[dimensionmap.get("?DIMENSION_UNIQUE_NAME")].toString();
			String dimensionPropertyVariable = Olap4ldLinkedDataUtil.makeUriToParameter(dimensionProperty);
			whereClause += "?obs <"+dimensionProperty+"> ?"+dimensionPropertyVariable;
			selectClause += " ?"+dimensionPropertyVariable;
		}
		
		Map<String, Integer> measuremap = Olap4ldLinkedDataUtil.getNodeResultFields(measures.get(0));
		for (int i = 1; i < measures.size(); i++) {
			String measureProperty = measures.get(i)[measuremap.get("?MEASURE_UNIQUE_NAME")].toString();
			String measurePropertyVariable = Olap4ldLinkedDataUtil.makeUriToParameter(measureProperty);
			whereClause += "?obs <"+measureProperty+"> ?"+measurePropertyVariable;
			selectClause += " ?"+measurePropertyVariable;
		}
		
		
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes() + "select "
				+ selectClause + "where { " + whereClause + "}" + groupByClause
				+ orderByClause;
		
		
		return sparql(query);
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

}
