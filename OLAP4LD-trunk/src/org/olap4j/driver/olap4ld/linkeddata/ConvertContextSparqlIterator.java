package org.olap4j.driver.olap4ld.linkeddata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	private int conversionfunction;

	public static final int MIOEUR2EUR = 1;
	
	private SailRepository repo;
	private String triples;
	
	

	public ConvertContextSparqlIterator(SailRepository repo, List<Node[]> cube,
			PhysicalOlapIterator root, List<Node[]> cubes,
			List<Node[]> measures, List<Node[]> dimensions,
			List<Node[]> hierarchies, List<Node[]> levels,
			List<Node[]> members, int conversionfunction, String domainuri) {

		this.repo = repo;
		
		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		this.members = members;

		this.conversionfunction = conversionfunction;

		List<Node[]> myBindings = new ArrayList<Node[]>();

		switch (this.conversionfunction) {
		case ConvertContextSparqlIterator.MIOEUR2EUR:

			Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
					.getNodeResultFields(cubes.get(0));

			String dataset = cubes.get(1)[cubemap.get("?CUBE_NAME")].toString();

			// How to decide for the uri?
			String newdataset = null;
			try {
				newdataset = domainuri+URLEncoder.encode(dataset,"UTF-8");
			} catch (UnsupportedEncodingException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}

			// We need two queries, one for the metadata and one for the data.

			// Metadata

			// Data

			try {
				
				RepositoryConnection con = this.repo.getConnection();

				// Construct query to create triples
				String constructquery = "";

				String prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
						+ "PREFIX olap4ld:<http://purl.org/olap4ld/>";
				String construct = "_:obs qb:dataSet <"
						+ newdataset
						+ ">. "
						+ "_:obs <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg_eur. "
						+ "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
						+ "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#unit> <http://estatwrap.ontologycentral.com/dic/unit#EUR>. "
						+ "_:obs <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
						+ "_:obs <http://purl.org/dc/terms/date> ?time.";
				String select = "?geo ?unit1 ?indic_na1 ?time ((?boneg * 1000000) as ?boneg_eur) ";
				String where = "?obs1 qb:dataSet <"
						+ dataset
						+ ">. "
						+ "?obs1 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg. "
						+ "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
						+ "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#unit> ?unit1. "
						+ "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
						+ "?obs1 <http://purl.org/dc/terms/date> ?time. "
						+ "FILTER (?unit1 = <http://estatwrap.ontologycentral.com/dic/unit#MIO_EUR>)";

				constructquery = prefixes + "construct { " + construct
						+ " } where { { select " + select + " where {" + where
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
//				String insertquery = "PREFIX olap4ld:<http://purl.org/olap4ld/> INSERT DATA { GRAPH <http://manually> { "
//						+ triples + " } }";
//				
//				Olap4ldUtil._log.config("SPARQL query: " + insertquery);
//				
//				Update updateQuery = con.prepareUpdate(QueryLanguage.SPARQL,
//						insertquery);
//				updateQuery.execute();

				// Would not work: prolog error
				//ByteArrayInputStream inputstream = new ByteArrayInputStream(w.toString().getBytes());
				
				// UTF-8 encoding seems important
				InputStream stream = new ByteArrayInputStream(triples.getBytes("UTF-8"));
				
				con.add(stream, "", RDFFormat.RDFXML);
				
				// Select query for the output
				prefixes = "PREFIX qb:<http://purl.org/linked-data/cube#> "
						+ "PREFIX olap4ld:<http://purl.org/olap4ld/>";
				select = "?geo ?unit1 ?indic_na1 ?time ?boneg_eur ";
				where = "?obs1 qb:dataSet <"
						+ newdataset
						+ ">. "
						+ "?obs1 <http://purl.org/linked-data/sdmx/2009/measure#obsValue> ?boneg_eur. "
						+ "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#geo> ?geo. "
						+ "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#unit> ?unit1. "
						+ "?obs1 <http://ontologycentral.com/2009/01/eurostat/ns#indic_na> ?indic_na1. "
						+ "?obs1 <http://purl.org/dc/terms/date> ?time. ";

				String observationquery = prefixes + " select " + select
						+ " where {" + where + " }";

				Olap4ldUtil._log.config("SPARQL query: " + observationquery);
				ByteArrayOutputStream boas = new ByteArrayOutputStream();
				// FileOutputStream fos = new
				// FileOutputStream("/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/resources/result.srx");

				SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(
						boas);

				TupleQuery tupleQuery = con.prepareTupleQuery(
						QueryLanguage.SPARQL, observationquery);
				tupleQuery.evaluate(sparqlWriter);

				ByteArrayInputStream bais = new ByteArrayInputStream(
						boas.toByteArray());

				// String xmlwriterstreamString =
				// Olap4ldLinkedDataUtil.convertStreamToString(bais);
				// System.out.println(xmlwriterstreamString);
				// Transform sparql xml to nx
				InputStream nx = Olap4ldLinkedDataUtil
						.transformSparqlXmlToNx(bais);

				// Only log if needed
				if (Olap4ldUtil._isDebug) {
					String test2 = Olap4ldLinkedDataUtil
							.convertStreamToString(nx);
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

			break;

		default:
			break;
		}

		this.iterator = myBindings.iterator();
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

}
