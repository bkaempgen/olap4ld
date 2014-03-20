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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Member;
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

public class Olap2SparqlAlgorithmSesameIterator implements PhysicalOlapIterator {

	private List<Node[]> cubes = new ArrayList<Node[]>();
	private List<Node[]> measures = new ArrayList<Node[]>();
	@SuppressWarnings("unused")
	private List<Node[]> dimensions = new ArrayList<Node[]>();
	// private List<Node[]> hierarchies;
	@SuppressWarnings("unused")
	private List<Node[]> levels = new ArrayList<Node[]>();
	// private List<Node[]> members;

	// We collect necessary parts of the SPARQL query.
	String selectClause = " ";
	String whereClause = " ";
	String groupByClause = " ";
	String orderByClause = " ";

	private SailRepository repo;
	private String query;
	private List<Node[]> result;
	private Iterator<Node[]> iterator;
	private HashMap<Integer, Integer> levelHeightMap;
	
	public Olap2SparqlAlgorithmSesameIterator(SailRepository repo,
			List<Node[]> cubes, List<Node[]> measures, List<Node[]> dimensions,
			List<Node[]> levels, List<Node[]> slicesrollups,
			List<Integer> levelheights, List<Node[]> projections,
			List<List<Node[]>> membercombinations,
			List<Node[]> hierarchysignature) {
		this.repo = repo;

		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		// this.hierarchies = so.hierarchies;
		this.levels = levels;
		// One problem could be that this may be a huge number of members.
		// this.members = so.members;

		// Maybe first check that every metadata element at least has one
		// header?

		// Prepare inputs

		// Initialise triple store with dataset URI

		// Evaluate cube
		evaluateCube();

		// Evaluate SlicesRollups

		// HashMaps for level height of dimension
		this.levelHeightMap = new HashMap<Integer, Integer>();

		/*
		 * At Roll-up, all dimensions are mentioned which are not sliced. At
		 * Slice, all sliced dimensions are mentioned. Thus, we assume
		 * SlicesRollups to contain all dimensions that are not sliced (even if
		 * no actual roll-up is done).
		 */
		evaluateSlicesRollups(slicesrollups, levelheights);

		// Evaluate Dices
		evaluateDices(membercombinations, hierarchysignature);

		// Evaluate Projections
		evaluateProjections(projections);

		// Query triple store with SPARQL query
		// Now that we consider DSD in queries, we need to consider DSD
		// locations
		if (groupByClause.equals(" ")) {
			groupByClause = "";
		} else {
			groupByClause = " group by " + groupByClause;
		}

		if (orderByClause.equals(" ")) {
			orderByClause = "";
		} else {
			orderByClause = " order by " + orderByClause;
		}

		query = Olap4ldLinkedDataUtil.getStandardPrefixes() + "select "
				+ selectClause + askForFrom(false) + askForFrom(true)
				+ "where { " + whereClause + "}" + groupByClause
				+ orderByClause;

		// At initialisation, we execute the sparql query.
		// XXX: Should we not do this only if next() or hasNext()?
		this.result = sparql();

		this.iterator = result.iterator();
	}

	/**
	 * Simply copied over from embedded sesame.
	 * 
	 * @param query
	 * @param caching
	 * @return
	 */
	private List<Node[]> sparql() {

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

	/**
	 * Should correspond to OLAP-2-SPARQL algorithm.
	 * 
	 * 1) Add named measures.
	 * 
	 * We use aggregation functions as defined by QB4OLAP.
	 * 
	 * Different from original OLAP-2-SPARQL algorithm, we also create implicit
	 * measures.
	 * 
	 */
	private void evaluateProjections(List<Node[]> projections) {
		// Now, for each measure, we create a measure value column.
		// Any measure should be contained only once, unless calculated
		// measures
		HashMap<Integer, Boolean> measureMap = new HashMap<Integer, Boolean>();
		boolean first = true;

		Map<String, Integer> map = Olap4ldLinkedDataUtil
				.getNodeResultFields(measures.get(0));

		for (Node[] measure : projections) {

			if (first) {
				first = false;
				continue;
			}

			// For formulas, this is different
			// XXX: Needs to be put before creating the Logical Olap Operator
			// Tree
			// Will probably not work since MEASURE_AGGREGATOR
			if (measure[map.get("?MEASURE_AGGREGATOR")].toString().equals(
					"http://purl.org/olap#calculated")) {

				/*
				 * For now, hard coded. Here, I also partly evaluate a query
				 * string, thus, I could do the same with filters.
				 */
				// Measure measure1 = (Measure) ((MemberNode) ((CallNode)
				// measure
				// .getExpression()).getArgList().get(0)).getMember();
				// Measure measure2 = (Measure) ((MemberNode) ((CallNode)
				// measure
				// .getExpression()).getArgList().get(1)).getMember();
				//
				// String operatorName = ((CallNode) measure.getExpression())
				// .getOperatorName();

				// Measure
				// Measure property has aggregator attached to it at the end,
				// e.g., "AVG".
				// String measureProperty1 = Olap4ldLinkedDataUtil
				// .convertMDXtoURI(measure1.getUniqueName()).replace(
				// "AGGFUNC" + measure.getAggregator().name(), "");
				// We do not encode the aggregation function in the measure,
				// any
				// more.
				// String measurePropertyVariable1 = makeUriToParameter(measure1
				// .getUniqueName());
				//
				// String measureProperty2 = Olap4ldLinkedDataUtil
				// .convertMDXtoURI(measure2.getUniqueName()).replace(
				// "AGGFUNC" + measure.getAggregator().name(), "");
				// String measurePropertyVariable2 = makeUriToParameter(measure2
				// .getUniqueName());

				// We take the aggregator from the measure
				// selectClause += " " + measure1.getAggregator().name() + "(?"
				// + measurePropertyVariable1 + " " + operatorName + " "
				// + "?" + measurePropertyVariable2 + ")";
				//
				// // I have to select them only, if we haven't inserted any
				// // before.
				// if (!measureMap.containsKey(measureProperty1.hashCode())) {
				// whereClause += " ?obs <" + measureProperty1 + "> ?"
				// + measurePropertyVariable1 + ". ";
				// measureMap.put(measureProperty1.hashCode(), true);
				// }
				// if (!measureMap.containsKey(measureProperty2.hashCode())) {
				// whereClause += " ?obs <" + measureProperty2 + "> ?"
				// + measurePropertyVariable2 + ". ";
				// measureMap.put(measureProperty2.hashCode(), true);
				// }

			} else {

				// As always, remove Aggregation Function from Measure Name
				String measureProperty = measure[map
						.get("?MEASURE_UNIQUE_NAME")].toString().replace(
						"AGGFUNC"
								+ measure[map.get("?MEASURE_AGGREGATOR")]
										.toString()
										.replace("http://purl.org/olap#", "")
										.toUpperCase(), "");

				// We also remove aggregation function from Measure Property
				// Variable so
				// that the same property is not selected twice.
				Node measurePropertyVariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(measure[map
								.get("?MEASURE_UNIQUE_NAME")]
								.toString()
								.replace(
										"AGGFUNC"
												+ measure[map
														.get("?MEASURE_AGGREGATOR")]
														.toString()
														.replace(
																"http://purl.org/olap#",
																"")
														.toUpperCase(), ""));

				// Unique name for variable
				Node uniqueMeasurePropertyVariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(measure[map
								.get("?MEASURE_UNIQUE_NAME")].toString());

				String aggregationfunction = measure[map
						.get("?MEASURE_AGGREGATOR")].toString().replace(
						"http://purl.org/olap#", "");

				if (measure[map.get("?MEASURE_AGGREGATOR")].toString().equals(
						"null")) {
					// In this case, we can only use top
					aggregationfunction = "Max";
				}

				// We take the aggregator from the measure
				// Since we use OPTIONAL, there might be empty columns,
				// which is why we need
				// to convert them to decimal.
				selectClause += " (" + aggregationfunction + "(?"
						+ measurePropertyVariable + ") as ?"
						+ uniqueMeasurePropertyVariable + "_new )";

				// According to spec, every measure needs to be set for every
				// observation only once
				if (!measureMap.containsKey(measureProperty.hashCode())) {
					whereClause += "?obs <" + measureProperty + "> ?"
							+ measurePropertyVariable + ".";
					measureMap.put(measureProperty.hashCode(), true);
				}
			}
		}
	}

	/**
	 * Should correspond to OLAP-to-SPARQL algorithm in SSB paper. There:
	 * 
	 * 1) We take the first position as a template of what hierarchies /
	 * dimensions are filtered for. (XXX: Properly align that with description
	 * of CellSet in olap4ld paper)
	 * 
	 * 2) Check whether new graph patterns needed
	 * 
	 * 3) Add filters
	 * 
	 * @param membercombinations
	 * 
	 */
	private void evaluateDices(List<List<Node[]>> membercombinations,
			List<Node[]> hierarchysignature) {
		// For each filter tuple
		if (membercombinations != null && !membercombinations.isEmpty()) {

			// We assume that each position has the same metadata (i.e.,
			// Levels)
			// so that we only need to add graph patterns for the first
			// position
			Map<String, Integer> map = Olap4ldLinkedDataUtil
					.getNodeResultFields(membercombinations.get(0).get(0));
			Map<String, Integer> signaturemap = Olap4ldLinkedDataUtil
					.getNodeResultFields(hierarchysignature.get(0));

			// Check whether new graph patterns needed
			// First is header
			for (int i = 1; i < membercombinations.get(0).size(); i++) {
				int levelnumber = new Integer(
						membercombinations.get(0).get(i)[map
								.get("?LEVEL_NUMBER")].toString());
				int levelmaxnumber = new Integer(
						hierarchysignature.get(i)[signaturemap
								.get("?HIERARCHY_MAX_LEVEL_NUMBER")].toString());
				// We should be testing whether diceslevelHeight higher than
				// slicesRollupsLevelHeight
				// To compute Dices Level Height, we need the levelmaxnumber of
				// the hierarchy. Thus, we also have the hierarchysignature.
				Integer diceslevelHeight = levelmaxnumber - levelnumber;
				String dimensionProperty = membercombinations.get(0).get(i)[map
						.get("?DIMENSION_UNIQUE_NAME")].toString();

				Integer slicesRollupsLevelHeight = levelHeightMap
						.get(dimensionProperty.hashCode());

				// Check on possibly existing graph patterns
				if (slicesRollupsLevelHeight == null) {
					String levelURI = membercombinations.get(0).get(i)[map
							.get("?LEVEL_UNIQUE_NAME")].toString();

					whereClause += addLevelPropertyPath(0, diceslevelHeight,
							dimensionProperty, levelURI);
				} else if (new Integer(
						membercombinations.get(0).get(i)[map
								.get("?MEMBER_TYPE")].toString()) != Member.Type.MEASURE
						.ordinal()
						&& diceslevelHeight > slicesRollupsLevelHeight) {

					String levelURI = membercombinations.get(0).get(i)[map
							.get("?LEVEL_UNIQUE_NAME")].toString();

					whereClause += addLevelPropertyPath(
							slicesRollupsLevelHeight, diceslevelHeight,
							dimensionProperty, levelURI);

				}
			}

			// Add filter
			whereClause += " FILTER (";
			List<String> orList = new ArrayList<String>();

			for (List<Node[]> membercombination : membercombinations) {

				// Each position is an OR
				List<String> andList = new ArrayList<String>();

				Map<String, Integer> map1 = Olap4ldLinkedDataUtil
						.getNodeResultFields(membercombination.get(0));
				Map<String, Integer> signaturemap1 = Olap4ldLinkedDataUtil
						.getNodeResultFields(hierarchysignature.get(0));

				// First is header
				for (int i = 1; i < membercombination.size(); i++) {
					// We need to know the variable to filter
					// First, we need to convert it to URI representation.
					Node dimensionPropertyVariable = Olap4ldLinkedDataUtil
							.makeUriToVariable(membercombination.get(i)[map1
									.get("?DIMENSION_UNIQUE_NAME")].toString());

					// We need to know the member to filter
					String memberResource = membercombination.get(i)[map1
							.get("?MEMBER_UNIQUE_NAME")].toString();

					int levelnumber = new Integer(
							membercombination.get(i)[map1.get("?LEVEL_NUMBER")]
									.toString());
					int levelmaxnumber = new Integer(
							hierarchysignature.get(i)[signaturemap1
									.get("?HIERARCHY_MAX_LEVEL_NUMBER")]
									.toString());

					// Need to know the level of the member
					Integer diceslevelHeight = levelmaxnumber - levelnumber;

					if (isResourceAndNotLiteral(memberResource)) {
						andList.add(" ?" + dimensionPropertyVariable
								+ diceslevelHeight + " = " + "<"
								+ memberResource + "> ");
					} else {
						// For some reason, we need to convert the variable
						// using str.
						andList.add(" str(?" + dimensionPropertyVariable
								+ diceslevelHeight + ") = " + "\""
								+ memberResource + "\" ");
					}

				}
				// For sesame, instead of AND is &&
				orList.add(Olap4ldLinkedDataUtil.implodeArray(
						andList.toArray(new String[0]), " && "));
			}
			// For sesame, instead of OR is ||
			whereClause += Olap4ldLinkedDataUtil.implodeArray(
					orList.toArray(new String[0]), " || ");
			whereClause += ") ";

		}

		// This huge thing probably tries to combine filter expressions to
		// efficient SPARQL expressions
		// // First, what dimension?
		// // If Measures, then do not filter (since the
		// // selectionpredicates also contain measures, since that was
		// // easier to do beforehand)
		// try {
		// if (position.get(0).getDimension().getDimensionType()
		// .equals(Dimension.Type.MEASURE)) {
		// continue;
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // We assume that all contained members are of the same
		// // dimension and the same level
		// String slicerDimensionProperty = LdOlap4jUtil
		// .convertMDXtoURI(position.get(0).getDimension()
		// .getUniqueName());
		// String slicerDimensionPropertyVariable = makeParameter(position
		// .get(0).getDimension().getUniqueName());
		//
		// /*
		// * slicerLevelNumber would give me the difference between the
		// * maximal depth of all levels and the level depth (presumably
		// * the same for all tuples). This tells us the number of
		// * property-value pairs from the most granular element.
		// */
		// Integer slicerLevelNumber = (position.get(0).getLevel()
		// .getHierarchy().getLevels().size() - 1 - position
		// .get(0).getLevel().getDepth());
		//
		// /*
		// * Here, instead of going through all the members and filter for
		// * them one by one, we need to "compress" the filter expression
		// * somehow.
		// */
		// // Check whether the leveltype is integer and define smallest
		// // and biggest
		// boolean isInteger = true;
		// int smallestTuple = 0;
		// int biggestTuple = 0;
		// int smallestLevelMember = 0;
		// int biggestLevelMember = 0;
		// try {
		// // Initialize
		// smallestTuple = Integer.parseInt(position.get(0)
		// .getUniqueName());
		// biggestTuple = smallestTuple;
		// // Search for the smallest and the tallest member of
		// // slicerTuple
		// for (Member member : position) {
		//
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestTuple) {
		// smallestTuple = x;
		// }
		// if (x > biggestTuple) {
		// biggestTuple = x;
		// }
		// }
		// // Initialize
		// smallestLevelMember = smallestTuple;
		// biggestLevelMember = smallestTuple;
		// // Search for the smallest and the tallest member of all
		// // level
		// // members
		// try {
		// for (Member member : position.get(0).getLevel()
		// .getMembers()) {
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestLevelMember) {
		// smallestLevelMember = x;
		// }
		// if (x > biggestLevelMember) {
		// biggestLevelMember = x;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// } catch (NumberFormatException nFE) {
		// isInteger = false;
		// }
		//
		// boolean isRestricted = false;
		// try {
		// for (Member levelMember : position.get(0).getLevel()
		// .getMembers()) {
		// if (isInteger) {
		// // Then we do not need to do this
		// break;
		// }
		// boolean isContained = false;
		// for (Member slicerMember : position) {
		// // if any member is not contained in the
		// // slicerTuple, it is restricted
		// if (levelMember.getUniqueName().equals(
		// slicerMember.getUniqueName())) {
		// isContained = true;
		// break;
		// }
		// }
		// if (!isContained) {
		// isRestricted = true;
		// break;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // Now, we create the string that shall be put between filter.
		// String filterstring = "";
		// if (isInteger) {
		//
		// if (smallestTuple > smallestLevelMember) {
		// filterstring = "?" + slicerDimensionPropertyVariable
		// + " >= " + smallestTuple + " AND ";
		// }
		// if (biggestTuple < biggestLevelMember) {
		// filterstring += "?" + slicerDimensionPropertyVariable
		// + " <= " + biggestTuple;
		// }
		//
		// } else if (isRestricted) {
		// String[] slicerTupleArray = new String[position.size()];
		//
		// for (int j = 0; j < slicerTupleArray.length; j++) {
		//
		// Member member = position.get(j);
		// String memberString = LdOlap4jUtil
		// .convertMDXtoURI(member.getUniqueName());
		//
		// // Check whether member is uri or literal value
		// if (memberString.startsWith("http://")) {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = <"
		// + memberString + "> ";
		// } else {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = \""
		// + memberString + "\" ";
		// }
		// slicerTupleArray[j] = memberString;
		// }
		//
		// filterstring = LdOlap4jUtil.implodeArray(slicerTupleArray,
		// " || ");
		// } else {
		// // Nothing to do, there is nothing to filter for.
		// break;
		// }
		//
		// // Only if this kind of level has not been selected, yet, we
		// // need to do it.
		// if (levelHeightMap.containsKey(slicerDimensionProperty
		// .hashCode())) {
		// query += " FILTER(" + filterstring + ").";
		//
		// } else {
		//
		// // Since we are on a specific level, we need to add
		// // intermediary
		// // property-values
		// if (slicerLevelNumber == 0) {
		// query += "?obs <" + slicerDimensionProperty + "> ?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// } else {
		// // Level path will start with ?obs and end with
		// // slicerConcept
		// // // First, what dimension?
		// // If Measures, then do not filter (since the
		// // selectionpredicates also contain measures, since that was
		// // easier to do beforehand)
		// try {
		// if (position.get(0).getDimension().getDimensionType()
		// .equals(Dimension.Type.MEASURE)) {
		// continue;
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // We assume that all contained members are of the same
		// // dimension and the same level
		// String slicerDimensionProperty = LdOlap4jUtil
		// .convertMDXtoURI(position.get(0).getDimension()
		// .getUniqueName());
		// String slicerDimensionPropertyVariable = makeParameter(position
		// .get(0).getDimension().getUniqueName());
		//
		// /*
		// * slicerLevelNumber would give me the difference between the
		// * maximal depth of all levels and the level depth (presumably
		// * the same for all tuples). This tells us the number of
		// * property-value pairs from the most granular element.
		// */
		// Integer slicerLevelNumber = (position.get(0).getLevel()
		// .getHierarchy().getLevels().size() - 1 - position
		// .get(0).getLevel().getDepth());
		//
		// /*
		// * Here, instead of going through all the members and filter for
		// * them one by one, we need to "compress" the filter expression
		// * somehow.
		// */
		// // Check whether the leveltype is integer and define smallest
		// // and biggest
		// boolean isInteger = true;
		// int smallestTuple = 0;
		// int biggestTuple = 0;
		// int smallestLevelMember = 0;
		// int biggestLevelMember = 0;
		// try {
		// // Initialize
		// smallestTuple = Integer.parseInt(position.get(0)
		// .getUniqueName());
		// biggestTuple = smallestTuple;
		// // Search for the smallest and the tallest member of
		// // slicerTuple
		// for (Member member : position) {
		//
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestTuple) {
		// smallestTuple = x;
		// }
		// if (x > biggestTuple) {
		// biggestTuple = x;
		// }
		// }
		// // Initialize
		// smallestLevelMember = smallestTuple;
		// biggestLevelMember = smallestTuple;
		// // Search for the smallest and the tallest member of all
		// // level
		// // members
		// try {
		// for (Member member : position.get(0).getLevel()
		// .getMembers()) {
		// int x = Integer.parseInt(member.getUniqueName());
		// if (x < smallestLevelMember) {
		// smallestLevelMember = x;
		// }
		// if (x > biggestLevelMember) {
		// biggestLevelMember = x;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// } catch (NumberFormatException nFE) {
		// isInteger = false;
		// }
		//
		// boolean isRestricted = false;
		// try {
		// for (Member levelMember : position.get(0).getLevel()
		// .getMembers()) {
		// if (isInteger) {
		// // Then we do not need to do this
		// break;
		// }
		// boolean isContained = false;
		// for (Member slicerMember : position) {
		// // if any member is not contained in the
		// // slicerTuple, it is restricted
		// if (levelMember.getUniqueName().equals(
		// slicerMember.getUniqueName())) {
		// isContained = true;
		// break;
		// }
		// }
		// if (!isContained) {
		// isRestricted = true;
		// break;
		// }
		// }
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// // Now, we create the string that shall be put between filter.
		// String filterstring = "";
		// if (isInteger) {
		//
		// if (smallestTuple > smallestLevelMember) {
		// filterstring = "?" + slicerDimensionPropertyVariable
		// + " >= " + smallestTuple + " AND ";
		// }
		// if (biggestTuple < biggestLevelMember) {
		// filterstring += "?" + slicerDimensionPropertyVariable
		// + " <= " + biggestTuple;
		// }
		//
		// } else if (isRestricted) {
		// String[] slicerTupleArray = new String[position.size()];
		//
		// for (int j = 0; j < slicerTupleArray.length; j++) {
		//
		// Member member = position.get(j);
		// String memberString = LdOlap4jUtil
		// .convertMDXtoURI(member.getUniqueName());
		//
		// // Check whether member is uri or literal value
		// if (memberString.startsWith("http://")) {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = <"
		// + memberString + "> ";
		// } else {
		// memberString = "?"
		// + slicerDimensionPropertyVariable + " = \""
		// + memberString + "\" ";
		// }
		// slicerTupleArray[j] = memberString;
		// }
		//
		// filterstring = LdOlap4jUtil.implodeArray(slicerTupleArray,
		// " || ");
		// } else {
		// // Nothing to do, there is nothing to filter for.
		// break;
		// }
		//
		// // Only if this kind of level has not been selected, yet, we
		// // need to do it.
		// if (levelHeightMap.containsKey(slicerDimensionProperty
		// .hashCode())) {
		// query += " FILTER(" + filterstring + ").";
		//
		// } else {
		//
		// // Since we are on a specific level, we need to add
		// // intermediary
		// // property-values
		// if (slicerLevelNumber == 0) {
		// query += "?obs <" + slicerDimensionProperty + "> ?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// } else {
		// // Level path will start with ?obs and end with
		// // slicerConcept
		// String levelPath = "";
		//
		// for (int i = 0; i < slicerLevelNumber; i++) {
		// levelPath += " ?" + slicerDimensionPropertyVariable
		// + i + ". ?"
		// + slicerDimensionPropertyVariable + i
		// + " skos:narrower ";
		// }
		//
		// query += "?obs <" + slicerDimensionProperty + "> "
		// + levelPath + "?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// }
		// }
		// }
		// } String levelPath = "";
		//
		// for (int i = 0; i < slicerLevelNumber; i++) {
		// levelPath += " ?" + slicerDimensionPropertyVariable
		// + i + ". ?"
		// + slicerDimensionPropertyVariable + i
		// + " skos:narrower ";
		// }
		//
		// query += "?obs <" + slicerDimensionProperty + "> "
		// + levelPath + "?"
		// + slicerDimensionPropertyVariable + " FILTER("
		// + filterstring + ").";
		// }
		// }
		// }
		// }
	}

	private void evaluateSlicesRollups(List<Node[]> slicesrollups,
			List<Integer> levelheights) {

		// First is header
		for (int i = 1; i < slicesrollups.size(); i++) {

			Map<String, Integer> map = Olap4ldLinkedDataUtil
					.getNodeResultFields(slicesrollups.get(0));

			// At the moment, we do not support hierarchy/roll-up, but
			// simply
			// query for all dimension members
			// String hierarchyURI = LdOlap4jUtil.decodeUriForMdx(hierarchy
			// .getUniqueName());
			String dimensionProperty = slicesrollups.get(i)[map
					.get("?DIMENSION_UNIQUE_NAME")].toString();
			// XXX Why exactly do I include the level? Probably, because I need
			// the level depth from the hierarchy.
			String levelURI = slicesrollups.get(i)[map
					.get("?LEVEL_UNIQUE_NAME")].toString();

			/*
			 * Now, depending on level depth we need to behave differently
			 * Slicer level is the number of property-value pairs from the most
			 * granular value For that, the maximum depth and the level depth
			 * are used. If max depth == level depth => levelHeight == 0 If max
			 * depth > level depth => levelHeight == max depth - level depth
			 * 
			 * Relationship between level_number, hierarchy_max_level_number and
			 * levelHeight:
			 * 
			 * See: http://www.linked-data-cubes.org/index.php/RollupOp#
			 * Relationship_between_level_number
			 * .2C_hierarchy_max_level_number_and_levelHeight
			 * 
			 * TODO: THis is quite a simple approach, does not consider possible
			 * access restrictions.
			 */

			// Header
			int levelHeight = levelheights.get(i);
			// Note as inserted.
			levelHeightMap.put(dimensionProperty.hashCode(), levelHeight);

			// Since level_number = 0 means a slice, we start with
			whereClause += addLevelPropertyPath(0, levelHeight,
					dimensionProperty, levelURI);

			Node dimensionPropertyVariable = Olap4ldLinkedDataUtil
					.makeUriToVariable(dimensionProperty);
			selectClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
			groupByClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
			orderByClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
		}
	}

	private void evaluateCube() {

		Map<String, Integer> map = Olap4ldLinkedDataUtil
				.getNodeResultFields(this.cubes.get(0));

		String ds = this.cubes.get(1)[map.get("?CUBE_NAME")].toString();

		whereClause += "?obs qb:dataSet <" + ds + ">. ";

		// TODO One possibility is it to directly create the tree
		// Add physical operator that retrieves data from URI and stores it
		// in triple store
		// ExecIterator bi = null;
		// _root = bi;
	}

	/**
	 * This method returns graph patterns for a property path for levels.
	 * 
	 * @param levelStart
	 *            startingPoint of level
	 * @param levelHeight
	 *            endPoint of level
	 * @param dimensionProperty
	 * @param levelURI
	 * @return
	 */
	private String addLevelPropertyPath(Integer levelStart,
			Integer levelHeight, String dimensionProperty, String levelURI) {

		String whereClause = "";

		Node dimensionPropertyVariable = Olap4ldLinkedDataUtil
				.makeUriToVariable(dimensionProperty);

		if (levelHeight == 0) {
			/*
			 * For now, we simply query for all and later match it to the pivot
			 * table. A more performant way would be to filter only for those
			 * dimension members, that are mentioned by any position. Should be
			 * easy possible to do that if we simply ask each position for the
			 * i'th position member.
			 */
			whereClause += "?obs <" + dimensionProperty + "> ?"
					+ dimensionPropertyVariable + levelHeight + ". ";

			// If level height is 0, it could be that no level is existing which
			// is why we leave that
			// pattern out.

		} else {

			// Level path will start with ?obs and end with
			// slicerConcept
			String levelPath = "";

			for (int i = levelStart; i < levelHeight; i++) {
				levelPath += " ?" + dimensionPropertyVariable + i + ". ?"
						+ dimensionPropertyVariable + i + " skos:narrower ";
			}

			whereClause += "?obs <" + dimensionProperty + "> " + levelPath
					+ "?" + dimensionPropertyVariable + levelHeight + ". ";

			// And this concept needs to be contained in the level.
			whereClause += "?" + dimensionPropertyVariable + levelHeight
					+ " skos:member <" + levelURI + ">. ";
			// Removed part of hasTopConcept, since we know the members
			// already.

		}

		return whereClause;

	}

	public boolean hasNext() {
		return iterator.hasNext();
	}

	/**
	 * We use the iterator of the sparql query result.
	 * 
	 * The sparql query result returns for each non-empty fact a node array
	 * (List<Node[]>) with each
	 */
	public Object next() {
		return iterator.next();
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
	}

	@Override
	public void init() throws Exception {
		;
	}

	@Override
	public void close() throws Exception {
		;
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		// Needed in case we have as arguments further exec iterators.
		;
	}

	/**
	 * Returns String representation of op.
	 */
	public String toString() {
		return "SparqlSesame: " + query;
	}

	@Override
	public List<Node[]> getCubes(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getDimensions(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getMeasures(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getHierarchies(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getLevels(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node[]> getMembers(Restrictions restrictions)
			throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Given a string, check whether resource or literal.
	 * 
	 * @param resource
	 * @return
	 */
	private boolean isResourceAndNotLiteral(String resource) {
		return resource.startsWith("http:");
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
	private String askForFrom(boolean isDsdQuery) {
		return " ";
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

}
