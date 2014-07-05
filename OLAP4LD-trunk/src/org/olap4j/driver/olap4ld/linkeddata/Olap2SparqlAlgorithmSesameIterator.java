package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Member;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

public class Olap2SparqlAlgorithmSesameIterator implements PhysicalOlapIterator {

	private List<Node[]> cubes = new ArrayList<Node[]>();
	private List<Node[]> measures = new ArrayList<Node[]>();
	private List<Node[]> dimensions = new ArrayList<Node[]>();
	private List<Node[]> newdimensions;
	private List<Node[]> hierarchies;
	private List<Node[]> levels = new ArrayList<Node[]>();
	private List<Node[]> members;

	// We collect necessary parts of the SPARQL query.
	String selectClause = " ";
	String whereClause = " ";
	String groupByClause = " ";
	String orderByClause = " ";

	private EmbeddedSesameEngine engine;
	private String query;
	private List<Node[]> result;
	private Iterator<Node[]> iterator;
	private HashMap<Integer, Integer> levelHeightMap;
	private ArrayList<Node[]> newmeasures;

	public Olap2SparqlAlgorithmSesameIterator(EmbeddedSesameEngine engine,
			List<Node[]> cubes, List<Node[]> measures, List<Node[]> dimensions,
			List<Node[]> hierarchies, List<Node[]> levels,
			List<Node[]> members, List<Node[]> slicesrollups,
			List<Integer> levelheights, List<Node[]> projections,
			List<List<Node[]>> membercombinations,
			List<Node[]> hierarchysignature) {
		this.engine = engine;

		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		// One problem could be that this may be a huge number of members.
		this.members = members;

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

		this.query = Olap4ldLinkedDataUtil.getStandardPrefixes() + "select "
				+ selectClause + askForFrom(false) + askForFrom(true)
				+ "where { " + whereClause + "}" + groupByClause
				+ orderByClause;

		// At initialisation, we execute the sparql query.
		// XXX: Should we not do this only if next() or hasNext()?

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
		HashMap<Integer, Boolean> projectedMeasureMap = new HashMap<Integer, Boolean>();
		boolean first = true;

		Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
				.getNodeResultFields(cubes.get(0));
		Map<String, Integer> measuremap = Olap4ldLinkedDataUtil
				.getNodeResultFields(measures.get(0));

		// At the same time, we can update the metadata.
		// Since we do a projection here, we should change the metadata. Only
		// keep those
		// measures that 1) are contained in projections and in measures.
		this.newmeasures = new ArrayList<Node[]>();

		for (Node[] measure : projections) {

			if (first) {
				newmeasures.add(projections.get(0));
				first = false;
				continue;
			}

			// XXX Should actually not make a difference anymore, since we do
			// not add them to projections in the first place.

			// Make sure that the projected measure actually is contained in
			// measures and add if so
			boolean contained = false;
			for (Node[] aMeasure : measures) {
				// add to newmeasures if 1) are contained in measures
				if (aMeasure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.equals(measure[measuremap.get("?MEASURE_UNIQUE_NAME")]
								.toString())) {
					contained = true;
				}
			}
			if (!contained) {
				continue;
			}
			newmeasures.add(measure);

			// For formulas, this is different
			// XXX: Needs to be put before creating the Logical Olap Operator
			// Tree
			// Will probably not work since MEASURE_AGGREGATOR
			if (measure[measuremap.get("?MEASURE_AGGREGATOR")].toString()
					.equals("http://purl.org/olap#calculated")) {

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
				// And now, see below
				String measureProperty = measure[measuremap
						.get("?MEASURE_UNIQUE_NAME")]
						.toString()
						.replace(
								"AGGFUNC"
										+ measure[measuremap.get("?MEASURE_AGGREGATOR")]
												.toString()
												.replace(
														"http://purl.org/olap#",
														"").toUpperCase(), "");
				// And now, also (possibly) remove Dataset Name from Measure
				// Name
				measureProperty = measureProperty.replace(
						cubes.get(1)[cubemap.get("?CUBE_NAME")].toString(), "");

				// We also remove aggregation function from Measure Property
				// Variable so
				// that the same property is not selected twice.
				String measurePropertyVariableString = measure[measuremap
						.get("?MEASURE_UNIQUE_NAME")]
						.toString()
						.replace(
								"AGGFUNC"
										+ measure[measuremap.get("?MEASURE_AGGREGATOR")]
												.toString()
												.replace(
														"http://purl.org/olap#",
														"").toUpperCase(), "");
				// Also, we remove dataset name
				measurePropertyVariableString = measurePropertyVariableString
						.replace(cubes.get(1)[cubemap.get("?CUBE_NAME")]
								.toString(), "");

				Node measurePropertyVariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(new Resource(
								measurePropertyVariableString));

				// Unique name for variable
				Node uniqueMeasurePropertyVariable = Olap4ldLinkedDataUtil
						.makeUriToVariable(measure[measuremap
								.get("?MEASURE_UNIQUE_NAME")]);

				String aggregationfunction;

				if (measure[measuremap.get("?MEASURE_AGGREGATOR")].toString()
						.equals("null")) {
					// In this case, we can only use top
					aggregationfunction = "Max";
				} else {
					aggregationfunction = measure[measuremap
							.get("?MEASURE_AGGREGATOR")].toString().replace(
							"http://purl.org/olap#", "");
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
				if (!projectedMeasureMap
						.containsKey(measureProperty.hashCode())) {
					whereClause += "?obs <" + measureProperty + "> ?"
							+ measurePropertyVariable + ".";
					projectedMeasureMap.put(measureProperty.hashCode(), true);
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
				// Since level_number counts from the root (ALL level) to
				// compute Dices Level Height, we need the levelmaxnumber of
				// the hierarchy. Thus, we also have the hierarchysignature.
				Integer diceslevelHeight = levelmaxnumber - levelnumber;
				Node dimensionProperty = membercombinations.get(0).get(i)[map
						.get("?DIMENSION_UNIQUE_NAME")];

				Integer slicesRollupsLevelHeight = levelHeightMap
						.get(dimensionProperty.hashCode());

				// Check on possibly existing graph patterns
				if (slicesRollupsLevelHeight == null) {
					String levelURI = membercombinations.get(0).get(i)[map
							.get("?LEVEL_UNIQUE_NAME")].toString();

					// Also considers equivalence classes
					whereClause += addLevelPropertyPath(0, diceslevelHeight,
							dimensionProperty, levelURI);
				} else if (new Integer(
						membercombinations.get(0).get(i)[map
								.get("?MEMBER_TYPE")].toString()) != Member.Type.MEASURE
						.ordinal()
						&& diceslevelHeight > slicesRollupsLevelHeight) {

					String levelURI = membercombinations.get(0).get(i)[map
							.get("?LEVEL_UNIQUE_NAME")].toString();

					// Also considers equivalence classes
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
									.get("?DIMENSION_UNIQUE_NAME")]);

					// We need to know the member to filter
					Node memberResource = membercombination.get(i)[map1
							.get("?MEMBER_UNIQUE_NAME")];

					int levelnumber = new Integer(
							membercombination.get(i)[map1.get("?LEVEL_NUMBER")]
									.toString());
					int levelmaxnumber = new Integer(
							hierarchysignature.get(i)[signaturemap1
									.get("?HIERARCHY_MAX_LEVEL_NUMBER")]
									.toString());

					// Need to know the level of the member
					Integer diceslevelHeight = levelmaxnumber - levelnumber;

					// How about: Here adding a variable for the
					// memberResource and later, for
					// each memberResource adding a new filter considering
					// equivalences.
					// andList.add(" ?" + dimensionPropertyVariable
					// + diceslevelHeight + " = " + "?"
					// + memberResourceVariable + " ");
					// andList.add(this.engine.createConditionConsiderEquivalences(
					// memberResource, new Variable(
					// dimensionPropertyVariable.toString()
					// + diceslevelHeight.toString())));
					andList.add("?" + dimensionPropertyVariable.toString()
							+ diceslevelHeight.toString() + " = " + "<"
							+ memberResource.toString() + ">");

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
			Node dimensionProperty = slicesrollups.get(i)[map
					.get("?DIMENSION_UNIQUE_NAME")];
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

		// Since we do a slice here, we should change the metadata.
		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(dimensions.get(0));
		Map<String, Integer> levelmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(levels.get(0));

		newdimensions = new ArrayList<Node[]>();

		boolean first = true;
		for (Node[] dimension : this.dimensions) {
			// Only if a level is contained in slicesrollups add
			// Is header added automatically?
			if (first) {
				newdimensions.add(dimensions.get(0));
				first = false;
				continue;
			}
			// Measure dimension should also be added.
			if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
					.toString().equals(
							Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
				newdimensions.add(dimension);
			}
			for (Node[] slicesrollup : slicesrollups) {
				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								slicesrollup[levelmap
										.get("?DIMENSION_UNIQUE_NAME")]
										.toString())) {
					newdimensions.add(dimension);
				}
			}
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
	 * This method returns graph patterns for a dimension assignment as well as
	 * property path for levels.
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
			Integer levelHeight, Node dimensionProperty, String levelURI) {

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
			// whereClause += "?obs <" + dimensionProperty + "> ?"
			// + dimensionPropertyVariable + levelHeight + ". ";

			// If level height is 0, it could be that no level is existing which
			// is why we leave that
			// pattern out.

			/*
			 * We need to extend this with entity consolidation. For that, we
			 * use an additional variable for the dimensionProperty (canonical
			 * name)
			 */

			// The variable we adapt from the dimensionProperty variable
			Variable dimensionPropertyDimensionVariable = Olap4ldLinkedDataUtil
					.makeUriToVariable(new Resource(dimensionProperty
							+ "Dimension"));

			// String condition = engine.createConditionConsiderEquivalences(
			// dimensionProperty, dimensionPropertyDimensionVariable);
			String condition = "?" + dimensionPropertyDimensionVariable.toString()
					+ " = <" + dimensionProperty + ">";

			whereClause += "?obs ?" + dimensionPropertyDimensionVariable + " ?"
					+ dimensionPropertyVariable + levelHeight + ". ";
			whereClause += " filter(" + condition + ") ";

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

			/*
			 * XXX Not done for this if branch, yet: We need to extend this with
			 * entity consolidation. For that, we use an additional variable for
			 * the dimensionProperty (canonical name)
			 */

		}

		return whereClause;

	}

	public boolean hasNext() {
		// Init?
		if (result == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return iterator.hasNext();
	}

	/**
	 * We use the iterator of the sparql query result.
	 * 
	 * The sparql query result returns for each non-empty fact a node array
	 * (List<Node[]>) with each
	 */
	public Object next() {

		// Init?
		if (result == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return iterator.next();
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
	}

	@Override
	public void init() throws Exception {

		if (result == null) {

			this.result = engine.sparql(query, false);

			// Not done, anymore.
			// After evaluation, we do "entity-consolidation"
//			this.result = this.engine
//					.replaceIdentifiersWithCanonical(this.result);
		}

		// Does not have input operators, therefore no other init necessary.

		this.iterator = this.result.iterator();

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
		return "OLAP-2-SPARQL query (SparqlSesame): " + query;
	}

	@Override
	public List<Node[]> getCubes(Restrictions restrictions)
			throws OlapException {
		return cubes;
	}

	@Override
	public List<Node[]> getDimensions(Restrictions restrictions)
			throws OlapException {
		return newdimensions;
	}

	@Override
	public List<Node[]> getMeasures(Restrictions restrictions)
			throws OlapException {
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

}
