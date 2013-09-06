package org.olap4j.driver.olap4ld.linkeddata;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Member;
import org.openrdf.repository.sail.SailRepository;
import org.semanticweb.yars.nx.Node;

/**
 * Converts from relational algebra plans to physical access plans.
 * 
 * @author benedikt
 */
public class LogicalOlap2SparqlOlapVisitor implements
		LogicalOlapOperatorQueryPlanVisitor {
	// the new root node
	ExecIterator _root;

	// We collect necessary parts of the SPARQL query.
	String selectClause = " ";
	String whereClause = " ";
	String groupByClause = " group by ";
	String orderByClause = " order by ";

	// Helper attributes
	private HashMap<Integer, Integer> levelHeightMap;

	// For the moment, we know the repo (we could wrap it also)
	private SailRepository repo;

	/**
	 * Constructor.
	 * 
	 * @param repo
	 * 
	 * @param ds
	 */
	public LogicalOlap2SparqlOlapVisitor(SailRepository repo) {
		this.repo = repo;
	}

	public void visit(RollupOp o) throws QueryException {
		// For rollup,
		RollupOp so = (RollupOp) o;

		List<Node[]> rollups = so.getRollups();
		List<Node[]> rollupssignature = so.getRollupsSignature();

		// HashMaps for level height of dimension
		this.levelHeightMap = new HashMap<Integer, Integer>();

		// First is header
		for (int i = 1; i < rollups.size(); i++) {

			Map<String, Integer> map = Olap4ldLinkedDataUtil
					.getNodeResultFields(rollups.get(0));
			Map<String, Integer> mapsignature = Olap4ldLinkedDataUtil
					.getNodeResultFields(rollupssignature.get(0));

			// At the moment, we do not support hierarchy/roll-up, but
			// simply
			// query for all dimension members
			// String hierarchyURI = LdOlap4jUtil.decodeUriForMdx(hierarchy
			// .getUniqueName());
			String dimensionProperty = rollups.get(i)[map
					.get("?DIMENSION_UNIQUE_NAME")].toString();
			// XXX Why exactly do I include the level?
			String levelURI = rollups.get(i)[map.get("?LEVEL_UNIQUE_NAME")]
					.toString();

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
			Integer levelHeight = (new Integer(
					rollupssignature.get(i)[mapsignature
							.get("?HIERARCHY_MAX_LEVEL_NUMBER")].toString()) - new Integer(
					rollups.get(i)[map.get("?LEVEL_NUMBER")].toString()));

			// Note as inserted.
			levelHeightMap.put(dimensionProperty.hashCode(), levelHeight);

			// Since level_number = 0 means a slice, we start with
			whereClause += addLevelPropertyPath(0, levelHeight,
					dimensionProperty, levelURI);

			String dimensionPropertyVariable = makeUriToParameter(dimensionProperty);
			selectClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
			groupByClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
			orderByClause += " ?" + dimensionPropertyVariable + levelHeight
					+ " ";
		}
	}

	public void visit(SliceOp o) throws QueryException {
		@SuppressWarnings("unused")
		SliceOp so = (SliceOp) o;

		// we do not need to do anything with SliceOp
		;
	}

	public void visit(DiceOp o) throws QueryException {
		DiceOp dop = (DiceOp) o;

		List<List<Node[]>> membercombinations = dop.getMemberCombinations();
		List<Node[]> hierarchysignature = dop.getHierarchySignature();

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
				// Dices Level Height
				Integer diceslevelHeight = levelmaxnumber - levelnumber;
				String dimensionProperty = membercombinations.get(0).get(i)[map
						.get("?DIMENSION_UNIQUE_NAME")].toString();

				Integer slicesRollupsLevelHeight = levelHeightMap
						.get(dimensionProperty.hashCode());

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
					String dimensionPropertyVariable = makeUriToParameter(membercombination
							.get(i)[map1.get("?DIMENSION_UNIQUE_NAME")]
							.toString());

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

	public void visit(ProjectionOp o) throws QueryException {
		ProjectionOp po = (ProjectionOp) o;

		List<Node[]> projections = po.getProjectedMeasures();

		// Now, for each measure, we create a measure value column.
		// Any measure should be contained only once, unless calculated
		// measures
		HashMap<Integer, Boolean> measureMap = new HashMap<Integer, Boolean>();
		Map<String, Integer> map = Olap4ldLinkedDataUtil
				.getNodeResultFields(projections.get(0));
		boolean first = true;

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
										.toString().replace(
												"http://purl.org/olap#", ""),
						"");
				String measurePropertyVariable = makeUriToParameter(measure[map
						.get("?MEASURE_UNIQUE_NAME")].toString());

				// We take the aggregator from the measure
				// Since we use OPTIONAL, there might be empty columns,
				// which is why we need
				// to convert them to decimal.
				selectClause += " ("
						+ measure[map.get("?MEASURE_AGGREGATOR")].toString()
								.replace("http://purl.org/olap#", "") + "(?"
						+ measurePropertyVariable + ") as ?"
						+ measurePropertyVariable + ")";

				// According to spec, every measure needs to be set for every
				// observation
				if (!measureMap.containsKey(measureProperty.hashCode())) {
					whereClause += "?obs <" + measureProperty + "> ?"
							+ measurePropertyVariable + ".";
					measureMap.put(measureProperty.hashCode(), true);
				}
			}
		}
	}

	public void visit(BaseCubeOp o) throws QueryException {
		BaseCubeOp so = (BaseCubeOp) o;

		Map<String, Integer> map = Olap4ldLinkedDataUtil.getNodeResultFields(so
				.getCube().get(0));

		// Cube gives us the URI of the dataset that we want to resolve
		String ds = so.getCube().get(1)[map.get("CUBE_NAME")].toString();

		whereClause += "?obs qb:dataSet" + ds + ". ";

		// TODO One possibility is it to directly create the tree
		// Add physical operator that retrieves data from URI and stores it
		// in triple store
		// ExecIterator bi = null;
		// _root = bi;
	}

	/**
	 * After iteration, create new root. I want to have simple SPARQL query +
	 * load RDF to store.
	 */
	public Object getNewRoot() {

		// Initialise triple store with dataset URI

		// Query triple store with SPARQL query
		// Now that we consider DSD in queries, we need to consider DSD
		// locations
		String query = Olap4ldLinkedDataUtil.getStandardPrefixes() + "select "
				+ selectClause + askForFrom(false) + askForFrom(true)
				+ "where { " + whereClause + "}" + groupByClause
				+ orderByClause;

		// Currently, we should have retrieved the data, already, therefore, we
		// only have one node.
		_root = new SparqlSesameExecIterator(repo, query);

		return _root;
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

		String dimensionPropertyVariable = makeUriToParameter(dimensionProperty);

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

	/**
	 * SPARQL does not allow all characters as parameter names, e.g.,
	 * ?sdmx-measure:obsValue. Therefore, we transform the URI representation
	 * into a parameter.
	 * 
	 * @param uriRepresentation
	 * @return
	 */
	private String makeUriToParameter(String uriRepresentation) {
		// We simply remove all special characters
		uriRepresentation = uriRepresentation.replaceAll("[^a-zA-Z0-9]+", "");
		return uriRepresentation;
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
