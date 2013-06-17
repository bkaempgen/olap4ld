/**
 * 
 */
package org.olap4j.driver.olap4ld;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.text.Segment;

import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.driver.olap4ld.Olap4ldConnection.Context;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.CubeNode;
import org.olap4j.mdx.DimensionNode;
import org.olap4j.mdx.DrillThroughNode;
import org.olap4j.mdx.HierarchyNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.mdx.LevelNode;
import org.olap4j.mdx.LiteralNode;
import org.olap4j.mdx.MemberNode;
import org.olap4j.mdx.ParameterNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeVisitor;
import org.olap4j.mdx.PropertyValueNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.Syntax;
import org.olap4j.mdx.WithMemberNode;
import org.olap4j.mdx.WithSetNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Measure.Aggregator;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;

/**
 * @author b-kaempgen
 * 
 *         * In olap4ld, the following process is executed, if an MDX query is
 *         issued:
 * 
 *         * The MDX query is parsed and validated. A
 *         "Parse tree model for an MDX SELECT statement" is created: SelectNode
 *         implements ParseTreeNode
 * 
 *         * executeOlapSparqlQuery(SelectNode selectNode) is executed on this
 *         SelectNode
 * 
 *         * A MdxMethodVisitor is instantiated and goes through the entire MDX
 *         tree to create the needed information for the SPARQL OLAP query.
 * 
 *         An MdxMethodVisitor has as Input a SelectNode and as Output
 * 
 * 
 *         MdxMethodVisitor goes through a MDX parse tree and returns the
 *         multidimensional element (e.g., cube, level, list of list of
 *         members,...)
 * 
 *         Momentarily, it can be used to: * Start from a cube node to retrieve
 *         the cube * Start from an axis node to retrieve the list of list of
 *         members ** From the list of list of members, we can retrieve the
 *         hierarchy list and measures
 * 
 */
public class MdxMethodVisitor<Object> implements ParseTreeVisitor<Object> {

	private Olap4ldStatement olap4jStatement;

	private Object current = null;

	private List<Member> withMembers = new ArrayList<Member>();
	private Cube cube;
	private List<Position> columnPositions = new ArrayList<Position>();
	private List<Position> rowPositions = new ArrayList<Position>();
	private List<Position> filterPositions = new ArrayList<Position>();

	public MdxMethodVisitor(Olap4ldStatement olap4jStatement) {
		this.olap4jStatement = olap4jStatement;
	}

	/**
	 * This is the starting point of going through a MDX select parse tree.
	 */
	public Object visit(SelectNode selectNode) {

		// We do not implement with, yet.
		if (!selectNode.getWithList().isEmpty()) {
			for (ParseTreeNode with : selectNode.getWithList()) {
				/*
				 * For each with, we need to create a new calculated measure and
				 * set the expression
				 * 
				 * For that, however, we need to identify the elements in the
				 * expression, so that the OLAP engine does not need to identify
				 * them, any more.
				 */
				// We do not need to use it, since the visitor stores it in
				// itself.
				// TODO: This is a hack. Instead, I want to use calculated
				// measures with their expression.
				// TODO: Accept should work also.
				with.accept(this);
			}
		}

		// I know, that getFrom gives me a cubeNode, therefore, I know the cube
		this.cube = (Cube) selectNode.getFrom().accept(this);

		// Filter (create MetaData for the filter axis)
		AxisNode filter = selectNode.getFilterAxis();

		if (filter.getExpression() != null) {
			/*
			 * If a set of tuples is supplied as the slicer expression, MDX will
			 * attempt to evaluate the set, aggregating the result cells in
			 * every tuple along the set. In other words, MDX will attempt to
			 * use the Aggregate function on the set, aggregating each measure
			 * by its associated aggregation function. The following examples
			 * show a valid WHERE clause using a set of tuples:
			 * 
			 * WHERE { ([Time].[1st half], [Route].[nonground]), ([Time].[1st
			 * half], [Route].[ground]) }
			 * 
			 * If the «slicer_specification» cannot be resolved into a single
			 * tuple, an error will occur.
			 * 
			 * For us, a tuple is a position in the axis.
			 */

			// I should create a visitor that goes through the AxisNode (axis)
			// and returns with positions
			// MdxMethodVisitor<Object> ldMethodVisitor = new
			// MdxMethodVisitor<Object>(
			// olap4jStatement);
			Object result = filter.accept(this);
			List<Object> listResult = (List<Object>) result;

			/*
			 * From the method visitor, we get a list of (possible list of)
			 * members
			 */
			filterPositions = this.createPositionsList(listResult);

			// pw.println();
			// pw.print("WHERE ");
			// filterAxis.unparse(writer);
		}

		// When going through the axes: Prepare groupbylist, a set of levels
		// Prepare list of levels (excluding Measures!)
		// Go through tuple get dimensions, if not measure
		// ArrayList<Level> groupbylist = new ArrayList<Level>();
		List<AxisNode> axisList = selectNode.getAxisList();

		// First is columns
		AxisNode columnAxis = axisList.get(0);
		Object result = columnAxis.accept(this);
		List<Object> listResult = (List<Object>) result;

		/*
		 * From the method visitor, we get a list of (possible list of) members
		 */
		columnPositions = this.createPositionsList(listResult);

		// Second is rows
		AxisNode rowAxis = axisList.get(1);
		result = rowAxis.accept(this);
		List<Object> listResult2 = (List<Object>) result;

		/*
		 * From the method visitor, we get a list of (possible list of) members
		 */
		rowPositions = this.createPositionsList(listResult2);
		
		if (axisList.size() > 2) {
			throw new UnsupportedOperationException("Only columns and rows are supported in MDX query!");
		}

		return null;
	}

	@Override
	public Object visit(AxisNode axis) {
		// TODO Auto-generated method stub
		return axis.getExpression().accept(this);
	}

	/**
	 * In our case, a calcMember node always returns one measure with a new name
	 * and an expression of a call node with two member nodes.
	 */
	public Object visit(WithMemberNode calcMemberNode) {
		List<IdentifierSegment> identifierList = calcMemberNode.getIdentifier()
				.getSegmentList();

		String[] identifierArray = new String[identifierList.size()];

		for (int i = 0; i < identifierList.size(); i++) {
			identifierArray[i] = identifierList.get(i).toString();
		}

		String calculatedMemberName = Olap4ldLinkedDataUtil.implodeArray(
				identifierArray, ".");

		String operatorName = ((CallNode) calcMemberNode.getExpression())
				.getOperatorName();

		Member member1 = (Member) ((CallNode) calcMemberNode.getExpression())
				.getArgList().get(0).accept(this);

		Member member2 = (Member) ((CallNode) calcMemberNode.getExpression())
				.getArgList().get(1).accept(this);

		MemberNode memberNode1 = new MemberNode(null, member1);
		MemberNode memberNode2 = new MemberNode(null, member2);

		List<ParseTreeNode> args = new ArrayList<ParseTreeNode>();
		args.add(memberNode1);
		args.add(memberNode2);

		CallNode callNode = new CallNode(null, operatorName, Syntax.Internal,
				args);

		// We need to set a level, we take the same level as the first member
		// We need to set ordinal: Ordinal should be Level.Members.size +
		// withMembers.size
		// TODO: LEVEL_CARDINALITY is not solved, yet.
		try {
			Member calculatedMeasure = new Olap4ldMeasure(
					(Olap4ldLevel) member1.getLevel(), calculatedMemberName,
					calculatedMemberName, calculatedMemberName, "", null,
					Aggregator.CALCULATED, callNode, Datatype.INTEGER, true,
					member1.getLevel().getMembers().size() + withMembers.size());
			
			withMembers.add(calculatedMeasure);
			
			return null;
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			throw new UnsupportedOperationException(
					"Probably no members of the level were found.");
		}
	}

	@Override
	public Object visit(WithSetNode calcSetNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(CallNode call) {
		/*
		 * Crossjoin returns the results in the order of the first set
		 * expression, then within that, by the second second expression.
		 */
		if (call.getOperatorName().toLowerCase().equals("crossjoin")) {
			return callCrossJoin(call);
		}

		if (call.getOperatorName().toLowerCase().equals("members")) {
			return callMembers(call);
		}

		/*
		 * The sorting functions (Order, TopCount, BottomCount, TopPercent,
		 * Hierarchize, etc.) use a stable sorting algorithm. Metadata methods
		 * such as <Hierarchy>.Members return their results in natural order.
		 */
		if (call.getOperatorName().toLowerCase().equals("hierarchize")) {
			return callHierarchize(call);
		}

		if (call.getOperatorName().equals("{}")) {
			return callBraces(call);
		}

		if (call.getOperatorName().toLowerCase().equals("filter")) {
			return callFilter(call);
		}

		if (call.getOperatorName().toLowerCase().equals("cast")) {
			return callCast(call);
		}

		if (call.getOperatorName().toLowerCase().equals("currentmember")) {
			return callCurrentMember(call);
		}

		if (call.getOperatorName().toLowerCase().equals("name")) {
			return callName(call);
		}

		if (call.getOperatorName().toLowerCase().equals("<")
				|| call.getOperatorName().toLowerCase().equals("<=")
				|| call.getOperatorName().toLowerCase().equals(">")
				|| call.getOperatorName().toLowerCase().equals(">=")) {
			return callNumericComparator(call);
		}

		if (call.getOperatorName().toLowerCase().equals("+")
				|| call.getOperatorName().toLowerCase().equals("-")
				|| call.getOperatorName().toLowerCase().equals("*")
				|| call.getOperatorName().toLowerCase().equals("/")) {
			return callNumBinOp(call);
		}

		if (call.getOperatorName().toLowerCase().equals("and")) {
			return callBooleanAND(call);
		}

		if (call.getOperatorName().toLowerCase().equals(":")) {
			return callColon(call);
		}

		throw new UnsupportedOperationException("The "
				+ call.getOperatorName().toLowerCase()
				+ " method is not supported by olap4ld, yet.");
	}

	private Object callColon(CallNode call) {
		// Here, we have two Identifier nodes, each having their own name, but
		// we need to create a new i-node
		return null;
	}

	private Object callCurrentMember(CallNode call) {
		return current;
	}

	private Object callBooleanAND(CallNode call) {
		Boolean one = (Boolean) call.getArgList().get(0).accept(this);
		Boolean two = (Boolean) call.getArgList().get(1).accept(this);

		return (Object) new Boolean(one.booleanValue() && two.booleanValue());

	}

	/**
	 * This operator should seldomly be called since most calculations will be done in the engine and
	 * not when interpreting the MDX.
	 * 
	 * @param call
	 * @return
	 */
	private Object callNumBinOp(CallNode call) {
		
		try {
			// If we cannot cast to double, we return null.
			// Also, we need to decode to uri representation
			Double one = new Double(Olap4ldLinkedDataUtil.convertMDXtoURI(call.getArgList().get(0).accept(this).toString()));
			Double two = new Double(Olap4ldLinkedDataUtil.convertMDXtoURI(call.getArgList().get(1).accept(this).toString()));
			if (call.getOperatorName().equals("+")) {
				return (Object) new Double(one.doubleValue() + two.doubleValue());
			}
			if (call.getOperatorName().equals("-")) {
				return (Object) new Double(one.doubleValue() - two.doubleValue());
			}
			if (call.getOperatorName().equals("*")) {
				return (Object) new Double(one.doubleValue() * two.doubleValue());
			}
			if (call.getOperatorName().equals("/")) {
				return (Object) new Double(one.doubleValue() / two.doubleValue());
			}
		} catch (Exception e) {
			return null;
		}
		
		throw new UnsupportedOperationException(
				"Binary numeric operator unknown.");
	}

	/**
	 * OLAP4J seems to work with java.math.BigDecimal, therefore we need to
	 * assume that we get those.
	 * 
	 * @param call
	 * @return
	 */
	private Object callNumericComparator(CallNode call) {
		Double one = (Double) call.getArgList().get(0).accept(this);
		Double two = (Double) call.getArgList().get(1).accept(this);

		if (call.getOperatorName().equals("<")) {
			return (Object) new Boolean(one.doubleValue() < two.doubleValue());
		}
		if (call.getOperatorName().equals("<=")) {
			return (Object) new Boolean(one.doubleValue() <= two.doubleValue());
		}
		if (call.getOperatorName().equals(">")) {
			return (Object) new Boolean(one.doubleValue() > two.doubleValue());
		}
		if (call.getOperatorName().equals(">=")) {
			return (Object) new Boolean(one.doubleValue() >= two.doubleValue());
		}
		throw new UnsupportedOperationException("Numeric comparator unknown.");
	}

	private Object callName(CallNode call) {
		// Only one element
		Object arg = call.getArgList().get(0).accept(this);
		if (arg instanceof Member) {
			Member member = (Member) arg;
			return (Object) member.getName();
		}
		throw new UnsupportedOperationException(
				"So far Name only for members supported!");
	}

	/**
	 * Cast operator
	 * 
	 * The Cast operator converts scalar expressions to other types. The syntax
	 * is
	 * 
	 * Cast(<Expression> AS <Type>)
	 * 
	 * where <Type> is one of:
	 * 
	 * BOOLEAN NUMERIC DECIMAL STRING
	 * 
	 * @param call
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Object callCast(CallNode call) {
		// TODO: How is cast args given?
		// The second parameter gives us the cast
		String caster = (String) call.getArgList().get(1).accept(this);
		String value = (String) call.getArgList().get(0).accept(this);
		// We have to translate MDX first to real value since for the square brackets
		value = Olap4ldLinkedDataUtil.convertMDXtoURI(value);
		if (caster.equals("NUMERIC")) {
			return (Object) (new Double(value));
		}
		throw new UnsupportedOperationException(
				"So far Name only for NUMERIC casts supported!");
	}

	@SuppressWarnings("unchecked")
	private Object callCrossJoin(CallNode call) {
		// Will be two lists
		List<Object> list = (List<Object>) call.getArgList().get(0)
				.accept(this);
		List<List<Object>> result = new ArrayList<List<Object>>();
		List<Object> list2 = (List<Object>) call.getArgList().get(1)
				.accept(this);
		// Would return one list of lists
		for (Object object : list) {
			for (Object object1 : list2) {
				List<Object> newList = new ArrayList<Object>();
				// TODO: Possibly, object and object1 are tuples of members.
				newList.add(object);
				newList.add(object1);
				result.add(newList);
			}
		}
		return (Object) result;
	}

	/**
	 * The filter function will have as first argument a list of elements. As
	 * second argument, it will have a logical expression that needs to be valid
	 * in order to have an element included.
	 * 
	 * The filter function returns a new list of elements that have been
	 * filtered.
	 * 
	 * @param call
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Object callFilter(CallNode call) {
		List<ParseTreeNode> args = call.getArgList();
		List<Object> listToFilter = (List<Object>) args.get(0).accept(this);

		List<Object> elementList = new ArrayList<Object>();
		for (Object object : listToFilter) {

			// For now, always members
			current = (Object) object;
			/*
			 * Now, that I know which element to filter, I can go through the
			 * second part and replace currentmember with this member
			 */

			Object object1 = args.get(1).accept(this);
			// This object should be a Boolean value
			Boolean bool = (Boolean) object1;
			if (bool) {
				elementList.add(object);
			}

			// listToFilter.remove(object);
		}
		return (Object) elementList;
	}

	@SuppressWarnings("unchecked")
	private Object callChildren(CallNode call) {

		/*
		 * Children can only be for a member
		 */
		Object object = call.getArgList().get(0).accept(this);

		try {
			if (object instanceof Member) {
				Member member = (Member) object;
				NamedList<? extends Member> children = member.getChildMembers();
				return (Object) children;
			} else {
				throw new UnsupportedOperationException(
						"Children can only be executed on members!");
			}

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Object callMembers(CallNode call) {

		/*
		 * Members can be for a dimension, hierarchy, level
		 */
		Object object = call.getArgList().get(0).accept(this);

		List<Member> allMembers = new ArrayList<Member>();
		try {
			if (object instanceof Dimension) {
				Dimension dimension = (Dimension) object;
				// Would return a list of Members
				NamedList<Hierarchy> hierarchies = dimension.getHierarchies();
				for (int i = 0; i < hierarchies.size(); i++) {
					Hierarchy hierarchy = hierarchies.get(i);
					NamedList<Level> levels = hierarchy.getLevels();
					for (int j = 0; j < levels.size(); j++) {
						Level level = levels.get(j);
						List<Member> members = level.getMembers();
						for (Member member : members) {
							allMembers.add(member);
						}
					}
				}
				return (Object) allMembers;
			}
			if (object instanceof Hierarchy) {
				Hierarchy hierarchy = (Hierarchy) object;
				NamedList<Level> levels = hierarchy.getLevels();
				for (int j = 0; j < levels.size(); j++) {
					Level level = levels.get(j);
					List<Member> members = level.getMembers();
					for (Member member : members) {
						allMembers.add(member);
					}
				}
				return (Object) allMembers;
			}
			if (object instanceof Level) {
				Level level = (Level) object;
				// Would return a list of Members
				return (Object) level.getMembers();
			}

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Object callHierarchize(CallNode call) {

		List<Object> list = (List<Object>) call.getArgList().get(0)
				.accept(this);
		for (Object object : list) {
			if (object instanceof List) {
				// First order for the first, then the second...
				// TODO
			}
			// First order
			// TODO
		}
		// Would return a sorted list
		return (Object) list;
	}

	@SuppressWarnings("unchecked")
	private Object callBraces(CallNode call) {
		List<Object> result = new ArrayList<Object>();
		// Single objects
		for (ParseTreeNode parseTreeObj : call.getArgList()) {
			Object object = parseTreeObj.accept(this);
			// A list should not be put in another list.
			/*
			 * TODO: Is this correct? What if I have List and Member?
			 */
			if (object instanceof List) {
				List<Object> list = (List<Object>) object;
				for (Object object1 : list) {
					result.add(object1);
				}
			} else {
				result.add(object);
			}
		}

		// Now, I want to return a list of the arguments
		return (Object) result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object visit(IdentifierNode id) {
		//
		List<IdentifierSegment> segmentList = id.getSegmentList();

		// If we have withMembers, then look there, first.
		if (!withMembers.isEmpty()) {
			String[] identifierArray = new String[segmentList.size()];

			for (int i = 0; i < segmentList.size(); i++) {
				identifierArray[i] = segmentList.get(i).toString();
			}

			String calculatedMemberName = Olap4ldLinkedDataUtil.implodeArray(
					identifierArray, ".");

			for (Member member : withMembers) {
				if (member.getUniqueName().equals(calculatedMemberName)) {
					return (Object) member;
				}
			}
		}

		// Assume that only one segment
		/*
		 * TODO: .members is then seen as part of the name which is not ideal.
		 */
		int segmentIndex = 0;
		// .name() removes the square brackets if contained, therefore, we use toString()
		String segmentName = segmentList.get(segmentIndex).toString();
		segmentIndex++;
		// Now, we check whether name is available
		try {
			NamedList<Cube> cubes = this.olap4jStatement.olap4jConnection
					.getOlapSchema().getCubes();
			Cube cube = cubes.get(segmentName);
			// Watch out cubes.contains has as parameter a cube!
			if (cube != null) {
				return (Object) cube;
			} else {
				boolean first = true;
				for (Cube cube1 : cubes) {

					// If cube already given by the query, use it.
					if (this.cube != null && first) {
						cube1 = this.cube;
						first = false;
					} else if (cube != null && !first) {
						throw new UnsupportedOperationException(
								"If a cube is given, we should find the identifier in there!");
					}

					NamedList<Dimension> dimensions = cube1.getDimensions();

					Dimension dimension = dimensions.get(segmentName);

					/*
					 * Two possibilities: Either search for the single
					 * identifier or unique identifier of segments
					 */
					if (dimension == null) {

						// Go through everything
						for (Dimension dimension1 : dimensions) {
							NamedList<Hierarchy> hierarchies = dimension1
									.getHierarchies();

							Hierarchy hierarchy = hierarchies.get(segmentName);
							if (hierarchy != null) {
								return (Object) hierarchy;
							} else {
								for (Hierarchy hierarchy1 : hierarchies) {
									NamedList<Level> levels = hierarchy1
											.getLevels();

									Level level = levels.get(segmentName);
									if (level != null) {
										return (Object) level;
									} else {
										for (Level level1 : levels) {
											List<Member> members = level1
													.getMembers();
											for (Member member : members) {
												if (member.getName().equals(
														segmentName)) {
													return (Object) member;
												}
											}
										}
									}
								}
							}
						}

					} else if (dimension != null
							&& segmentIndex >= segmentList.size()) {
						return (Object) dimension;

					} else if (dimension != null) {
						segmentName = segmentList.get(segmentIndex).toString();

						Hierarchy hierarchy = dimension.getHierarchies().get(
								segmentName);
						segmentIndex++;
						if (hierarchy == null) {
							throw new UnsupportedOperationException(
									"MDX identifier not properly given:"
											+ segmentName);
						} else if (hierarchy != null
								&& segmentIndex >= segmentList.size()) {
							return (Object) hierarchy;
						} else {
							segmentName = segmentList.get(segmentIndex).toString();

							Level level = hierarchy.getLevels()
									.get(segmentName);
							segmentIndex++;
							if (level == null) {
								throw new UnsupportedOperationException(
										"MDX identifier not properly given:"
												+ segmentName);
							} else if (level != null
									&& segmentIndex >= segmentList.size()) {
								return (Object) level;
							} else {
								segmentName = segmentList.get(segmentIndex).toString();

								for (Member member : level.getMembers()) {
									if (member.getName().equals(segmentName)) {
										return (Object) member;
									}
								}
							}
						}
					}
				}
			}

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String identifier = "";
		for (int i = 0; i < segmentList.size(); i++) {
			identifier += segmentList.get(i).toString();
		}
		throw new UnsupportedOperationException(
				"There should be an OLAP element for each identifier, also this one:"
						+ identifier);
	}

	public Cube getCube() {
		return cube;
	}

	public List<Position> getColumnPositions() {
		return columnPositions;
	}

	public List<Position> getRowPositions() {
		return rowPositions;
	}

	public List<Position> getFilterPositions() {
		return filterPositions;
	}

	@Override
	public Object visit(ParameterNode parameterNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	public Object visit(CubeNode cubeNode) {
		return (Object) cubeNode.getCube();
	}

	@Override
	public Object visit(DimensionNode dimensionNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(HierarchyNode hierarchyNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	public Object visit(LevelNode levelNode) {
		// TODO Auto-generated method stub
		return (Object) levelNode.getLevel();
	}

	@SuppressWarnings("unchecked")
	public Object visit(MemberNode memberNode) {
		// TODO Auto-generated method stub
		return (Object) memberNode.getMember();
	}

	/**
	 * Numeric literal value use BigDecimal, in which case we transform it into
	 * double.
	 */
	@Override
	public Object visit(LiteralNode literalNode) {
		Object value = (Object) literalNode.getValue();

		if (value instanceof BigDecimal) {
			return (Object) (new Double(((BigDecimal) value).doubleValue()));
		}
		if (value instanceof String) {
			return (Object) ((String) value);
		}
		throw new UnsupportedOperationException(
				"So far, only BigDecimal and Strings as literalNodes supported.");
	}

	@Override
	public Object visit(PropertyValueNode propertyValueNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(DrillThroughNode drillThroughNode) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Transforms the list of lists of members into a position list.
	 * 
	 * @param list
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<Position> createPositionsList(List<Object> list) {
		// I go through list, and check whether again a list
		List<Position> positions = new ArrayList<Position>();

		// List of "positions"
		for (Object object : list) {
			List<Member> positionMembers = new ArrayList<Member>();
			if (object instanceof List) {
				List<Object> list1 = (List<Object>) object;
				for (Object object2 : list1) {
					// Now, should be member
					if (object2 instanceof Member) {
						positionMembers.add((Member) object2);
					} else {
						throw new UnsupportedOperationException(
								"Inside the list we should only have members.");
					}
				}
			} else if (object instanceof Member) {
				positionMembers.add((Member) object);
			} else {
				throw new UnsupportedOperationException(
						"Inside the list we should only have members.");
			}
			Olap4ldPosition aPosition = new Olap4ldPosition(positionMembers,
					positions.size());
			positions.add(aPosition);
		}
		return positions;
	}
}
