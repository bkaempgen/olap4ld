/**
 * 
 */
package org.olap4j.driver.ld;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.text.Segment;

import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.driver.ld.LdOlap4jConnection.Context;
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
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;

/**
 * @author b-kaempgen
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
@SuppressWarnings("hiding")
public class MdxMethodVisitor<Object> implements ParseTreeVisitor<Object> {

	private LdOlap4jStatement olap4jStatement;

	private Context olap4jContext;
	private Object current;
	private List<Member> withMembers;

	public MdxMethodVisitor(LdOlap4jStatement olap4jStatement) {
		this.olap4jStatement = olap4jStatement;
		olap4jContext = new Context(olap4jStatement.olap4jConnection, null,
				null, null, null, null, null, null);
		current = null;
		withMembers = new ArrayList<Member>();
	}

	@Override
	public Object visit(SelectNode selectNode) {
		// TODO Auto-generated method stub
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
	@SuppressWarnings("unchecked")
	public Object visit(WithMemberNode calcMemberNode) {
		List<IdentifierSegment> identifierList = calcMemberNode.getIdentifier()
				.getSegmentList();

		String[] identifierArray = new String[identifierList.size()];

		for (int i = 0; i < identifierList.size(); i++) {
			identifierArray[i] = identifierList.get(i).getName();
		}

		String calculatedMemberName = LdOlap4jUtil.implodeArray(
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
			Member calculatedMember = new LdOlap4jMeasure(
					(LdOlap4jLevel) member1.getLevel(), calculatedMemberName,
					calculatedMemberName, calculatedMemberName, "", null,
					Measure.Aggregator.CALCULATED, callNode, Datatype.INTEGER,
					true, member1.getLevel().getMembers().size()
							+ withMembers.size());
			// We store the calculatedMember as a possible IdentifierNode
			withMembers.add(calculatedMember);
			return (Object) calculatedMember;
		} catch (OlapException e) {
			// TODO Auto-generated catch block
			throw new UnsupportedOperationException("Probably no members of the level were found.");
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

		throw new UnsupportedOperationException(
				"This method is not supported by olap4ld, yet.");
	}

	private Object callCurrentMember(CallNode call) {
		return current;
	}

	private Object callBooleanAND(CallNode call) {
		Boolean one = (Boolean) call.getArgList().get(0).accept(this);
		Boolean two = (Boolean) call.getArgList().get(1).accept(this);

		return (Object) new Boolean(one.booleanValue() && two.booleanValue());

	}

	private Object callNumBinOp(CallNode call) {
		Double one = (Double) call.getArgList().get(0).accept(this);
		Double two = (Double) call.getArgList().get(1).accept(this);

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
		if (caster.equals("NUMERIC")) {
			return (Object) (new Double(value));
		}
		throw new UnsupportedOperationException(
				"So far Name only for NUMERIC casts supported!");
	}

	@SuppressWarnings("unchecked")
	private Object callCrossJoin(CallNode call) {
		// Will be two lists
		MdxMethodVisitor<Object> newvisitor = new MdxMethodVisitor<Object>(
				this.olap4jStatement);
		List<Object> list = (List<Object>) call.getArgList().get(0)
				.accept(newvisitor);
		List<List<Object>> result = new ArrayList<List<Object>>();
		MdxMethodVisitor<Object> newvisitor1 = new MdxMethodVisitor<Object>(
				this.olap4jStatement);
		List<Object> list2 = (List<Object>) call.getArgList().get(1)
				.accept(newvisitor1);
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
			 * Now, that I now which element to filter, I can go through the
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

		MdxMethodVisitor<Object> newvisitor = new MdxMethodVisitor<Object>(
				this.olap4jStatement);

		/*
		 * Children can only be for a member 
		 */
		Object object = call.getArgList().get(0).accept(newvisitor);

		try {
			if (object instanceof Member) {
				Member member = (Member) object;
				NamedList<? extends Member> children = member.getChildMembers();
				return (Object) children;
			} else {
				throw new UnsupportedOperationException("Children can only be executed on members!");
			}

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Object callMembers(CallNode call) {

		MdxMethodVisitor<Object> newvisitor = new MdxMethodVisitor<Object>(
				this.olap4jStatement);

		/*
		 * Members can be for a dimension, hierarchy, level
		 */
		Object object = call.getArgList().get(0).accept(newvisitor);

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
						Level level = levels.get(i);
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
		// Can only be a list
		MdxMethodVisitor<Object> newvisitor = new MdxMethodVisitor<Object>(
				this.olap4jStatement);
		List<Object> list = (List<Object>) call.getArgList().get(0)
				.accept(newvisitor);
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
				identifierArray[i] = segmentList.get(i).getName();
			}

			String calculatedMemberName = LdOlap4jUtil.implodeArray(
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
		String segmentName = segmentList.get(segmentIndex).getName();
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
				for (Cube cube1 : cubes) {
					NamedList<Dimension> dimensions = cube1.getDimensions();

					Dimension dimension = dimensions.get(segmentName);

					/*
					 * Two possibilities: Either search for the single
					 * identifier or unique identifier of segments
					 */
					if (dimension == null) {

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

						Hierarchy hierarchy = dimension.getHierarchies().get(
								segmentList.get(segmentIndex).getName());
						segmentIndex++;
						if (hierarchy == null) {
							throw new UnsupportedOperationException(
									"MDX identifier not properly given:"+segmentList.get(segmentIndex).getName());
						} else if (hierarchy != null
								&& segmentIndex >= segmentList.size()) {
							return (Object) hierarchy;
						} else {
							Level level = hierarchy.getLevels().get(
									segmentList.get(segmentIndex).getName());
							segmentIndex++;
							if (level == null) {
								throw new UnsupportedOperationException(
										"MDX identifier not properly given:"+segmentList.get(segmentIndex).getName());
							} else if (level != null
									&& segmentIndex >= segmentList.size()) {
								return (Object) level;
							} else {
								segmentName = segmentList.get(segmentIndex)
										.getName();

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
			identifier += segmentList.get(i).getName();
		}
		throw new UnsupportedOperationException(
				"There should be an OLAP element for each identifier, also this one:"+identifier);
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
	public List<Position> createPositionsList(List<Object> list) {
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
			LdOlap4jPosition aPosition = new LdOlap4jPosition(positionMembers,
					positions.size());
			positions.add(aPosition);
		}
		return positions;
	}

	/**
	 * TODO: From the parsed list of lists of members, it should be possible to
	 * create the hierarchy list. Note, we do also include the measures
	 * hierarchy, here, since the hierarchies give an impression of the look of
	 * the pivot table.
	 * 
	 * @param list
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Hierarchy> createHierarchyList(List<Object> list) {
		/*
		 * The list of hierarchies used in an axis, the dimensionality in terms
		 * of hierarchies
		 */
		List<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();

		// We have a list of tuples (lists)

		// List of "positions"
		Object object = list.get(0);
		if (object instanceof List) {
			List<Object> list1 = (List<Object>) object;
			for (Object object2 : list1) {
				// Now, should be member
				if (object2 instanceof Member) {
					if (object2 instanceof Measure) {
						// Measures are used, also.
						hierarchyList.add(((Member) object2).getHierarchy());
					} else {
						hierarchyList.add(((Member) object2).getHierarchy());
					}
				} else {
					throw new UnsupportedOperationException(
							"Inside the list we should only have members.");
				}
			}
		} else if (object instanceof Member) {
			if (object instanceof Measure) {
				hierarchyList.add(((Member) object).getHierarchy());
			} else {
				hierarchyList.add(((Member) object).getHierarchy());
			}
		} else {
			throw new UnsupportedOperationException(
					"Inside the list we should only have members.");
		}
		return hierarchyList;
	}

	/**
	 * TODO: From the parsed list of lists of members, it should be possible to
	 * create a level list. Note, we do also include the measures
	 * level, here this is done for hierarchies also.
	 * 
	 * @param list
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Level> createLevelList(List<Object> list) {
		/*
		 * The list of hierarchies used in an axis, the dimensionality in terms
		 * of hierarchies
		 */
		List<Level> levelList = new ArrayList<Level>();

		// We have a list of tuples (lists)

		// List of "positions"
		Object object = list.get(0);
		if (object instanceof List) {
			List<Object> list1 = (List<Object>) object;
			for (Object object2 : list1) {
				// Now, should be member
				if (object2 instanceof Member) {
					if (object2 instanceof Measure) {
						// Measures are used, also.
						levelList.add(((Member) object2).getLevel());
					} else {
						levelList.add(((Member) object2).getLevel());
					}
				} else {
					throw new UnsupportedOperationException(
							"Inside the list we should only have members.");
				}
			}
		} else if (object instanceof Member) {
			if (object instanceof Measure) {
				levelList.add(((Member) object).getLevel());
			} else {
				levelList.add(((Member) object).getLevel());
			}
		} else {
			throw new UnsupportedOperationException(
					"Inside the list we should only have members.");
		}
		return levelList;
	}
	
	/**
	 * From the parsed list of lists of members, it should be possible to create
	 * the measures list.
	 * 
	 * @param list
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Measure> createMeasureList(List<Object> list) {
		/*
		 * The list of measures used in an axis
		 */
		List<Measure> measureList = new ArrayList<Measure>();

		// We have a list of tuples (lists)
		for (Object object : list) {
			if (object instanceof List) {
				List<Object> list1 = (List<Object>) object;
				for (Object object2 : list1) {
					// Now, should be member
					if (object2 instanceof Member) {
						Member member = (Member) object2;
						if (member.getMemberType() == Member.Type.MEASURE
								|| member.getMemberType() == Member.Type.FORMULA) {
							measureList.add((Measure) member);
						}
					} else {
						throw new UnsupportedOperationException(
								"Inside the list we should only have members.");
					}
				}
			} else if (object instanceof Member) {
				Member member = (Member) object;
				if (member.getMemberType() == Member.Type.MEASURE
						|| member.getMemberType() == Member.Type.FORMULA) {
					measureList.add((Measure) member);
				}
			} else {
				throw new UnsupportedOperationException(
						"Inside the list we should only have members.");
			}
		}
		return measureList;
	}

}
