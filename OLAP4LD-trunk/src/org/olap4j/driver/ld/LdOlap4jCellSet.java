/*
//
// Licensed to Benedikt Kämpgen under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Benedikt Kämpgen licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package org.olap4j.driver.ld;

import org.olap4j.*;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.CubeNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.WithMemberNode;
import org.olap4j.driver.ld.Factory;
import org.olap4j.driver.ld.MetadataReader;
import org.olap4j.driver.ld.LdHelper;
import org.olap4j.driver.ld.LdOlap4jCell;
import org.olap4j.driver.ld.LdOlap4jCellProperty;
import org.olap4j.driver.ld.LdOlap4jCellSet;
import org.olap4j.driver.ld.LdOlap4jCellSetAxis;
import org.olap4j.driver.ld.LdOlap4jCellSetAxisMetaData;
import org.olap4j.driver.ld.LdOlap4jCellSetMemberProperty;
import org.olap4j.driver.ld.LdOlap4jCellSetMetaData;
import org.olap4j.driver.ld.LdOlap4jConnection;
import org.olap4j.driver.ld.XmlaOlap4jMemberBase;
import org.olap4j.driver.ld.LdOlap4jPosition;
import org.olap4j.driver.ld.XmlaOlap4jPositionMember;
import org.olap4j.driver.ld.XmlaOlap4jPreparedStatement;
import org.olap4j.driver.ld.LdOlap4jStatement;
import org.olap4j.driver.ld.LdOlap4jUtil;
import org.olap4j.impl.Olap4jUtil;

import static org.olap4j.driver.ld.LdOlap4jUtil.*;

import org.olap4j.metadata.*;
import org.semanticweb.yars.nx.Node;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.CellSet} for XML/A providers.
 * 
 * <p>
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs; it is
 * instantiated using {@link Factory#newCellSet}.
 * </p>
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jCellSet.java 455 2011-05-24 10:01:26Z jhyde $
 * @since May 24, 2007
 */
abstract class LdOlap4jCellSet implements CellSet {
	final LdOlap4jStatement olap4jStatement;
	protected boolean closed;
	private LdOlap4jCellSetMetaData metaData;
	private final Map<Integer, Cell> cellMap = new HashMap<Integer, Cell>();
	/**
	 * The value map that the OLAP query populates and from which the cells are
	 * queried. It stores string representations of cell values of indexed
	 * measures in string arrays, keyed-off by a hashcode of the concatenation
	 * of the uniquely identifying list of members.
	 */
	private final Map<Integer, String[]> newValueMap = new HashMap<Integer, String[]>();
	private final List<LdOlap4jCellSetAxis> axisList = new ArrayList<LdOlap4jCellSetAxis>();
	private final List<CellSetAxis> immutableAxisList = Olap4jUtil
			.cast(Collections.unmodifiableList(axisList));
	private LdOlap4jCellSetAxis filterAxis;
	// TODO Remove
	private Map<Integer, Integer> hierarchyCellMap;
	/*
	 * OLAP query datastructure:
	 * 
	 * groupbylist - set of inquired dimensions measurelist - set of measures
	 * selectionpredicates - set of set of members of dimensions to restrict
	 */
	private ArrayList<Level> groupbylist;
	private ArrayList<Measure> measurelist;
	private ArrayList<List<Member>> selectionpredicates;
	// TODO: debugging is done?
	private static final boolean DEBUG = true;

	private static final List<String> standardProperties = Arrays.asList(
			"UName", "Caption", "LName", "LNum", "DisplayInfo");

	/**
	 * Creates an XmlaOlap4jCellSet.
	 * 
	 * @param olap4jStatement
	 *            Statement
	 */
	LdOlap4jCellSet(LdOlap4jStatement olap4jStatement) {
		assert olap4jStatement != null;
		this.olap4jStatement = olap4jStatement;
		this.closed = false;
	}

	/**
	 * Returns the error-handler.
	 * 
	 * @return Error handler
	 */
	private LdHelper getHelper() {
		return olap4jStatement.olap4jConnection.helper;
	}

	/**
	 * MDX evaluates the axis and slicer dimensions first, building the
	 * structure of the result cube before retrieving the information from the
	 * cube to be queried.
	 * 
	 * @param selectNode
	 * @throws OlapException
	 */
	@SuppressWarnings("unchecked")
	void createMetaDataFromSelectNode(SelectNode selectNode)
			throws OlapException {

		// Create visitor that we will use throughout.
		MdxMethodVisitor<Object> visitor = new MdxMethodVisitor<Object>(
				olap4jStatement);

		// Here, I need to accept more
		// selectNode.accept(ldVisitor);

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
				Object calculatedMeasure = visitor.visit((WithMemberNode) with);
			}
		}

		// From
		// MdxMethodVisitor<Object> cubeldMethodVisitor = new
		// MdxMethodVisitor<Object>(
		// olap4jStatement);
		// I know, that getFrom gives me a cubeNode, therefore, I know the cube
		Cube cube = (Cube) selectNode.getFrom().accept(visitor);

		// REVIEW: We should not modify the connection. It is not safe, because
		// connection might be shared between multiple statements with different
		// cubes. Caller should call
		//
		// connection.setCatalog(
		// cellSet.getMetaData().getCube().getSchema().getCatalog().getName())
		//
		// before doing metadata queries.
		try {
			// Our cubes have exactly one schema and one catalog.
			this.olap4jStatement.olap4jConnection.setCatalog(cube.getSchema()
					.getCatalog().getName());
		} catch (SQLException e) {
			throw getHelper().createException(
					"Internal error: setting catalog '"
							+ cube.getSchema().getCatalog().getName()
							+ "' caused error");
		}

		// Filter (create MetaData for the filter axis)
		AxisNode filter = selectNode.getFilterAxis();

		List<Position> filterPositions = new ArrayList<Position>();
		List<Hierarchy> filterHierarchies = new ArrayList<Hierarchy>();

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
			Object result = filter.accept(visitor);
			List<Object> listResult = (List<Object>) result;

			/*
			 * From the method visitor, we get a list of (possible list of)
			 * members
			 */
			filterPositions = visitor.createPositionsList(listResult);
			filterHierarchies = visitor.createHierarchyList(listResult);

			// pw.println();
			// pw.print("WHERE ");
			// filterAxis.unparse(writer);
		}

		// I need to create a filter axis metadata
		final LdOlap4jCellSetAxisMetaData filterAxisMetaData = new LdOlap4jCellSetAxisMetaData(
				olap4jStatement.olap4jConnection, filter.getAxis(),
				Collections.<Hierarchy> unmodifiableList(filterHierarchies),
				Collections.<LdOlap4jCellSetMemberProperty> emptyList());

		// I need to create a filter axis
		final LdOlap4jCellSetAxis filterCellSetAxis = new LdOlap4jCellSetAxis(
				this, filter.getAxis(),
				Collections.unmodifiableList(filterPositions));
		filterAxis = filterCellSetAxis;

		// Axis (create MetaData for one specific axis)
		final List<LdOlap4jCellSetAxisMetaData> axisMetaDataList = new ArrayList<LdOlap4jCellSetAxisMetaData>();

		// When going through the axes: Prepare groupbylist, a set of levels
		// Prepare list of levels (excluding Measures!)
		// Go through tuple get dimensions, if not measure
		this.groupbylist = new ArrayList<Level>();

		for (AxisNode axis : selectNode.getAxisList()) {

			// I need to add the list of hierarchies
			/*
			 * Consider
			 * "select Time.members * Customer.members on 0 from Sales". The
			 * columns axis will have a list of tuples, and each tuple will have
			 * a member of the Time hierarchy and a member of the Customer
			 * hierarchy. Therefore getHierarchies should return a list { <time
			 * hierarchy>, <customers hierarchy> }.
			 * 
			 * If you don't know which hierarchies, I suppose you could return a
			 * list of nulls. If you don't even know the arity of the axis, you
			 * could return null rather than a list. The spec doesn't say
			 * whether either of these are allowed. Arguably it should, but
			 * anyway.
			 * 
			 * The list of hierarchies for the axes (also filter axis) can be
			 * used as a signature to determine, which dimensions are covered by
			 * the axes. All other dimensions are assumed to be slicer
			 * dimensions and filter with their default members (in a query).
			 */

			// I should create a visitor that goes through the AxisNode (axis)
			// and returns with positions
			// MdxMethodVisitor<Object> ldMethodVisitor = new
			// MdxMethodVisitor<Object>(
			// olap4jStatement);
			Object result = axis.accept(visitor);
			List<Object> listResult = (List<Object>) result;

			/*
			 * From the method visitor, we get a list of (possible list of)
			 * members
			 */
			List<Position> positions = visitor.createPositionsList(listResult);
			List<Hierarchy> hierarchies = visitor
					.createHierarchyList(listResult);
			List<Level> levels = visitor.createLevelList(listResult);
			for (Level level : levels) {
				// Since any dimension can only be mentioned once per axis,
				// there should not be inserted a level twice.
				if (!level.getUniqueName().equals("Measures")) {
					groupbylist.add(level);
				}
			}

			// TODO: Properties are not computed, yet.
			List<LdOlap4jCellSetMemberProperty> propertyList = new ArrayList<LdOlap4jCellSetMemberProperty>();
			List<LdOlap4jCellSetMemberProperty> properties = propertyList;

			// Add cellSetAxis to list of axes
			final LdOlap4jCellSetAxis cellSetAxis = new LdOlap4jCellSetAxis(
					this, axis.getAxis(),
					Collections.unmodifiableList(positions));
			axisList.add(cellSetAxis);

			// Add axisMetaData to list of axes metadata
			final LdOlap4jCellSetAxisMetaData axisMetaData = new LdOlap4jCellSetAxisMetaData(
					olap4jStatement.olap4jConnection, axis.getAxis(),
					hierarchies, properties);

			axisMetaDataList.add(axisMetaData);
		}

		List<LdOlap4jCellProperty> cellProperties = new ArrayList<LdOlap4jCellProperty>();
		// I do not support cell properties, yet.
		if (!selectNode.getCellPropertyList().isEmpty()) {
			// pw.println();
			// pw.print("CELL PROPERTIES ");
			// k = 0;
			// for (IdentifierNode cellProperty : cellPropertyList) {
			// if (k++ > 0) {
			// pw.print(", ");
			// }
			// cellProperty.unparse(writer);
			// }
		}

		this.metaData = new LdOlap4jCellSetMetaData(olap4jStatement,
				(LdOlap4jCube) cube, filterAxisMetaData, axisMetaDataList,
				cellProperties);

		/*
		 * It should be possible to execute the query and to built up a hash map
		 * for the results, which can be used in the cells.
		 */

		/*
		 * I take the first Tuple of Members, get their Dimensions (apart from
		 * the Measure) and use those to query for a bunch of values hoping that
		 * I will calculate many needed. Then, I put the values in a hash map.
		 * For the next tuple, I check whether they are already contained in the
		 * hash map; if not, I again calculate a bunch of values and store them
		 * in the hash map.
		 */

		/*
		 * In the following, we create selection predicates: For specific
		 * dimensions, lists of members that the cube shall be diced with.
		 * 
		 * The reason that we had so many more filters is, that we create a
		 * CrossJoin of filtered member sets.
		 * 
		 * For now, we check first, whether a certain member already is
		 * contained.
		 */

		// Prepare selectionpredicates, a list of list of members of dimensions
		// (including measure dimension, since we filter it out later).
		selectionpredicates = new ArrayList<List<Member>>();

		List<HashMap<Integer, Member>> selectionhashmaps = new ArrayList<HashMap<Integer, Member>>();

		// No dimension is to be mentioned in several axes, therefore, we can do
		// the following
		int numberOfDimensions = 0;
		for (LdOlap4jCellSetAxisMetaData axisMetaData : axisMetaDataList) {
			numberOfDimensions += axisMetaData.getHierarchies().size();
		}
		numberOfDimensions += filterHierarchies.size();

		// Create list for each dimension to be restricted
		for (int i = 0; i < numberOfDimensions; i++) {
			selectionpredicates.add(new ArrayList<Member>());
			selectionhashmaps.add(new HashMap<Integer, Member>());
		}

		// Go through all positions of filter
		for (Position list : filterCellSetAxis.positions) {
			// For each position, we get a new member for each restriction set
			List<Member> members = list.getMembers();
			for (int i = 0; i < filterHierarchies.size(); i++) {
				// Check first whether measure
				Member member = members.get(i);

				// Check whether member already contained.
				if (!selectionhashmaps.get(i).containsKey(
						member.getUniqueName().hashCode())) {
					selectionhashmaps.get(i).put(
							members.get(i).getUniqueName().hashCode(),
							members.get(i));
					selectionpredicates.get(i).add(members.get(i));
				}
			}
		}

		// For each axis, add members to the appropriate member list of the
		// dimension.
		// TODO: Possibly, here we have room for improvements since a new graph
		// pattern will be created for filtering although this dimension will
		// have been selected already anyway.
		int axisIndex = 0;
		for (LdOlap4jCellSetAxis axis : axisList) {
			List<Position> positions = axis.positions;
			for (Position position : positions) {
				List<Member> members = position.getMembers();
				for (int i = 0; i < axis.getAxisMetaData().getHierarchies()
						.size(); i++) {

					if (!selectionhashmaps.get(
							i + axisIndex + filterHierarchies.size())
							.containsKey(
									members.get(i).getUniqueName().hashCode())) {
						selectionhashmaps.get(
								i + axisIndex + filterHierarchies.size()).put(
								members.get(i).getUniqueName().hashCode(),
								members.get(i));
						selectionpredicates.get(
								i + axisIndex + filterHierarchies.size()).add(
								members.get(i));
					}
				}
			}
			axisIndex += axis.getAxisMetaData().getHierarchies().size();
		}

		// Prepare measurelist, a set of measures
		this.measurelist = new ArrayList<Measure>();

		HashMap<Integer, Measure> measurehashmap = new HashMap<Integer, Measure>();

		for (LdOlap4jCellSetAxis axis : axisList) {
			List<Position> positions = axis.positions;
			for (Position position : positions) {
				List<Member> members = position.getMembers();
				for (int i = 0; i < members.size(); i++) {
					// Check whether measure already contained.
					if ((members.get(i).getMemberType() == Member.Type.MEASURE || members
							.get(i).getMemberType() == Member.Type.FORMULA)
							&& !measurehashmap.containsKey(members.get(i)
									.getUniqueName().hashCode())) {
						measurehashmap.put(members.get(i).getUniqueName()
								.hashCode(), (Measure) members.get(i));
						measurelist.add((Measure) members.get(i));
					}
				}
			}
		}

		// Go through all positions
		for (Position list : filterCellSetAxis.positions) {
			// For each position, we get a new member for each restriction set
			List<Member> members = list.getMembers();
			for (int i = 0; i < members.size(); i++) {
				// Check whether measure already contained.
				if ((members.get(i).getMemberType() == Member.Type.MEASURE || members
						.get(i).getMemberType() == Member.Type.FORMULA)
						&& !measurehashmap.containsKey(members.get(i)
								.getUniqueName().hashCode())) {
					measurehashmap.put(members.get(i).getUniqueName()
							.hashCode(), (Measure) members.get(i));
					measurelist.add((Measure) members.get(i));
				}
			}
		}

		// If no measure, then find default
		if (measurelist.isEmpty()) {
			// Default is simply the first we find.
			LdOlap4jUtil._log
					.info("Get default (first available) measure in cube.");
			if (!cube.getMeasures().isEmpty()) {
				measurelist.add(cube.getMeasures().get(0));
			} else {
				throw new UnsupportedOperationException(
						"There should always be at least one measure in the cube.");
			}
		}

		/*
		 * If groupbylist or measurelist is empty, we do not need to proceed.
		 * 
		 */
		if (groupbylist.isEmpty() || measurelist.isEmpty()) {
			return;
		}
		
		List<Node[]> olapQueryResult = this.olap4jStatement.olap4jConnection.myLinkedData
				.getOlapResult(groupbylist, measurelist, selectionpredicates,
						cube);

		// Now, insert into hash map

		/*
		 * We cannot directly fill the cellMap, since there is no direct
		 * correspondence between the sparql result and the "pivot" table.
		 * 
		 * Every sparql result gives the measures for a specific cell.
		 * 
		 * For a sparse result set, it would be best to find out the ordinal
		 * value for each returned value. How can we find the ordinal value? By
		 * finding the coordinates. For each axis there is a coordinate. Can we
		 * find the coordinate by looking at the column values?
		 */

		boolean first = true;
		String concatNr;
		for (Node[] node : olapQueryResult) {
			concatNr = "";
			if (first) {
				// The first is not interesting, the variable names are not
				// expressive
				first = false;
				continue;
			}

			/*
			 * Each value in a node corresponds to a dimension in the dimension
			 * list, but this dimension list does not correspond to the
			 * positions.
			 * 
			 * We now create a hash map which consist of Numeric representation
			 * of concatenation of all dimension members An array with the
			 * values for each measure, ordered.
			 */

			for (int i = 0; i < groupbylist.size(); i++) {
				concatNr += node[i].toString();
			}

			String[] valueArray = new String[measurelist.size()];

			for (int e = groupbylist.size(); e < groupbylist.size()
					+ measurelist.size(); e++) {
				valueArray[e - groupbylist.size()] = node[e].toString();
			}

			newValueMap.put(concatNr.hashCode(), valueArray);

			// final Object value = getTypedValue(cell);
			// final String formattedValue = stringElement(cell, "FmtValue");
			// final String formatString = stringElement(cell, "FormatString");
			// Olap4jUtil.discard(formatString);

			// for (Element element : childElements(cell)) {
			// String tag = element.getLocalName();
			// final Property property = metaData.propertiesByTag.get(tag);
			// if (property != null) {
			// propertyValues.put(property, element.getTextContent());
			// }
			// }

		}
	}

	/**
	 * Gets response from the XMLA request and populates cell set axes and cells
	 * with it.
	 * 
	 * @throws OlapException
	 *             on error
	 */
	@Deprecated
	void populate() throws OlapException {
		byte[] bytes = olap4jStatement.getBytes();

		Document doc;
		try {
			doc = parse(bytes);
		} catch (IOException e) {
			throw getHelper().createException("error creating CellSet", e);
		} catch (SAXException e) {
			throw getHelper().createException("error creating CellSet", e);
		}
		// <SOAP-ENV:Envelope>
		// <SOAP-ENV:Header/>
		// <SOAP-ENV:Body>
		// <xmla:ExecuteResponse>
		// <xmla:return>
		// <root>
		// (see below)
		// </root>
		// <xmla:return>
		// </xmla:ExecuteResponse>
		// </SOAP-ENV:Body>
		// </SOAP-ENV:Envelope>
		final Element envelope = doc.getDocumentElement();
		if (DEBUG) {
			System.out.println(LdOlap4jUtil.toString(doc, true));
		}
		assert envelope.getLocalName().equals("Envelope");
		assert envelope.getNamespaceURI().equals(SOAP_NS);
		Element body = findChild(envelope, SOAP_NS, "Body");
		Element fault = findChild(body, SOAP_NS, "Fault");
		if (fault != null) {
			/*
			 * <SOAP-ENV:Fault> <faultcode>SOAP-ENV:Client.00HSBC01</faultcode>
			 * <faultstring>XMLA connection datasource not found</faultstring>
			 * <faultactor>Mondrian</faultactor> <detail> <XA:error
			 * xmlns:XA="http://mondrian.sourceforge.net"> <code>00HSBC01</code>
			 * <desc>The Mondrian XML: Mondrian Error:Internal error: no catalog
			 * named 'LOCALDB'</desc> </XA:error> </detail> </SOAP-ENV:Fault>
			 */
			// TODO: log doc to logfile
			throw getHelper().createException(
					"XMLA provider gave exception: "
							+ LdOlap4jUtil.prettyPrint(fault));
		}
		Element executeResponse = findChild(body, XMLA_NS, "ExecuteResponse");
		Element returnElement = findChild(executeResponse, XMLA_NS, "return");
		// <root> has children
		// <xsd:schema/>
		// <OlapInfo>
		// <CubeInfo>
		// <Cube>
		// <CubeName>FOO</CubeName>
		// </Cube>
		// </CubeInfo>
		// <AxesInfo>
		// <AxisInfo/> ...
		// </AxesInfo>
		// </OlapInfo>
		// <Axes>
		// <Axis>
		// <Tuples>
		// </Axis>
		// ...
		// </Axes>
		// <CellData>
		// <Cell/>
		// ...
		// </CellData>
		final Element root = findChild(returnElement, MDDATASET_NS, "root");

		if (olap4jStatement instanceof XmlaOlap4jPreparedStatement) {
			this.metaData = ((XmlaOlap4jPreparedStatement) olap4jStatement).cellSetMetaData;
		} else {
			this.metaData = createMetaData(root);
		}

		// todo: use CellInfo element to determine mapping of cell properties
		// to XML tags
		/*
		 * <CellInfo> <Value name="VALUE"/> <FmtValue name="FORMATTED_VALUE"/>
		 * <FormatString name="FORMAT_STRING"/> </CellInfo>
		 */

		final Element axesNode = findChild(root, MDDATASET_NS, "Axes");

		// First pass, gather up a list of member unique names to fetch
		// all at once.
		//
		// NOTE: This approach allows the driver to fetch a large number
		// of members in one round trip, which is much more efficient.
		// However, if the axis has a very large number of members, the map
		// may use too much memory. This is an unresolved issue.
		final MetadataReader metadataReader = metaData.cube.getMetadataReader();
		final Map<String, LdOlap4jMember> memberMap = new HashMap<String, LdOlap4jMember>();
		List<String> uniqueNames = new ArrayList<String>();
		for (Element axisNode : findChildren(axesNode, MDDATASET_NS, "Axis")) {
			final Element tuplesNode = findChild(axisNode, MDDATASET_NS,
					"Tuples");

			for (Element tupleNode : findChildren(tuplesNode, MDDATASET_NS,
					"Tuple")) {
				for (Element memberNode : findChildren(tupleNode, MDDATASET_NS,
						"Member")) {
					final String uname = stringElement(memberNode, "UName");
					uniqueNames.add(uname);
				}
			}
		}

		// Fetch all members on all axes. Hopefully it can all be done in one
		// round trip, or they are in cache already.
		metadataReader.lookupMembersByUniqueName(uniqueNames, memberMap);

		// Second pass, populate the axis.
		final Map<Property, Object> propertyValues = new HashMap<Property, Object>();
		for (Element axisNode : findChildren(axesNode, MDDATASET_NS, "Axis")) {
			final String axisName = axisNode.getAttribute("name");
			final Axis axis = lookupAxis(axisName);
			final ArrayList<Position> positions = new ArrayList<Position>();
			final LdOlap4jCellSetAxis cellSetAxis = new LdOlap4jCellSetAxis(
					this, axis, Collections.unmodifiableList(positions));
			if (axis.isFilter()) {
				filterAxis = cellSetAxis;
			} else {
				axisList.add(cellSetAxis);
			}
			final Element tuplesNode = findChild(axisNode, MDDATASET_NS,
					"Tuples");
			for (Element tupleNode : findChildren(tuplesNode, MDDATASET_NS,
					"Tuple")) {
				final List<Member> members = new ArrayList<Member>();
				for (Element memberNode : findChildren(tupleNode, MDDATASET_NS,
						"Member")) {
					String hierarchyName = memberNode.getAttribute("Hierarchy");
					final String uname = stringElement(memberNode, "UName");
					XmlaOlap4jMemberBase member = memberMap.get(uname);
					if (member == null) {
						final String caption = stringElement(memberNode,
								"Caption");
						final int lnum = integerElement(memberNode, "LNum");
						final Hierarchy hierarchy = lookupHierarchy(
								metaData.cube, hierarchyName);
						final Level level = hierarchy.getLevels().get(lnum);
						member = new XmlaOlap4jSurpriseMember(this, level,
								hierarchy, lnum, caption, uname);
					}
					propertyValues.clear();
					for (Element childNode : childElements(memberNode)) {
						LdOlap4jCellSetMemberProperty property = ((LdOlap4jCellSetAxisMetaData) cellSetAxis
								.getAxisMetaData()).lookupProperty(
								hierarchyName, childNode.getLocalName());
						if (property != null) {
							String value = childNode.getTextContent();
							propertyValues.put(property, value);
						}
					}
					if (!propertyValues.isEmpty()) {
						member = new XmlaOlap4jPositionMember(member,
								propertyValues);
					}
					members.add(member);
				}
				positions.add(new LdOlap4jPosition(members, positions.size()));
			}
		}

		// If XMLA did not return a filter axis, it means that the WHERE clause
		// evaluated to zero tuples. (If the query had no WHERE clause, it
		// would have evaluated to a single tuple with zero positions.)
		if (filterAxis == null) {
			filterAxis = new LdOlap4jCellSetAxis(this, Axis.FILTER,
					Collections.<Position> emptyList());
		}

		// Now, we fill the cells
		final Element cellDataNode = findChild(root, MDDATASET_NS, "CellData");
		for (Element cell : findChildren(cellDataNode, MDDATASET_NS, "Cell")) {
			propertyValues.clear();
			final int cellOrdinal = Integer.valueOf(cell
					.getAttribute("CellOrdinal"));
			final Object value = getTypedValue(cell);
			final String formattedValue = stringElement(cell, "FmtValue");
			final String formatString = stringElement(cell, "FormatString");
			Olap4jUtil.discard(formatString);
			for (Element element : childElements(cell)) {
				String tag = element.getLocalName();
				final Property property = metaData.propertiesByTag.get(tag);
				if (property != null) {
					propertyValues.put(property, element.getTextContent());
				}
			}
			cellMap.put(cellOrdinal, new LdOlap4jCell(this, cellOrdinal, value,
					formattedValue, propertyValues));
		}
	}

	/**
	 * Returns the value of a cell, cast to the appropriate Java object type
	 * corresponding to the XML schema (XSD) type of the value.
	 * 
	 * <p>
	 * The value type must conform to XSD definitions of the XML element. See <a
	 * href="http://books.xmlschemata.org/relaxng/relax-CHP-19.html">RELAX NG,
	 * Chapter 19</a> for a full list of possible data types.
	 * 
	 * <p>
	 * This method does not currently support all types; must numeric types are
	 * supported, but no dates are yet supported. Those not supported fall back
	 * to Strings.
	 * 
	 * @param cell
	 *            The cell of which we want the casted object.
	 * @return The object with a correct value.
	 * @throws OlapException
	 *             if any error is encountered while casting the cell value
	 */
	private Object getTypedValue(Element cell) throws OlapException {
		Element elm = findChild(cell, MDDATASET_NS, "Value");
		if (elm == null) {
			// Cell is null.
			return null;
		}

		// The object type is contained in xsi:type attribute.
		String type = elm.getAttribute("xsi:type");
		try {
			if (type.equals("xsd:int")) {
				return LdOlap4jUtil.intElement(cell, "Value");
			} else if (type.equals("xsd:integer")) {
				return LdOlap4jUtil.integerElement(cell, "Value");
			} else if (type.equals("xsd:double")) {
				return LdOlap4jUtil.doubleElement(cell, "Value");
			} else if (type.equals("xsd:float")) {
				return LdOlap4jUtil.floatElement(cell, "Value");
			} else if (type.equals("xsd:long")) {
				return LdOlap4jUtil.longElement(cell, "Value");
			} else if (type.equals("xsd:boolean")) {
				return LdOlap4jUtil.booleanElement(cell, "Value");
			} else {
				return LdOlap4jUtil.stringElement(cell, "Value");
			}
		} catch (Exception e) {
			throw getHelper().createException(
					"Error while casting a cell value to the correct java type for"
							+ " its XSD type " + type, e);
		}
	}

	/**
	 * Creates metadata for a cell set, given the DOM of the XMLA result.
	 * 
	 * @param root
	 *            Root node of XMLA result
	 * @return Metadata describing this cell set
	 * @throws OlapException
	 *             on error
	 * 
	 * 
	 */
	@Deprecated
	private LdOlap4jCellSetMetaData createMetaData(Element root)
			throws OlapException {
		// final Element olapInfo = findChild(root, MDDATASET_NS, "OlapInfo");
		// final Element cubeInfo = findChild(olapInfo, MDDATASET_NS,
		// "CubeInfo");
		// final Element cubeNode = findChild(cubeInfo, MDDATASET_NS, "Cube");
		// final Element cubeNameNode = findChild(cubeNode, MDDATASET_NS,
		// "CubeName");
		// final String cubeName = gatherText(cubeNameNode);
		//
		// // REVIEW: If there are multiple cubes with the same name, we should
		// // qualify by catalog and schema. Currently we just take the first.
		// XmlaOlap4jCube cube = lookupCube(
		// olap4jStatement.olap4jConnection.olap4jDatabaseMetaData,
		// cubeName);
		// if (cube == null) {
		// throw getHelper().createException(
		// "Internal error: cube '" + cubeName + "' not found");
		// }
		// // REVIEW: We should not modify the connection. It is not safe,
		// because
		// // connection might be shared between multiple statements with
		// different
		// // cubes. Caller should call
		// //
		// // connection.setCatalog(
		// //
		// cellSet.getMetaData().getCube().getSchema().getCatalog().getName())
		// //
		// // before doing metadata queries.
		// try {
		// this.olap4jStatement.olap4jConnection.setCatalog(cube.getSchema()
		// .getCatalog().getName());
		// } catch (SQLException e) {
		// throw getHelper().createException(
		// "Internal error: setting catalog '"
		// + cube.getSchema().getCatalog().getName()
		// + "' caused error");
		// }
		// final Element axesInfo = findChild(olapInfo, MDDATASET_NS,
		// "AxesInfo");
		// final List<Element> axisInfos = findChildren(axesInfo, MDDATASET_NS,
		// "AxisInfo");
		// final List<CellSetAxisMetaData> axisMetaDataList = new
		// ArrayList<CellSetAxisMetaData>();
		// XmlaOlap4jCellSetAxisMetaData filterAxisMetaData = null;
		// for (Element axisInfo : axisInfos) {
		// final String axisName = axisInfo.getAttribute("name");
		// Axis axis = lookupAxis(axisName);
		// final List<Element> hierarchyInfos = findChildren(axisInfo,
		// MDDATASET_NS, "HierarchyInfo");
		// final List<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();
		// /*
		// * <OlapInfo> <AxesInfo> <AxisInfo name="Axis0"> <HierarchyInfo
		// * name="Customers"> <UName
		// * name="[Customers].[MEMBER_UNIQUE_NAME]"/> <Caption
		// * name="[Customers].[MEMBER_CAPTION]"/> <LName
		// * name="[Customers].[LEVEL_UNIQUE_NAME]"/> <LNum
		// * name="[Customers].[LEVEL_NUMBER]"/> <DisplayInfo
		// * name="[Customers].[DISPLAY_INFO]"/> </HierarchyInfo> </AxisInfo>
		// * ... </AxesInfo> <CellInfo> <Value name="VALUE"/> <FmtValue
		// * name="FORMATTED_VALUE"/> <FormatString name="FORMAT_STRING"/>
		// * </CellInfo> </OlapInfo>
		// */
		// final List<XmlaOlap4jCellSetMemberProperty> propertyList = new
		// ArrayList<XmlaOlap4jCellSetMemberProperty>();
		// for (Element hierarchyInfo : hierarchyInfos) {
		// final String hierarchyName = hierarchyInfo.getAttribute("name");
		// Hierarchy hierarchy = lookupHierarchy(cube, hierarchyName);
		// hierarchyList.add(hierarchy);
		// for (Element childNode : childElements(hierarchyInfo)) {
		// String tag = childNode.getLocalName();
		// if (standardProperties.contains(tag)) {
		// continue;
		// }
		// final String propertyUniqueName = childNode
		// .getAttribute("name");
		// final XmlaOlap4jCellSetMemberProperty property = new
		// XmlaOlap4jCellSetMemberProperty(
		// propertyUniqueName, hierarchy, tag);
		// propertyList.add(property);
		// }
		// }
		// final XmlaOlap4jCellSetAxisMetaData axisMetaData = new
		// XmlaOlap4jCellSetAxisMetaData(
		// olap4jStatement.olap4jConnection, axis, hierarchyList,
		// propertyList);
		// if (axis.isFilter()) {
		// filterAxisMetaData = axisMetaData;
		// } else {
		// axisMetaDataList.add(axisMetaData);
		// }
		// }
		// if (filterAxisMetaData == null) {
		// filterAxisMetaData = new XmlaOlap4jCellSetAxisMetaData(
		// olap4jStatement.olap4jConnection, Axis.FILTER,
		// Collections.<Hierarchy> emptyList(),
		// Collections.<XmlaOlap4jCellSetMemberProperty> emptyList());
		// }
		// // cellInfo
		// final Element cellInfo = findChild(olapInfo, MDDATASET_NS,
		// "CellInfo");
		// List<XmlaOlap4jCellProperty> cellProperties = new
		// ArrayList<XmlaOlap4jCellProperty>();
		// for (Element element : childElements(cellInfo)) {
		// cellProperties.add(new XmlaOlap4jCellProperty(element
		// .getLocalName(), element.getAttribute("name")));
		// }
		// return new XmlaOlap4jCellSetMetaData(olap4jStatement, cube,
		// filterAxisMetaData, axisMetaDataList, cellProperties);
		return null;
	}

	/**
	 * Looks up a cube among all of the schemas in all of the catalogs in this
	 * connection.
	 * 
	 * <p>
	 * If there are several with the same name, returns the first.
	 * 
	 * @param databaseMetaData
	 *            Database metadata
	 * @param cubeName
	 *            Cube name
	 * @return Cube, or null if not found
	 * @throws OlapException
	 *             on error
	 */
	private LdOlap4jCube lookupCube(LdOlap4jDatabaseMetaData databaseMetaData,
			String cubeName) throws OlapException {
		for (Catalog catalog : databaseMetaData.olap4jConnection
				.getOlapCatalogs()) {
			for (Schema schema : catalog.getSchemas()) {
				for (Cube cube : schema.getCubes()) {
					if (cubeName.equals(cube.getName())) {
						return (LdOlap4jCube) cube;
					}
					if (cubeName.equals("[" + cube.getName() + "]")) {
						return (LdOlap4jCube) cube;
					}
				}
			}
		}
		return null;
	}

	/**
	 * Looks up a hierarchy in a cube with a given name or, failing that, a
	 * given unique name. Throws if not found.
	 * 
	 * @param cube
	 *            Cube
	 * @param hierarchyName
	 *            Name (or unique name) of hierarchy.
	 * @return Hierarchy
	 * @throws OlapException
	 *             on error
	 */
	private Hierarchy lookupHierarchy(LdOlap4jCube cube, String hierarchyName)
			throws OlapException {
		Hierarchy hierarchy = cube.getHierarchies().get(hierarchyName);
		if (hierarchy == null) {
			for (Hierarchy hierarchy1 : cube.getHierarchies()) {
				if (hierarchy1.getUniqueName().equals(hierarchyName)) {
					hierarchy = hierarchy1;
					break;
				}
			}
			if (hierarchy == null) {
				throw getHelper().createException(
						"Internal error: hierarchy '" + hierarchyName
								+ "' not found in cube '" + cube.getName()
								+ "'");
			}
		}
		return hierarchy;
	}

	/**
	 * Looks up an Axis with a given name.
	 * 
	 * @param axisName
	 *            Name of axis
	 * @return Axis
	 */
	private Axis lookupAxis(String axisName) {
		if (axisName.startsWith("Axis")) {
			final Integer ordinal = Integer.valueOf(axisName.substring("Axis"
					.length()));
			return Axis.Factory.forOrdinal(ordinal);
		} else {
			return Axis.FILTER;
		}
	}

	public CellSetMetaData getMetaData() {
		return metaData;
	}

	public Cell getCell(List<Integer> coordinates) {
		// We concat the members.
		String concatNr = "";
		Measure measure = null;
		for (int i = 0; i < coordinates.size(); i++) {
			Position position = getAxes().get(i).getPositions()
					.get(coordinates.get(i));
			for (Member member : position.getMembers()) {
				if (member.getMemberType() == Member.Type.MEASURE
						|| member.getMemberType() == Member.Type.FORMULA) {
					if (measure != null) {
						// There should not be several measures per cell
						throw new UnsupportedOperationException(
								"There should not be used several measures per cell!");
					}
					measure = (Measure) member;
				} else {
					concatNr += LdOlap4jUtil.convertMDXtoURI(member
							.getUniqueName());
				}
			}
		}

		/*
		 * We need to know the number of the measure
		 */
		int index = 0;
		for (int i = 0; i < this.measurelist.size() && measure != null; i++) {
			if (measurelist.get(i).getName().equals(measure.getName())) {
				index = i;
				break;
			}
		}

		/*
		 * Now, we have the members and the measure index
		 */
		String[] hashedCells = newValueMap.get(concatNr.hashCode());
		if (hashedCells != null && hashedCells[index] != null) {
			return new LdOlap4jCell(this, coordinatesToOrdinal(coordinates), hashedCells[index],
					hashedCells[index],
					Collections.<Property, Object> emptyMap());
		} else {
			return new LdOlap4jCell(this, coordinatesToOrdinal(coordinates), "", "",
					Collections.<Property, Object> emptyMap());
		}
		//
		// // Filter axis we need to consider, also
		// // Slicer dimensions, for which data is retrieved for a single member
		// // If several positions are given, we need to take them as an OR
		// List<Member> filtermembers = new ArrayList<Member>();
		//
		// for (Position position : filterAxis.getPositions()) {
		// /*
		// * TODO: Momentarily, this only makes sense if each position
		// * contains exaclty one measure.
		// */
		// for (Member member : position.getMembers()) {
		// if (member.getMemberType() == Member.Type.MEASURE) {
		// if (measure != null) {
		// // There should not be several measures per cell
		// throw new UnsupportedOperationException(
		// "There should not be used several measures per cell!");
		// }
		// measure = (Measure) member;
		// } else {
		// // Add to filtermembers (that should be "or"ed
		// filtermembers.add(member);
		// }
		// }
		// }
		//
		// // If no measure, then find default
		// if (measure == null) {
		// // Default is simply the first we find.
		// measure = cube.getMeasures().get(0);
		// }
		//
		// /*
		// * Dimensions that are not explicitly assigned to an axis are assumed
		// to
		// * be slicer dimensions and filter with their default members.
		// *
		// * I simply ask for the hierarchies in each axis. For all dimensions
		// of
		// * the cube, I find those which are not represented, and take the
		// * DEFAULT_HIERARCHY, and its DEFAULT_MEMBER.
		// *
		// * The problem is, that we currently do not have default members. If
		// no
		// * default member is set, we do not consider this dimension, at the
		// * moment. That means, we take all (do not distinguish), which can
		// bring
		// * problems in SPARQL.
		// *
		// * TODO: implement this above.
		// */
		//
		// // Caching
		// Integer cachedResults = this.getResults(this, pos, cube, members,
		// filtermembers, measure);
		//
		// if (cachedResults == null || cachedResults == 0) {
		// return new LdOlap4jCell(this, pos, null, "",
		// Collections.<Property, Object> emptyMap());
		// } else {
		// // Now, create result: We need: a value
		// Cell cell = this.olap4jStatement.olap4jConnection.myLinkedData
		// .getOlapCellResult(this, pos, cube, members, filtermembers,
		// measure);
		// if (cell == null) {
		// if (pos < 0 || pos >= maxOrdinal()) {
		// throw new IndexOutOfBoundsException();
		// } else {
		// // Cell is within bounds, but is not held in the cache
		// // because
		// // it has no value. Manufacture a cell with an empty value.
		// return new LdOlap4jCell(this, pos, null, "",
		// Collections.<Property, Object> emptyMap());
		// }
		// }
		// return cell;
		// }
	}

	public Cell getCell(int ordinal) {
		return getCell(ordinalToCoordinates(ordinal));
	}

	public Cell getCell(Position... positions) {
		if (positions.length != getAxes().size()) {
			throw new IllegalArgumentException(
					"cell coordinates should have dimension "
							+ getAxes().size());
		}
		List<Integer> coords = new ArrayList<Integer>(positions.length);
		for (Position position : positions) {
			coords.add(position.getOrdinal());
		}
		return getCell(coords);
	}

	// private Integer getResults(LdOlap4jCellSet xmlaOlap4jCellSet, int pos,
	// LdOlap4jCube cube, List<Member> members,
	// List<Member> filtermembers, Measure measure) {
	//
	// // Take all member unique names (which are the concept uris) and
	// // concatenate, translate into integer
	// // and return
	// String memberConcat = "";
	// int memberConcatHashCode = 0;
	// for (Member member : members) {
	//
	// String conceptURIs = LdOlap4jUtil.decodeUriForMdx(member
	// .getUniqueName());
	// memberConcat += conceptURIs;
	// }
	// // Measure also concat (!)
	// memberConcat += measure.getUniqueName();
	// memberConcatHashCode = memberConcat.hashCode();
	// // Do not take filter members into account, since they will be used with
	// // OR
	// // for (Member member : filtermembers) {
	// // memberConcat += member.getCaption();
	// // }
	//
	// if (this.hierarchyCellMap == null
	// || !this.hierarchyCellMap.containsKey(memberConcatHashCode)) {
	//
	// /*
	// * I now have all the hierarchies of involved queries. For quicker
	// * queries, I will have a large query asking for members of each
	// * hierarchy and the number of results, and store it in a map of
	// * Integer (concatenated members to Long) and Integer (Number of
	// * occurrences). Then, when resolving, I can simply return null for
	// * each time, there is no occurrence.
	// */
	//
	// this.hierarchyCellMap = olap4jStatement.olap4jConnection.myLinkedData
	// .getOlapResultEstimation(this, pos, cube, members,
	// filtermembers, measure);
	// }
	//
	// if (this.hierarchyCellMap.containsKey(memberConcatHashCode)) {
	// return this.hierarchyCellMap.get(memberConcatHashCode);
	// } else {
	// // This means, we return null, which will be translated into an
	// // empty value anyway.
	// return null;
	// }
	// }

	/**
	 * Returns a string describing the maximum coordinates of this cell set; for
	 * example "2, 3" for a cell set with 2 columns and 3 rows.
	 * 
	 * @return description of cell set bounds
	 */
	private String getBoundsAsString() {
		StringBuilder buf = new StringBuilder();
		int k = 0;
		for (CellSetAxis axis : getAxes()) {
			if (k++ > 0) {
				buf.append(", ");
			}
			buf.append(axis.getPositionCount());
		}
		return buf.toString();
	}

	public List<CellSetAxis> getAxes() {
		return immutableAxisList;
	}

	public CellSetAxis getFilterAxis() {
		return filterAxis;
	}

	/**
	 * Returns the ordinal of the last cell in this cell set. This is the
	 * product of the cardinalities of all axes.
	 * 
	 * @return ordinal of last cell in cell set
	 */
	private int maxOrdinal() {
		int modulo = 1;
		for (CellSetAxis axis : axisList) {
			modulo *= axis.getPositionCount();
		}
		return modulo;
	}

	public List<Integer> ordinalToCoordinates(int ordinal) {
		List<CellSetAxis> axes = getAxes();
		final List<Integer> list = new ArrayList<Integer>(axes.size());
		int modulo = 1;
		for (CellSetAxis axis : axes) {
			int prevModulo = modulo;
			modulo *= axis.getPositionCount();
			list.add((ordinal % modulo) / prevModulo);
		}
		if (ordinal < 0 || ordinal >= modulo) {
			throw new IndexOutOfBoundsException("Cell ordinal " + ordinal
					+ ") lies outside CellSet bounds (" + getBoundsAsString()
					+ ")");
		}
		return list;
	}

	/**
	 * From a list of coordinates that relate to the axes (getAxes),
	 */
	public int coordinatesToOrdinal(List<Integer> coordinates) {
		List<CellSetAxis> axes = getAxes();
		if (coordinates.size() != axes.size()) {
			throw new IllegalArgumentException(
					"Coordinates have different dimension "
							+ coordinates.size() + " than axes " + axes.size());
		}
		int modulo = 1;
		int ordinal = 0;
		int k = 0;
		for (CellSetAxis axis : axes) {
			final Integer coordinate = coordinates.get(k++);
			if (coordinate < 0 || coordinate >= axis.getPositionCount()) {
				throw new IndexOutOfBoundsException("Coordinate " + coordinate
						+ " of axis " + k + " is out of range ("
						+ getBoundsAsString() + ")");
			}
			// Add to ordinal the actual coordinate of the axis times the size
			// of the last axis
			ordinal += coordinate * modulo;
			// modulo is the number of positions in this axis
			modulo *= axis.getPositionCount();
		}
		return ordinal;
	}

	public boolean next() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void close() throws SQLException {
		this.closed = true;
	}

	public boolean wasNull() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getString(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public byte getByte(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public short getShort(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getInt(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public long getLong(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public float getFloat(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public double getDouble(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Date getDate(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getString(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public byte getByte(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public short getShort(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getInt(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public long getLong(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public float getFloat(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public double getDouble(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Date getDate(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void clearWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public String getCursorName() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int findColumn(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isBeforeFirst() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isAfterLast() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isFirst() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isLast() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void beforeFirst() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void afterLast() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean first() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean last() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean absolute(int row) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean relative(int rows) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean previous() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setFetchDirection(int direction) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getFetchDirection() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setFetchSize(int rows) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getFetchSize() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getType() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getConcurrency() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean rowUpdated() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean rowInserted() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean rowDeleted() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateNull(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBytes(int columnIndex, byte x[]) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateNull(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateString(String columnLabel, String x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBytes(String columnLabel, byte x[]) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void insertRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void deleteRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void refreshRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void cancelRowUpdates() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void moveToInsertRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void moveToCurrentRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public OlapStatement getStatement() {
		return olap4jStatement;
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Ref getRef(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Clob getClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Array getArray(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Ref getRef(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Blob getBlob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Clob getClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Array getArray(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public URL getURL(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public URL getURL(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	// implement Wrapper

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Implementation of {@link Member} for a member which is not present in the
	 * cube (probably because the member is a calculated member defined in the
	 * query).
	 */
	private static class XmlaOlap4jSurpriseMember implements
			XmlaOlap4jMemberBase {
		private final LdOlap4jCellSet cellSet;
		private final Level level;
		private final Hierarchy hierarchy;
		private final int lnum;
		private final String caption;
		private final String uname;

		/**
		 * Creates an XmlaOlap4jSurpriseMember.
		 * 
		 * @param cellSet
		 *            Cell set
		 * @param level
		 *            Level
		 * @param hierarchy
		 *            Hierarchy
		 * @param lnum
		 *            Level number
		 * @param caption
		 *            Caption
		 * @param uname
		 *            Member unique name
		 */
		XmlaOlap4jSurpriseMember(LdOlap4jCellSet cellSet, Level level,
				Hierarchy hierarchy, int lnum, String caption, String uname) {
			this.cellSet = cellSet;
			this.level = level;
			this.hierarchy = hierarchy;
			this.lnum = lnum;
			this.caption = caption;
			this.uname = uname;
		}

		public final LdOlap4jCube getCube() {
			return cellSet.metaData.cube;
		}

		public final LdOlap4jConnection getConnection() {
			return getCatalog().olap4jDatabaseMetaData.olap4jConnection;
		}

		public final LdOlap4jCatalog getCatalog() {
			return getCube().olap4jSchema.olap4jCatalog;
		}

		public Map<Property, Object> getPropertyValueMap() {
			return Collections.emptyMap();
		}

		public NamedList<? extends Member> getChildMembers() {
			return Olap4jUtil.emptyNamedList();
		}

		public int getChildMemberCount() {
			return 0;
		}

		public Member getParentMember() {
			return null;
		}

		public Level getLevel() {
			return level;
		}

		public Hierarchy getHierarchy() {
			return hierarchy;
		}

		public Dimension getDimension() {
			return hierarchy.getDimension();
		}

		public Type getMemberType() {
			return Type.UNKNOWN;
		}

		public boolean isAll() {
			return false; // FIXME
		}

		public boolean isChildOrEqualTo(Member member) {
			return false; // FIXME
		}

		public boolean isCalculated() {
			return false; // FIXME
		}

		public int getSolveOrder() {
			return 0; // FIXME
		}

		public ParseTreeNode getExpression() {
			return null;
		}

		public List<Member> getAncestorMembers() {
			return Collections.emptyList(); // FIXME
		}

		public boolean isCalculatedInQuery() {
			return true; // probably
		}

		public Object getPropertyValue(Property property) {
			return null;
		}

		public String getPropertyFormattedValue(Property property) {
			return null;
		}

		public void setProperty(Property property, Object value) {
			throw new UnsupportedOperationException();
		}

		public NamedList<Property> getProperties() {
			return Olap4jUtil.emptyNamedList();
		}

		public int getOrdinal() {
			return -1; // FIXME
		}

		public boolean isHidden() {
			return false;
		}

		public int getDepth() {
			return lnum;
		}

		public Member getDataMember() {
			return null;
		}

		public String getName() {
			return caption;
		}

		public String getUniqueName() {
			return uname;
		}

		public String getCaption() {
			return caption;
		}

		public String getDescription() {
			return null;
		}

		public boolean isVisible() {
			return true;
		}
	}
}

// End XmlaOlap4jCellSet.java
