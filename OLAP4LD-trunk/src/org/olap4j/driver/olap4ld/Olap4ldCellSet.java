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
package org.olap4j.driver.olap4ld;

import static org.olap4j.driver.olap4ld.Olap4ldUtil.MDDATASET_NS;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.SOAP_NS;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.XMLA_NS;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.childElements;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.findChild;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.findChildren;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.integerElement;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.parse;
import static org.olap4j.driver.olap4ld.Olap4ldUtil.stringElement;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.CellSetMetaData;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.driver.olap4ld.helper.GetFirstOccurrenceVisitor;
import org.olap4j.driver.olap4ld.helper.GraphVizVisualisationVisitor;
import org.olap4j.driver.olap4ld.helper.LdHelper;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.driver.olap4ld.helper.PreprocessMdxVisitor;
import org.olap4j.driver.olap4ld.linkeddata.BaseCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.ConvertCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.DiceOp;
import org.olap4j.driver.olap4ld.linkeddata.DrillAcrossOp;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOperatorQueryPlanVisitor;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapQueryPlan;
import org.olap4j.driver.olap4ld.linkeddata.ProjectionOp;
import org.olap4j.driver.olap4ld.linkeddata.QueryException;
import org.olap4j.driver.olap4ld.linkeddata.ReconciliationCorrespondence;
import org.olap4j.driver.olap4ld.linkeddata.Restrictions;
import org.olap4j.driver.olap4ld.linkeddata.RollupOp;
import org.olap4j.driver.olap4ld.linkeddata.SliceOp;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;
import org.olap4j.metadata.Schema;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * Implementation of {@link org.olap4j.CellSet} for XML/A providers.
 * 
 * <p>
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs; it is
 * instantiated using {@link Factory#newCellSet}.
 * </p>
 * 
 * @author jhyde, bkaempgen
 * @version $Id: XmlaOlap4jCellSet.java 455 2011-05-24 10:01:26Z jhyde $
 * @since May 24, 2007
 */
abstract class Olap4ldCellSet implements CellSet {
	// Meta Meta Data
	final Olap4ldStatement olap4jStatement;
	protected boolean closed;
	@SuppressWarnings("unused")
	private static final List<String> standardProperties = Arrays.asList(
			"UName", "Caption", "LName", "LNum", "DisplayInfo");
	// Metadata
	private Olap4ldCellSetMetaData metaData;
	private List<Olap4ldCellSetAxis> axisList;
	private List<CellSetAxis> immutableAxisList;
	private Olap4ldCellSetAxis filterAxis;

	// Data
	private final Map<Integer, Cell> cellMap = new HashMap<Integer, Cell>();
	/**
	 * The value map that the OLAP query populates and from which the cells are
	 * queried. It stores string representations of cell values of indexed
	 * measures in string arrays, keyed-off by a hashcode of the concatenation
	 * of the uniquely identifying list of members.
	 */
	private boolean areCellsPopulated = false;
	private final Map<Integer, String[]> newValueMap = new HashMap<Integer, String[]>();
	private LogicalOlapQueryPlan queryplan;
	// The list of measures queried here as part of metadata
	private List<Member> measureList;
	private ArrayList<Integer> dimensionindicesList;

	/**
	 * Creates an XmlaOlap4jCellSet.
	 * 
	 * @param olap4jStatement
	 *            Statement
	 */
	Olap4ldCellSet(Olap4ldStatement olap4jStatement) {
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
	 * Execute the query plan.
	 * 
	 * @throws OlapException
	 */
	private void populateCells() throws OlapException {

		Olap4ldUtil._log.info("Execute logical query plan: "
				+ queryplan.toString());
		/*
		 * Now, execute Logical OLAP Operator Query Tree in LinkedDataEngine
		 */
		List<Node[]> olapQueryResult = olap4jStatement.olap4jConnection.myLinkedData
				.executeOlapQuery(queryplan);

		Olap4ldUtil._log.info("Execute logical query plan: Cache results.");
		long time = System.currentTimeMillis();

		// Important part, we need to allow efficient access to the results
		cacheDataFromOlapQuery(olapQueryResult);

		areCellsPopulated = true;
		// We track the time it takes to prepare the query
		time = System.currentTimeMillis() - time;
		Olap4ldUtil._log
				.info("Execute logical query plan: Cache results finished in "
						+ time + "ms.");
	}

	/**
	 * 
	 * This is the method to map an MDX query to 1) metadata of a pivot table
	 * and 2) an initial logical olap query plan to populate the pivot table
	 * cells.
	 * 
	 * We create: this.metaData = metadata; this.axisList = axisList;
	 * this.immutableAxisList =
	 * Olap4jUtil.cast(Collections.unmodifiableList(axisList)); this.filterAxis
	 * = filterCellSetAxis;
	 * 
	 * From this metadata, we can then create a nested set of OLAP operations (=
	 * OLAP query)
	 * 
	 * 
	 * @param selectNode
	 * @throws OlapException
	 */
	public void createMetadata(SelectNode selectNode) throws OlapException {

		// Any advantage of having a prepared statement? No, since we use NON
		// EMPTY CLAUSE.
		// if (olap4jStatement instanceof Olap4ldPreparedStatement) {
		// this.metaData = ((Olap4ldPreparedStatement)
		// olap4jStatement).cellSetMetaData;
		// } else {

		// I pre-process the select node
		// I need to replace Identifier.Members with Members(Identifier)
		PreprocessMdxVisitor<Object> preprocessMdxVisitor = new PreprocessMdxVisitor<Object>();
		selectNode.accept(preprocessMdxVisitor);

		// Visitor to create: metaData, axisList, filterAxis

		MdxMetadataExtractorVisitor<Object> visitor = new MdxMetadataExtractorVisitor<Object>(
				olap4jStatement);
		selectNode.accept(visitor);

		// Can also return the global cube
		// Question: Do we actually get back the "global cube" here? Yes.
		// This is the reason for generating a new cube (global cube): Olap4ld
		// requires exactly on cube?
		Cube cube = visitor.getCube();

		// Axes Metadata (create MetaData for one specific axis)
		List<Olap4ldCellSetAxisMetaData> axisMetaDataList = new ArrayList<Olap4ldCellSetAxisMetaData>();
		// Axes
		List<Olap4ldCellSetAxis> axisList = new ArrayList<Olap4ldCellSetAxis>();
		// Properties are not computed, yet.
		List<Olap4ldCellSetMemberProperty> propertyList = new ArrayList<Olap4ldCellSetMemberProperty>();
		List<Olap4ldCellSetMemberProperty> properties = propertyList;

		Axis columnAxis = Axis.COLUMNS;
		List<Position> columnPositions = visitor.getColumnPositions();
		List<Hierarchy> columnHierarchies = new ArrayList<Hierarchy>();
		columnHierarchies = visitor.createHierarchyList(columnPositions);
		// List<Level> columnLevels = new ArrayList<Level>();
		// columnLevels = this.createLevelList(columnPositions);

		// Add axisMetaData to list of axes metadata
		final Olap4ldCellSetAxisMetaData columnAxisMetaData = new Olap4ldCellSetAxisMetaData(
				this.olap4jStatement.olap4jConnection, columnAxis,
				columnHierarchies, properties);

		axisMetaDataList.add(columnAxisMetaData);

		// Add cellSetAxis to list of axes
		final Olap4ldCellSetAxis columnCellSetAxis = new Olap4ldCellSetAxis(
				this, columnAxis, Collections.unmodifiableList(columnPositions));
		axisList.add(columnCellSetAxis);

		Axis rowAxis = Axis.ROWS;
		List<Position> rowPositions = visitor.getRowPositions();
		List<Hierarchy> rowHierarchies = new ArrayList<Hierarchy>();
		rowHierarchies = visitor.createHierarchyList(rowPositions);
		// List<Level> rowLevels = new ArrayList<Level>();
		// rowLevels = this.createLevelList(rowPositions);

		// Add axisMetaData to list of axes metadata
		final Olap4ldCellSetAxisMetaData rowAxisMetaData = new Olap4ldCellSetAxisMetaData(
				this.olap4jStatement.olap4jConnection, rowAxis, rowHierarchies,
				properties);
		axisMetaDataList.add(rowAxisMetaData);

		// Add cellSetAxis to list of axes
		final Olap4ldCellSetAxis rowCellSetAxis = new Olap4ldCellSetAxis(this,
				rowAxis, Collections.unmodifiableList(rowPositions));
		axisList.add(rowCellSetAxis);

		Axis filterAxis = Axis.FILTER;
		List<Position> filterPositions = visitor.getFilterPositions();
		// Create filter hierarchies
		List<Hierarchy> filterHierarchies = new ArrayList<Hierarchy>();
		filterHierarchies = visitor.createHierarchyList(filterPositions);
		// List<Level> filterLevels = new ArrayList<Level>();
		// filterLevels = this.createLevelList(filterPositions);
		// I need to create a filter axis metadata
		Olap4ldCellSetAxisMetaData filterAxisMetaData = new Olap4ldCellSetAxisMetaData(
				this.olap4jStatement.olap4jConnection, filterAxis,
				Collections.<Hierarchy> unmodifiableList(filterHierarchies),
				Collections.<Olap4ldCellSetMemberProperty> emptyList());
		// I need to create a filter axis
		Olap4ldCellSetAxis filterCellSetAxis = new Olap4ldCellSetAxis(this,
				filterAxis, Collections.unmodifiableList(filterPositions));

		List<Olap4ldCellProperty> cellProperties = new ArrayList<Olap4ldCellProperty>();
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

		Olap4ldCellSetMetaData metadata = new Olap4ldCellSetMetaData(
				olap4jStatement, (Olap4ldCube) cube, filterAxisMetaData,
				axisMetaDataList, cellProperties);

		this.metaData = metadata;
		this.axisList = axisList;
		this.immutableAxisList = Olap4jUtil.cast(Collections
				.unmodifiableList(axisList));
		this.filterAxis = filterCellSetAxis;

		// We create helper metadata objects.

		// As a helper set, we use set to collect the measures
		HashSet<Member> usedMeasureSet = new HashSet<Member>();

		// Columns

		for (Position position : columnPositions) {
			List<Member> positionmembers = position.getMembers();
			for (int i = 0; i < positionmembers.size(); i++) {
				// Check whether measure already contained.
				if ((positionmembers.get(i).getMemberType() == Member.Type.MEASURE || positionmembers
						.get(i).getMemberType() == Member.Type.FORMULA)) {

					usedMeasureSet.add(positionmembers.get(i));
				}
			}
		}

		// Rows

		for (Position position : rowPositions) {
			List<Member> positionmembers = position.getMembers();
			for (int i = 0; i < positionmembers.size(); i++) {
				// Check whether measure already contained.
				if ((positionmembers.get(i).getMemberType() == Member.Type.MEASURE || positionmembers
						.get(i).getMemberType() == Member.Type.FORMULA)) {

					usedMeasureSet.add(positionmembers.get(i));
				}
			}
		}

		// In filter axis
		for (Position list : filterPositions) {
			// For each position, we get a new member for each restriction set
			List<Member> filterpositionmembers = list.getMembers();
			for (int i = 0; i < filterpositionmembers.size(); i++) {
				// Check whether measure already contained.
				if ((filterpositionmembers.get(i).getMemberType() == Member.Type.MEASURE || filterpositionmembers
						.get(i).getMemberType() == Member.Type.FORMULA)) {
					usedMeasureSet.add(filterpositionmembers.get(i));
				}
			}
		}

		// If no measure, then find default
		if (usedMeasureSet.isEmpty()) {
			// Default is simply the first we find.
			Olap4ldUtil._log
					.config("Get default (first available) measure in cube.");
			if (!metaData.cube.getMeasures().isEmpty()) {
				usedMeasureSet.add(metaData.cube.getMeasures().get(0));
			} else {
				throw new UnsupportedOperationException(
						"There should always be at least one measure in the cube.");
			}
		}

		// UsedMeasureSet we could store.
		// We store this list of measures for later looking up the number of a
		// measure.
		List<Member> myMeasureList = new ArrayList<Member>();

		// We have to set an ordering of measures.
		Iterator<Member> iterator = usedMeasureSet.iterator();
		while (iterator.hasNext()) {
			Member member = (Member) iterator.next();
			myMeasureList.add(member);
		}

		this.measureList = myMeasureList;

		// Now that we have retrieved all MDX metadata, we can create the
		// Logical Olap Query Plan
		this.queryplan = createInitialLogicalOlapQueryPlan();
	}

	/**
	 * In this implementation, we have not implemented the transformation from
	 * MDC Parse Tree to Logical Olap Query tree via a visitor pattern but via
	 * procedural code.
	 * 
	 * Here, we create an initial logical olap query plan from the retrieved
	 * metadata.
	 * 
	 * 
	 * @return
	 */
	private LogicalOlapQueryPlan createInitialLogicalOlapQueryPlan() {

		try {
			// We should not transform cube to Node, but search through cubes
			Restrictions restrictions = new Restrictions();
			restrictions.cubeNamePattern = Olap4ldLinkedDataUtil
					.convertMDXtoURI(metaData.cube.getUniqueName());
			List<Node[]> cubes = this.olap4jStatement.olap4jConnection.myLinkedData
					.getCubes(restrictions);

			Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
					.getNodeResultFields(cubes.get(0));

			List<Node[]> cube = new ArrayList<Node[]>();
			cube.add(cubes.get(0));
			for (Node[] aCube : cubes) {
				if (aCube[cubemap.get("?CUBE_NAME")]
						.equals(restrictions.cubeNamePattern)) {
					// We do not need all cubes.
					cube.add(aCube);
				}
			}

			LogicalOlapOp queryplanroot = null;

			/*
			 * The set of "available datasets" is given by the FROM clause.
			 * 
			 * For every dataset, we create an own logical query plan.
			 * 
			 * In case there are several datasets, we drill-across all the
			 * resulting data cubes.
			 * 
			 * I think, this process, we need to "formalise". What exactly do we
			 * do here?
			 * 
			 * 
			 * According to the global cube defintion we should: Only
			 * drill-across over datasets that 1) show one of the queried
			 * measures, 2) does have all the "inquired dimensions".
			 */

			String[] datasets = restrictions.cubeNamePattern.toString().split(
					",");
			List<LogicalOlapOp> singlecubequeryplans = new ArrayList<LogicalOlapOp>();

			Olap4ldUtil._log.info("Creating start ops:...");

			List<LogicalOlapOp> startops = new ArrayList<LogicalOlapOp>();

			// Without Convert-Cube or Merge-Cubes

			for (int i = 0; i < datasets.length; i++) {
				String dataset = datasets[i];

				restrictions = new Restrictions();
				restrictions.cubeNamePattern = new Resource(dataset);

				// Ask for the cube
				// List<Node[]> singlecube =
				// this.olap4jStatement.olap4jConnection.myLinkedData
				// .getCubes(restrictions);

				// So far, we only take into account the datasets,
				// directly. Soon, we
				// should automatically derive new datasets to query
				// here, also.

				// Now, I create all possible derived datasets

				// Cube from (cube, SlicesRollups, Dices, Projections)
				LogicalOlapOp basecube = new BaseCubeOp(
						restrictions.cubeNamePattern.toString());

				startops.add(basecube);
			}

			// // Convert-Cube combinations
			//
			//
			// // Merge-Cubes combinations
			// int DEPTH = 2;
			// for (int i = 0; i <= DEPTH; i++) {
			// XXX: generateMergeLqpsForDepth is now in TestCase
			// List<LogicalOlapOp> intermediaryops = Olap4ldLinkedDataUtil
			// .generateMergeLqpsForDepth(i, datasets, EmbeddedSesameEngine
			// .getReconciliationCorrespondences(true));
			// for (LogicalOlapOp logicalOlapOp : intermediaryops) {
			// startops.add(logicalOlapOp);
			// }
			// }

			/*
			 * Here, we should be loading the correspondences and creating all
			 * possible derived datasets using XSB prolog.
			 */

			Olap4ldUtil._log.info("Finished creating " + startops.size()
					+ " start ops.");

			Olap4ldUtil._log.info("Started collecting non-null query plans.");
			for (LogicalOlapOp startop : startops) {
				LogicalOlapOp singlequeryplan = createInitialQueryPlanPerDataSet(startop);
				if (singlequeryplan != null) {
					singlecubequeryplans.add(singlequeryplan);
				}
			}

			Olap4ldUtil._log.info("Finished collecting "
					+ singlecubequeryplans.size() + "non-null query plans.");

			// for (int i = 0; i < datasets.length; i++) {
			// String dataset = datasets[i];
			//
			// restrictions = new Restrictions();
			// restrictions.cubeNamePattern = new Resource(dataset);
			//
			// // Ask for the cube
			// try {
			//
			// // Here, the cubes get loaded.
			// // XXX: Necessary to do that before execution?
			// List<Node[]> singlecube =
			// this.olap4jStatement.olap4jConnection.myLinkedData
			// .getCubes(restrictions);
			//
			// // So far, we only take into account the datasets,
			// // directly. Soon, we
			// // should automatically derive new datasets to query
			// // here, also.
			//
			// // Now, I create all possible derived datasets
			//
			// // For every correspondence
			//
			//
			//
			// } catch (OlapException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }

			queryplanroot = null;
			// queryplanroot is simply
			// XXX: can we make it bushy?
			// XXX: Currently, every single dataset is queried, however,
			// not every
			// dataset fits the query.

			/*
			 * In theory, here we are evaluating the views, i.e., the definition
			 * of the global dataset by the single datasets and the automatic
			 * derivation of more single datasets.
			 */

			Olap4ldUtil._log.info("Started building drill-across query plan.");

			for (LogicalOlapOp logicalOlapOp : singlecubequeryplans) {
				if (queryplanroot == null) {
					queryplanroot = logicalOlapOp;
				} else {
					DrillAcrossOp drillacross = new DrillAcrossOp(
							queryplanroot, logicalOlapOp);
					queryplanroot = drillacross;
				}
			}

			Olap4ldUtil._log.info("Finished building drill-across query plan.");

			LogicalOlapQueryPlan myplan = new LogicalOlapQueryPlan(
					queryplanroot);

			try {
				Olap4ldUtil._log.info("DOT: " + myplan.toDOT() + ".");
			} catch (QueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return myplan;

		} catch (OlapException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;
	}

	private List<LogicalOlapOp> generateStartOps(String[] datasets) {

		Olap4ldUtil._log.info("Creating start ops:...");

		List<LogicalOlapOp> startops = new CopyOnWriteArrayList<LogicalOlapOp>();

		// Add all datasets
		for (int i = 0; i < datasets.length; i++) {
			startops.add(new BaseCubeOp(datasets[i]));
		}

		List<ReconciliationCorrespondence> correspondences = EmbeddedSesameEngine
				.getReconciliationCorrespondences(true);

		// cc
		for (ReconciliationCorrespondence convertCorrespondence : correspondences) {
			// Check whether convert-cube
			if (convertCorrespondence.getInputmembers2() == null) {

				for (LogicalOlapOp olapop : startops) {

					if (!derivedBy(olapop, convertCorrespondence)) {
						startops.add(new ConvertCubeOp(olapop,
								convertCorrespondence));
					}

				}

			}
		}

		// mc
		for (ReconciliationCorrespondence mergingCorrespondence : correspondences) {
			// Check whether merge-cubes
			if (mergingCorrespondence.getInputmembers2() != null) {

				for (LogicalOlapOp olapop1 : startops) {

					for (LogicalOlapOp olapop2 : startops) {

						if (!derivedBy(olapop1, mergingCorrespondence)
								&& !derivedBy(olapop2, mergingCorrespondence)) {

							startops.add(new ConvertCubeOp(olapop1, olapop2,
									mergingCorrespondence));

						}

					}

				}
			}
		}

		Olap4ldUtil._log.info("Finished creating " + startops.size()
				+ " start ops.");

		return startops;
	}

	private boolean derivedBy(LogicalOlapOp olapop,
			ReconciliationCorrespondence reconciliationCorrespondence) {
		GetFirstOccurrenceVisitor derivedbyvisitor = new GetFirstOccurrenceVisitor(
				new ConvertCubeOp(null, reconciliationCorrespondence));

		try {
			olapop.accept(derivedbyvisitor);
		} catch (QueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (derivedbyvisitor.getFirstOccurence() != null) {
			return true;

		} else {
			return false;
		}
	}

	/**
	 * Here, we try to create an initial query plan if the datasets fits.
	 * 
	 * @param startop
	 * @return
	 */
	private LogicalOlapOp createInitialQueryPlanPerDataSet(LogicalOlapOp startop) {

		Olap4ldUtil._log.info("Started creating query plan...");

		// BaseCube operator
		boolean first;

		// Cube from (cube, SlicesRollups, Dices, Projections)
		// LogicalOlapOp basecube = new BaseCubeOp(
		// restrictions.cubeNamePattern.toString());

		// Need to search for first cube starting from inputOp1 (which defines
		// the schema).
		LogicalOlapOp convertcube = startop;
		LogicalOlapOp previouscube = null;

		while (convertcube instanceof ConvertCubeOp) {

			// Check whether all inputmembers fit.

			// For now, we do not allow the same merge-cubes operator used
			// twice.
			if (previouscube != null
					&& ((ConvertCubeOp) convertcube).conversioncorrespondence
							.getname()
							.equals(((ConvertCubeOp) previouscube).conversioncorrespondence
									.getname())) {
				return null;
			}

			previouscube = convertcube;
			convertcube = ((ConvertCubeOp) convertcube).inputOp1;

		}

		BaseCubeOp basecube = (BaseCubeOp) convertcube;

		Restrictions restrictions = new Restrictions();
		restrictions.cubeNamePattern = new Resource(basecube.dataseturi);

		List<Node[]> measures = new ArrayList<Node[]>();
		List<Node[]> dimensions = new ArrayList<Node[]>();
		List<Node[]> hierarchies = new ArrayList<Node[]>();
		List<Node[]> levels = new ArrayList<Node[]>();
		List<Node[]> members = new ArrayList<Node[]>();
		try {
			measures = this.olap4jStatement.olap4jConnection.myLinkedData
					.getMeasures(restrictions);
			dimensions = this.olap4jStatement.olap4jConnection.myLinkedData
					.getDimensions(restrictions);
			hierarchies = this.olap4jStatement.olap4jConnection.myLinkedData
					.getHierarchies(restrictions);
			levels = this.olap4jStatement.olap4jConnection.myLinkedData
					.getLevels(restrictions);
			members = this.olap4jStatement.olap4jConnection.myLinkedData
					.getMembers(restrictions);

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Map<String, Integer> measuremap = Olap4ldLinkedDataUtil
				.getNodeResultFields(measures.get(0));

		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(dimensions.get(0));

		Map<String, Integer> hierarchymap = Olap4ldLinkedDataUtil
				.getNodeResultFields(hierarchies.get(0));

		Map<String, Integer> membermap = Olap4ldLinkedDataUtil
				.getNodeResultFields(members.get(0));

		// Projections from (cube, SlicesRollups, Dices, Projections)
		ArrayList<Node[]> projections = new ArrayList<Node[]>();

		// From measure set, fill projections
		boolean isFirst = true;
		// measureList is metadata describing requested measures
		for (Member member : this.measureList) {
			// Olap4ldMember member = (Olap4ldMember) member;
			// All multidimensional elements in MDX query first belong to the
			// global cube.

			// XXX: Should do that for all: Instead of transforming with
			// metaData.cube which is wrong, I
			// specifically ask
			// for that measure in that cube. If not existing, not adding.
			// List<Node[]> membernode = member
			// .transformMetadataObject2NxNodes(metaData.cube);

			Node membernameuri = Olap4ldLinkedDataUtil.convertMDXtoURI(member
					.getUniqueName());

			if (isFirst) {
				isFirst = false;
				projections.add(measures.get(0));
			}

			for (Node[] measure : measures) {
				// For measure, we cannot be sure about the encoding.
				if (measure[measuremap.get("?MEASURE_UNIQUE_NAME")].toString()
						.equals(membernameuri.toString())) {

					projections.add(measure);
				}
			}
		}

		// If projections empty (only contains header), return null since cube
		// will be "empty".

		if (projections.size() == 1) {
			return null;
		}

		LogicalOlapOp projection = new ProjectionOp(startop, projections);

		// Dice operator
		// XXX: Note: For now, the RollUp operator needs to be higher than Dice
		// operator
		// XXX: We only consider filterAxis for dice, for now.

		// Watch out: We do not want to have measure positions in the dice,
		// since measures are projected

		List<Node[]> newfilterAxisSignature = new ArrayList<Node[]>();
		List<List<Node[]>> newfilterAxisPositions = new ArrayList<List<Node[]>>();

		// With first position we create signature to be diced but only if diced
		// members available
		if (!filterAxis.positions.isEmpty()) {
			List<Member> signaturemembers = filterAxis.positions.get(0)
					.getMembers();
			first = true;
			for (Member member : signaturemembers) {
				Olap4ldMember olapmember = (Olap4ldMember) member;
				// No measures (from global cube)
				Olap4ldHierarchy hierarchy = olapmember.getHierarchy();
				if (!hierarchy.getUniqueName().equals(
						Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					if (first) {
						first = false;

						newfilterAxisSignature.add(hierarchies.get(0));
					}
					// Now, we need to find this hierarchy from the global cube
					// in the local cube.

					// If we do not find it, the resulting cube will be empty so
					// we can return null.
					// Yes, every cube that may not fit, needs to be "removed"
					// from the query plan.
					boolean containedInCube = false;
					Node hierarchynameuri = Olap4ldLinkedDataUtil
							.convertMDXtoURI(hierarchy.getUniqueName());
					for (Node[] ahierarchy : hierarchies) {
						if (ahierarchy[hierarchymap
								.get("?HIERARCHY_UNIQUE_NAME")]
								.equals(hierarchynameuri)) {

							/*
							 * The problem is that we should take the dataset
							 * specific hierarchy and not the hierarchy of the
							 * first member (which in our example is from
							 * gesis).
							 * 
							 * The problem is that we will have only one
							 * consolidated member from a consolidated hierarchy
							 * etc.
							 * 
							 * We then need to find the respective member and
							 * hierarchy in the original dataset.
							 * 
							 * Currently, I will probably not find the hierarchy
							 * for the dataset, since the hierarchy will have
							 * the name of another datasets dimension.
							 * 
							 * Thus, we also replace the single dataset
							 * identifiers with canonical values.
							 */

							newfilterAxisSignature.add(ahierarchy);
							containedInCube = true;
						}
					}
					// If hierarchy is not contained in dimensions of cube, cube
					// will be "empty" (since dice on that dimension would not
					// make sense since it is ALL)
					if (!containedInCube) {
						return null;
					}
				}
			}
		}
		// With all, we create newfilterAxisPositions
		for (Position position2 : filterAxis.positions) {
			ArrayList<Node[]> newMembers = new ArrayList<Node[]>();
			List<Member> filterpositionmembers = position2.getMembers();
			boolean first1 = true;
			for (Member member : filterpositionmembers) {
				Olap4ldMember olapmember = (Olap4ldMember) member;

				// No measures
				if (member.getMemberType() != Member.Type.MEASURE) {

					List<Node[]> membernodes = olapmember
							.transformMetadataObject2NxNodes(metaData.cube);

					if (first1) {
						first1 = false;
						newMembers.add(membernodes.get(0));
					}

					newMembers.add(membernodes.get(1));
				}
			}
			// Only if not empty (apart from header), we create and add the
			// position
			if (newMembers.size() != 1) {
				// Ordinal important?
				// Olap4ldPosition aPosition = new Olap4ldPosition(newMembers,
				// position2.getOrdinal());
				newfilterAxisPositions.add(newMembers);
			}
		}

		// If no dice, no dice
		LogicalOlapOp dice;
		if (newfilterAxisSignature.isEmpty()
				|| newfilterAxisSignature.size() == 1) {
			dice = projection;
		} else {

			// Dices from (cube, SlicesRollups, Dices, Projections)
			dice = new DiceOp(projection, newfilterAxisSignature,
					newfilterAxisPositions);

		}

		// Slice operator

		// When going through the axes: Prepare groupbylist, a set of levels
		// Prepare list of levels (excluding Measures!)
		// Go through tuple get dimensions, if not measure

		// Here, instead of adding all dimensions, only add those that actually
		// are rolled-up.

		ArrayList<Node[]> rollupslevels = new ArrayList<Node[]>();
		ArrayList<Node[]> rollupshierarchies = new ArrayList<Node[]>();
		ArrayList<Node[]> notslicedhierarchies = new ArrayList<Node[]>();

		first = true;

		// Columns
		/*
		 * For now, we assume that for each hierarchy, we query for members of
		 * one specific level, only.
		 * 
		 * XXX Note, also members could be diced, here also, but we do not
		 * recognise it.
		 */
		Position position = axisList.get(0).getPositions().get(0);

		List<Member> positionmembers = position.getMembers();

		for (Member member : positionmembers) {
			if (member.getMemberType() != Member.Type.MEASURE) {
				Olap4ldHierarchy olaphierarchy = (Olap4ldHierarchy) member
						.getLevel().getHierarchy();
				Olap4ldLevel olaplevel = (Olap4ldLevel) member.getLevel();
				if (first) {
					// Add headers
					first = false;
					rollupslevels.add(olaplevel
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(0));
					rollupshierarchies.add(olaphierarchy
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(0));
					notslicedhierarchies.add(olaphierarchy
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(0));
				}

				// Only do that if actual roll-up for that, we need to
				// compare with max_level
				int level_number = olaplevel.getDepth();
				int max_level_number = olaphierarchy.getLevels().size();
				if (level_number < max_level_number) {

					rollupslevels.add(olaplevel
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(1));
					rollupshierarchies.add(olaphierarchy
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(1));
				}
				notslicedhierarchies.add(olaphierarchy
						.transformMetadataObject2NxNodes(metaData.cube).get(1));
			}
		}

		// Rows
		/*
		 * For now, we assume that for each hierarchy, we query for members of
		 * one specific level, only.
		 * 
		 * XXX Again, note, also members could be diced, here also, we do not
		 * recognise it.
		 */
		position = axisList.get(1).getPositions().get(0);

		positionmembers = position.getMembers();

		for (Member member : positionmembers) {
			if (member.getMemberType() != Member.Type.MEASURE) {
				Olap4ldHierarchy olaphierarchy = (Olap4ldHierarchy) member
						.getLevel().getHierarchy();
				Olap4ldLevel olaplevel = (Olap4ldLevel) member.getLevel();
				if (first) {
					// Add headers
					first = false;
					rollupslevels.add(olaplevel
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(0));
					rollupshierarchies.add(olaphierarchy
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(0));
					notslicedhierarchies.add(olaphierarchy
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(0));
				}

				// Only do that if actual roll-up for that, we need to
				// compare with max_level
				int level_number = olaplevel.getDepth();
				int max_level_number = olaphierarchy.getLevels().size();
				if (level_number < max_level_number) {

					rollupslevels.add(olaplevel
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(1));
					rollupshierarchies.add(olaphierarchy
							.transformMetadataObject2NxNodes(metaData.cube)
							.get(1));
				}
				notslicedhierarchies.add(olaphierarchy
						.transformMetadataObject2NxNodes(metaData.cube).get(1));
			}
		}

		// Check whether all notsliced hierarchies are in the cube
		for (Node[] hierarchy : notslicedhierarchies) {
			boolean containedincube = false;

			Node dimensionnameuri = hierarchy[hierarchymap
					.get("?DIMENSION_UNIQUE_NAME")];

			for (Node[] aDimension : dimensions) {
				if (aDimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(dimensionnameuri.toString())) {
					containedincube = true;
				}
			}

			if (!containedincube) {
				// If notsliceddimension is not in cube, resulting cube will be
				// "empty".
				return null;
			}
		}

		// Find out which Dimensions to slice
		// Those that are not mentioned in the axes.
		List<Node[]> slicedDimensions = new ArrayList<Node[]>();
		// NamedList<Dimension> realdimensions = metaData.cube.getDimensions();

		// No first necessary since we go through realdimensions
		// Just take first.
		slicedDimensions.add(dimensions.get(0));
		for (Node[] realdimension : dimensions) {

			if ((realdimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
					.toString()
					.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME))) {
				continue;
			}

			boolean notsliced = false;

			// For every dimension, check whether in not sliced.
			for (Node[] notslicedhierarchy : notslicedhierarchies) {

				Map<String, Integer> notslicedhierarchymap = Olap4ldLinkedDataUtil
						.getNodeResultFields(notslicedhierarchies.get(0));

				if (notslicedhierarchy[notslicedhierarchymap
						.get("?DIMENSION_UNIQUE_NAME")].toString().equals(
						realdimension[dimensionmap
								.get("?DIMENSION_UNIQUE_NAME")].toString())) {
					notsliced = true;
				}
			}

			if (notsliced) {
				continue;
			} else {
				slicedDimensions.add(realdimension);
			}
		}

		// Test, slicedDimensions and

		// Slices part of SlicesRollups from (cube, SlicesRollups, Dices,
		// Projections)
		LogicalOlapOp slice = new SliceOp(dice, slicedDimensions);

		// RollupsPart of SlicesRollups from (cube, SlicesRollups, Dices,
		// Projections)
		// Rollup contains only hierarchies and levels that are actually higher
		// than Base level.
		LogicalOlapOp rollup = new RollupOp(slice, rollupshierarchies,
				rollupslevels);

		// Create indices for dimensions

		dimensionindicesList = new ArrayList<Integer>();

		for (Node[] dimension : dimensions) {
			// Check what number
			Measure measure = null;
			int index = 0;
			for (int i = 0; i < getAxes().size(); i++) {
				// Assume that first position tells us everything
				position = getAxes().get(i).getPositions().get(0);
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

						// If dimension == member.dimension then store
						// Add index in correct ordering.
						Node memberdimension = Olap4ldLinkedDataUtil
								.convertMDXtoURI(member.getDimension()
										.getUniqueName());
						if (memberdimension.equals(dimension[dimensionmap
								.get("?DIMENSION_UNIQUE_NAME")])) {
							dimensionindicesList.add(index);
						}
						// Count index
						index++;
					}
				}
			}

		}

		Olap4ldUtil._log.info("Finished creating " + rollup + " query plan.");

		return rollup;
	}

	/**
	 * 
	 * The purpose of this method is to store the results of an analytical query
	 * in a way so that getCell(...) can be evaluated.
	 * 
	 * Basically, we have a pivot table that describes observations from a
	 * multidimensional dataset. For each observation, we have a unique ordering
	 * of dimensions, and a unique combination of members of these dimensions.
	 * 
	 * @param olapQueryResult
	 */
	private void cacheDataFromOlapQuery(List<Node[]> olapQueryResult) {

		// Now, insert into hash map

		/*
		 * We cannot directly fill the cellMap, since there is no direct
		 * correspondence between the result and the "pivot" table: Per result,
		 * we may have several measures given. We may have missing results
		 * (sparseity).
		 * 
		 * Every result gives the measures for a specific cell.
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
			 * We now create a hash map which consist of: 1) Numeric
			 * representation of concatenation of all dimension members 2) An
			 * array with the values for each measure, ordered.
			 */

			int slicesRollupsSize = 0;
			for (CellSetAxisMetaData metadata : metaData.getAxesMetaData()) {
				List<Hierarchy> hierarchies = metadata.getHierarchies();
				for (Hierarchy hierarchy : hierarchies) {
					if (hierarchy.getName().equals(
							Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
						// We are only interested in the dimension columns.
						;
					} else {
						slicesRollupsSize++;
					}
				}

			}

			for (int i = 0; i < slicesRollupsSize; i++) {
				concatNr += node[i].toString();
			}

			// No header
			String[] valueArray = new String[measureList.size()];

			// No header
			for (int e = slicesRollupsSize; e < slicesRollupsSize
					+ measureList.size(); e++) {

				String nodevalue = node[e].toString();
				// For readability reasons, if numeric value, we round to to two
				// decimal.
				final String Digits = "(\\p{Digit}+)";
				final String HexDigits = "(\\p{XDigit}+)";
				// an exponent is 'e' or 'E' followed by an optionally
				// signed decimal integer.
				final String Exp = "[eE][+-]?" + Digits;
				final String fpRegex = ("[\\x00-\\x20]*" + // Optional leading
															// "whitespace"
						"[+-]?(" + // Optional sign character
						"NaN|" + // "NaN" string
						"Infinity|" + // "Infinity" string

						// A decimal floating-point string representing a finite
						// positive
						// number without a leading sign has at most five basic
						// pieces:
						// Digits . Digits ExponentPart FloatTypeSuffix
						//
						// Since this method allows integer-only strings as
						// input
						// in addition to strings of floating-point literals,
						// the
						// two sub-patterns below are simplifications of the
						// grammar
						// productions from the Java Language Specification, 2nd
						// edition, section 3.10.2.

						// Digits ._opt Digits_opt ExponentPart_opt
						// FloatTypeSuffix_opt
						"(((" + Digits + "(\\.)?(" + Digits + "?)(" + Exp
						+ ")?)|" +

						// . Digits ExponentPart_opt FloatTypeSuffix_opt
						"(\\.(" + Digits + ")(" + Exp + ")?)|" +

						// Hexadecimal strings
						"((" +
						// 0[xX] HexDigits ._opt BinaryExponent
						// FloatTypeSuffix_opt
						"(0[xX]" + HexDigits + "(\\.)?)|" +

						// 0[xX] HexDigits_opt . HexDigits BinaryExponent
						// FloatTypeSuffix_opt
						"(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

						")[pP][+-]?" + Digits + "))" + "[fFdD]?))" + "[\\x00-\\x20]*");// Optional
																						// trailing
																						// "whitespace"

				if (Pattern.matches(fpRegex, nodevalue)) {
					Double doubleValue = Double.valueOf(nodevalue); // Will not
					// throw
					// NumberFormatException

					try {
						DecimalFormat twoDForm = new DecimalFormat("#.##");
						doubleValue = Double.valueOf(twoDForm
								.format(doubleValue));

					} catch (NumberFormatException e1) {

					}

					nodevalue = doubleValue.toString();
				}

				valueArray[e - slicesRollupsSize] = nodevalue;
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
		// if (DEBUG) {
		// System.out.println(LdOlap4jUtil.toString(doc, true));
		// }
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
							+ Olap4ldUtil.prettyPrint(fault));
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

		if (olap4jStatement instanceof Olap4ldPreparedStatement) {
			this.metaData = ((Olap4ldPreparedStatement) olap4jStatement).cellSetMetaData;
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
		final Map<String, Olap4ldMember> memberMap = new HashMap<String, Olap4ldMember>();
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
			final Olap4ldCellSetAxis cellSetAxis = new Olap4ldCellSetAxis(this,
					axis, Collections.unmodifiableList(positions));
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
					Olap4ldMemberBase member = memberMap.get(uname);
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
						Olap4ldCellSetMemberProperty property = ((Olap4ldCellSetAxisMetaData) cellSetAxis
								.getAxisMetaData()).lookupProperty(
								hierarchyName, childNode.getLocalName());
						if (property != null) {
							String value = childNode.getTextContent();
							propertyValues.put(property, value);
						}
					}
					if (!propertyValues.isEmpty()) {
						member = new Olap4ldPositionMember(member,
								propertyValues);
					}
					members.add(member);
				}
				positions.add(new Olap4ldPosition(members, positions.size()));
			}
		}

		// If XMLA did not return a filter axis, it means that the WHERE clause
		// evaluated to zero tuples. (If the query had no WHERE clause, it
		// would have evaluated to a single tuple with zero positions.)
		if (filterAxis == null) {
			filterAxis = new Olap4ldCellSetAxis(this, Axis.FILTER,
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
			cellMap.put(cellOrdinal, new Olap4ldCell(this, cellOrdinal, value,
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
				return Olap4ldUtil.intElement(cell, "Value");
			} else if (type.equals("xsd:integer")) {
				return Olap4ldUtil.integerElement(cell, "Value");
			} else if (type.equals("xsd:double")) {
				return Olap4ldUtil.doubleElement(cell, "Value");
			} else if (type.equals("xsd:float")) {
				return Olap4ldUtil.floatElement(cell, "Value");
			} else if (type.equals("xsd:long")) {
				return Olap4ldUtil.longElement(cell, "Value");
			} else if (type.equals("xsd:boolean")) {
				return Olap4ldUtil.booleanElement(cell, "Value");
			} else {
				return Olap4ldUtil.stringElement(cell, "Value");
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
	private Olap4ldCellSetMetaData createMetaData(Element root)
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
	@SuppressWarnings("unused")
	@Deprecated
	private Olap4ldCube lookupCube(Olap4ldDatabaseMetaData databaseMetaData,
			String cubeName) throws OlapException {
		for (Catalog catalog : databaseMetaData.olap4jConnection
				.getOlapCatalogs()) {
			for (Schema schema : catalog.getSchemas()) {
				for (Cube cube : schema.getCubes()) {
					if (cubeName.equals(cube.getName())) {
						return (Olap4ldCube) cube;
					}
					if (cubeName.equals("[" + cube.getName() + "]")) {
						return (Olap4ldCube) cube;
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
	@Deprecated
	private Hierarchy lookupHierarchy(Olap4ldCube cube, String hierarchyName)
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
	@Deprecated
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

		if (!areCellsPopulated) {
			try {
				populateCells();
			} catch (OlapException e) {
				// TODO It would be nicer to have proper OlapException given.
				e.printStackTrace();
			}
		}

		// We collect the members in an ArrayList
		List<String> concatNrs = new ArrayList<String>();
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
					concatNrs.add(Olap4ldLinkedDataUtil.convertMDXtoURI(
							member.getUniqueName()).toString());
				}
			}
		}

		/*
		 * We need to know the index of each dimension
		 */
		String concatNr = "";
		for (int i = 0; i < dimensionindicesList.size(); i++) {
			if (dimensionindicesList.get(i) != null) {
				concatNr += concatNrs.get(dimensionindicesList.get(i));
			}
		}

		/*
		 * We need to know the number of the measure
		 */
		int index = 0;
		// No header
		for (int i = 0; i < measureList.size() && measure != null; i++) {

			// Problem in case the same measure is used by several cubes, then
			// we always use the first.
			if (measureList.get(i).getUniqueName()
					.equals(measure.getUniqueName())) {
				// No header
				index = i;
				break;
			}
		}

		/*
		 * Now, we have the members and the measure index
		 */
		String[] hashedCells = newValueMap.get(concatNr.hashCode());

		// We should be using property values since XMLA requires them:

		// Template: Now, we fill the cells
		// final Element cellDataNode = findChild(root, MDDATASET_NS,
		// "CellData");
		// for (Element cell : findChildren(cellDataNode, MDDATASET_NS, "Cell"))
		// {
		// propertyValues.clear();
		// final int cellOrdinal = Integer.valueOf(cell
		// .getAttribute("CellOrdinal"));
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
		// cellMap.put(cellOrdinal, new Olap4ldCell(this, cellOrdinal, value,
		// formattedValue, propertyValues));
		// }

		// We need to add properties
		Map<Property, Object> propertyValues = new HashMap<Property, Object>();

		if (hashedCells != null && hashedCells[index] != null) {

			propertyValues.put(Property.StandardCellProperty.FORMATTED_VALUE,
					hashedCells[index]);
			propertyValues.put(Property.StandardCellProperty.VALUE,
					hashedCells[index]);
			propertyValues.put(Property.StandardCellProperty.FORMAT_STRING,
					null);

			return new Olap4ldCell(this, coordinatesToOrdinal(coordinates),
					hashedCells[index], hashedCells[index], propertyValues);
		} else {
			return new Olap4ldCell(this, coordinatesToOrdinal(coordinates), "",
					"", Collections.<Property, Object> emptyMap());
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
	@SuppressWarnings("unused")
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
	private static class XmlaOlap4jSurpriseMember implements Olap4ldMemberBase {
		private final Olap4ldCellSet cellSet;
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
		XmlaOlap4jSurpriseMember(Olap4ldCellSet cellSet, Level level,
				Hierarchy hierarchy, int lnum, String caption, String uname) {
			this.cellSet = cellSet;
			this.level = level;
			this.hierarchy = hierarchy;
			this.lnum = lnum;
			this.caption = caption;
			this.uname = uname;
		}

		public final Olap4ldCube getCube() {
			return cellSet.metaData.cube;
		}

		public final Olap4ldConnection getConnection() {
			return getCatalog().olap4jDatabaseMetaData.olap4jConnection;
		}

		public final Olap4ldCatalog getCatalog() {
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

