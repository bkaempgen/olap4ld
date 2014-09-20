package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

public class LogicalToPhysical {

	// Input to OLAP operator
	private List<Node[]> slicedDimensions = new ArrayList<Node[]>();
	private List<Node[]> rollupslevels = new ArrayList<Node[]>();
	private List<Node[]> rollupshierarchies = new ArrayList<Node[]>();
	private List<List<Node[]>> membercombinations = new ArrayList<List<Node[]>>();
	private List<Node[]> hierarchysignature = new ArrayList<Node[]>();
	private List<Node[]> projectedMeasures = new ArrayList<Node[]>();
	private EmbeddedSesameEngine engine;

	public LogicalToPhysical(EmbeddedSesameEngine embeddedSesameEngine) {
		this.engine = embeddedSesameEngine;
	}

	/**
	 * 
	 * * Design decisions The LogicalOperators only get as input a cube and the
	 * parameters, but not specifically the metadata of the cube.
	 * 
	 * The PhysicalIterators get as input another iterator and the parameters,
	 * but also the metadata of the iterator. I create PhysicalIterators for
	 * metadata, they get as input the repo and the parameters (restrictions).
	 * Every LogicalOperator could be wrapped, get as input the identifier of a
	 * cube and the parameters and return a new identifier.
	 * 
	 * Mioeur2eur is another LogicalOperator that could get wrapped, get as
	 * input the identifier of a cube and the parameters and return a new
	 * identifier. Mioeur2eur would also include PhysicalIterators. The
	 * Mioeur2eur iterator would get as input another iterator,
	 * BaseCubeIterator, and return the conversion. Example:
	 * http://olap4ld.googlecode.com/dic/aggreg95#00;
	 * http://olap4ld.googlecode.com/dic/geo#US;
	 * http://olap4ld.googlecode.com/dic/indic_na#VI_PPS_EU28_HAB; 2012; 149;
	 * 
	 */

	/**
	 * 
	 * This method recursively creates a physical query plan from a logical one.
	 * 
	 * @param node
	 * @return
	 */
	public PhysicalOlapIterator compile(LogicalOlapOp node) {
		PhysicalOlapIterator theIterator = null;

		// If we have Drill-Across, we recursively compile and use nested loop.
		if (node instanceof DrillAcrossOp) {
			LogicalOlapOp inputop1 = ((DrillAcrossOp) node).inputop1;
			LogicalOlapOp inputop2 = ((DrillAcrossOp) node).inputop2;
			PhysicalOlapIterator iterator1;
			PhysicalOlapIterator iterator2;
			PhysicalOlapIterator intermediateIterator;

			/*
			 * Otherwise:
			 * 
			 * * We may have Slice/Dice/etc. over BaseCube -> No problem. * We
			 * may have Convert-Cube etc. over BaseCube -> No problem. * We may
			 * have Drill-Across over BaseCube -> Problem.
			 */
			if (inputop1 instanceof BaseCubeOp) {
				intermediateIterator = compile(inputop1);

				iterator1 = createOlap2SparqlIterator(intermediateIterator);
			} else {
				iterator1 = compile(inputop1);
			}
			if (inputop2 instanceof BaseCubeOp) {
				intermediateIterator = compile(inputop2);

				iterator2 = createOlap2SparqlIterator(intermediateIterator);
			} else {
				iterator2 = compile(inputop2);
			}
			theIterator = new DrillAcrossNestedLoopJoinSesameIterator(
					iterator1, iterator2);
		}

		// The thing is, we may have SPARQL2OLAP over Convert-Cube or over
		// Base-Cube.
		if (node instanceof RollupOp || node instanceof SliceOp
				|| node instanceof DiceOp || node instanceof ProjectionOp) {

			LogicalOlapOp inputop = null;

			if (node instanceof RollupOp) {
				// For rollup,
				RollupOp ro = (RollupOp) node;

				// Question is, are there sliced dimensions contained, already?
				// No. Only
				// rolled-up dimensions.
				// Are there dimensions mentioned, that are not roll-up but stay
				// on the
				// lowest level? Should be.
				this.rollupslevels = ro.rollupslevels;
				this.rollupshierarchies = ro.rollupshierarchies;

				inputop = ro.inputOp;
			}

			if (node instanceof SliceOp) {
				SliceOp so = (SliceOp) node;

				// we do not need to do anything with SliceOp
				// We assume that slice is called only once per query plan.
				this.slicedDimensions = so.slicedDimensions;

				inputop = so.inputOp;
			}

			if (node instanceof DiceOp) {
				DiceOp dop = (DiceOp) node;

				// Question: Is that corresponding to OLAP-2-SPARQL algorithm?
				// Yes.
				this.membercombinations = dop.membercombinations;
				// Hierarchy signature needed for max_level_height
				this.hierarchysignature = dop.hierarchysignature;

				inputop = dop.inputOp;
			}

			if (node instanceof ProjectionOp) {
				ProjectionOp po = (ProjectionOp) node;

				this.projectedMeasures = po.projectedMeasures;

				inputop = po.inputOp;
			}

			// Check inputop.
			if (inputop instanceof BaseCubeOp
					|| inputop instanceof ConvertCubeOp) {
				PhysicalOlapIterator iterator1 = compile(inputop);

				theIterator = createOlap2SparqlIterator(iterator1);
			} else {
				theIterator = compile(inputop);
			}
		}

		if (node instanceof ConvertCubeOp) {

			ConvertCubeOp so = (ConvertCubeOp) node;

			if (so.inputOp2 == null) {

				PhysicalOlapIterator iterator = compile(so.inputOp1);
				theIterator = new ConvertSparqlDerivedDatasetIterator(engine,
						iterator, null, so.conversioncorrespondence);

			} else if (so.inputOp1 == so.inputOp2) {
				// If both operators are the same, we can reuse the iterator.
				// Unfortunately, this does not work for further nested equal
				// operators.
				// If inside a logical olap query plan the same function is run
				// on
				// the same dataset, we
				// still have a problem that it is run twice. we could have a
				// hashmap storing
				// previous operators and reusing them if needed.
				PhysicalOlapIterator iterator = compile(so.inputOp1);
				theIterator = new ConvertSparqlDerivedDatasetIterator(engine,
						iterator, iterator, so.conversioncorrespondence);
			} else {
				PhysicalOlapIterator iterator1 = compile(so.inputOp1);
				PhysicalOlapIterator iterator2 = compile(so.inputOp2);

				theIterator = new ConvertSparqlDerivedDatasetIterator(engine,
						iterator1, iterator2, so.conversioncorrespondence);
			}

		}

		if (node instanceof BaseCubeOp) {
			
			theIterator = new BaseCubeSparqlDerivedDatasetIterator(engine,
					((BaseCubeOp) node).dataseturi);
			
			// XXX: Not sure whether that will make problems.
			// theIterator = createOlap2SparqlIterator(theIterator);
			
		}

		return theIterator;

	}

	private PhysicalOlapIterator createOlap2SparqlIterator(
			PhysicalOlapIterator iterator1) {

		PhysicalOlapIterator theIterator = null;

		try {
			Restrictions restrictions = new Restrictions();

			List<Node[]> measures = iterator1.getMeasures(restrictions);
			List<Node[]> dimensions = iterator1.getDimensions(restrictions);
			// List<Node[]> hierarchies =
			// iterator1.getHierarchies(restrictions);
			List<Node[]> levels = iterator1.getLevels(restrictions);
			// List<Node[]> members = iterator1.getMembers(restrictions);

			// Build slicesrollups
			Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
					.getNodeResultFields(dimensions.get(0));
			Map<String, Integer> levelmap = Olap4ldLinkedDataUtil
					.getNodeResultFields(levels.get(0));

			// First, we have to create slicesrollups
			List<Node[]> slicesrollups = new ArrayList<Node[]>();
			slicesrollups.add(levels.get(0));

			List<Integer> levelheights = new ArrayList<Integer>();
			// Header
			levelheights.add(-1);

			/*
			 * Note, since we start from the list of dimensions of the cube, the
			 * result dimensions will be in the ordering of the cube (and not
			 * the MDX query).
			 */

			// Find dimensions not in sliced and not in rolluplevel (that are
			// rolled-up).
			List<Node[]> basedimensions = new ArrayList<Node[]>();
			// Header
			basedimensions.add(dimensions.get(0));

			boolean first = true;
			// We check all dimensions of the input dataset
			for (Node[] dimension : dimensions) {

				if (first) {
					first = false;
					continue;
				}

				// If in rollupslevels, add and continue.
				boolean containedInRollupsLevels = false;
				for (int i = 1; i < rollupslevels.size(); i++) {

					Map<String, Integer> rollupshierarchiesmap = Olap4ldLinkedDataUtil
							.getNodeResultFields(rollupshierarchies.get(0));

					Node[] rolluplevel = rollupslevels.get(i);
					Node[] rollupshierarchy = rollupshierarchies.get(i);
					if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
							.toString().equals(
									rolluplevel[levelmap
											.get("?DIMENSION_UNIQUE_NAME")]
											.toString())) {
						containedInRollupsLevels = true;
						slicesrollups.add(rolluplevel);

						/*
						 * This is how you get levelHeight:
						 * HIERARCHY_MAX_LEVEL_NUMBER - LEVEL_NUMBER. OR:
						 * Hierarchy.maxdepth - Level.depth.
						 */
						Integer levelHeight = (new Integer(
								rollupshierarchy[rollupshierarchiesmap
										.get("?HIERARCHY_MAX_LEVEL_NUMBER")]
										.toString()) - new Integer(
								rolluplevel[levelmap.get("?LEVEL_NUMBER")]
										.toString()));
						levelheights.add(levelHeight);
						continue;
					}
				}
				boolean containedInSlicedDimensions = false;
				// We check whether a sliced dimension.
				for (Node[] sliceddimension : slicedDimensions) {
					if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
							.toString().equals(
									sliceddimension[dimensionmap
											.get("?DIMENSION_UNIQUE_NAME")]
											.toString())) {
						containedInSlicedDimensions = true;
					}
				}
				// As usual also check whether Measure dimension.
				if (!containedInRollupsLevels
						&& !containedInSlicedDimensions
						&& !dimension[dimensionmap
								.get("?DIMENSION_UNIQUE_NAME")]
								.toString()
								.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
					basedimensions.add(dimension);
				}
			}

			// Find lowest level of basedimensions and add
			first = true;
			for (Node[] basedimension : basedimensions) {
				if (first) {
					first = false;
					continue;
				}
				// Search for lowest level
				Node[] baselevel = null;
				first = true;
				for (Node[] level : levels) {
					if (first) {
						first = false;
						continue;
					}
					if (basedimension[dimensionmap
							.get("?DIMENSION_UNIQUE_NAME")].toString().equals(
							level[levelmap.get("?DIMENSION_UNIQUE_NAME")]
									.toString())) {
						if (baselevel == null) {
							baselevel = level;
						}
						int baselevelnumber = new Integer(
								baselevel[levelmap.get("?LEVEL_NUMBER")]
										.toString());
						int levelnumber = new Integer(
								level[levelmap.get("?LEVEL_NUMBER")].toString());

						if (baselevelnumber < levelnumber) {
							baselevel = level;
						}
					}
				}
				slicesrollups.add(baselevel);
				levelheights.add(0);
			}

			// Tests?
			// slicesrollups should contain a dimension for each apart from
			// slices.
			// Remember measure dimension that never gets sliced or rolled-up

			// Get number of non-measure dimensions
			int nonMeasureDimensions = dimensions.size() - 2;
			// Get size of sliced dimensions:
			int slicedDimensionsNumber;
			if (slicedDimensions.isEmpty()) {
				slicedDimensionsNumber = 0;
			} else {
				slicedDimensionsNumber = slicedDimensions.size() - 1;
			}

			int slicesrollupsshouldbesize = nonMeasureDimensions
					- slicedDimensionsNumber;

			if (slicesrollups.size() - 1 != slicesrollupsshouldbesize) {
				throw new UnsupportedOperationException(
						"Slicesrollups not properly created!");
			}

			// Create projections

			List<Node[]> projections;
			if (projectedMeasures.isEmpty()) {
				projections = measures;
			} else {
				projections = projectedMeasures;
			}

			// Currently, we should have retrieved the data, already, therefore,
			// we
			// only have one node.
			// We use the OLAP-2-SPARQL algorithm.
			theIterator = new Olap2SparqlAlgorithmSesameIterator(iterator1,
					this.engine, slicesrollups, levelheights, projections,
					membercombinations, hierarchysignature);

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return theIterator;
	}

}
