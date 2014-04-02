package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.openrdf.repository.sail.SailRepository;
import org.semanticweb.yars.nx.Node;

/**
 * Converts from relational algebra plans to physical access plans.
 * 
 * @author benedikt
 */
public class Olap2SparqlSesameVisitor implements
		LogicalOlapOperatorQueryPlanVisitor {

	// Input to OLAP operators
	private List<Node[]> cubes = new ArrayList<Node[]>();
	private List<Node[]> measures = new ArrayList<Node[]>();
	private List<Node[]> dimensions = new ArrayList<Node[]>();
	private List<Node[]> hierarchies;
	private List<Node[]> levels = new ArrayList<Node[]>();
	private List<Node[]> members;

	private List<Node[]> slicedDimensions = new ArrayList<Node[]>();
	private List<Node[]> rollupslevels = new ArrayList<Node[]>();
	private List<Node[]> rollupshierarchies = new ArrayList<Node[]>();
	private List<List<Node[]>> membercombinations = new ArrayList<List<Node[]>>();
	private List<Node[]> hierarchysignature = new ArrayList<Node[]>();
	private List<Node[]> projectedMeasures = new ArrayList<Node[]>();

	// For the moment, we know the repo (we could wrap it also)
	private SailRepository repo;

	/**
	 * Constructor.
	 * 
	 * @param repo
	 *            A repository filled with all available cubes.
	 * 
	 */
	public Olap2SparqlSesameVisitor(SailRepository repo) {
		this.repo = repo;
	}

	/**
	 * For now, we assume exactly two children.
	 */
	public void visit(DrillAcrossOp op) throws QueryException {
		// not supported in this implementation
	}

	public void visit(RollupOp o) throws QueryException {
		// For rollup,
		RollupOp so = (RollupOp) o;

		// Question is, are there sliced dimensions contained, already? No. Only
		// rolled-up dimensions.
		// Are there dimensions mentioned, that are not roll-up but stay on the
		// lowest level? Should be.
		this.rollupslevels = so.rollupslevels;
		this.rollupshierarchies = so.rollupshierarchies;
	}

	public void visit(SliceOp o) throws QueryException {
		SliceOp so = (SliceOp) o;

		// we do not need to do anything with SliceOp
		this.slicedDimensions = so.slicedDimensions;
	}

	public void visit(DiceOp o) throws QueryException {
		DiceOp dop = (DiceOp) o;

		// Question: Is that corresponding to OLAP-2-SPARQL algorithm? Yes.
		this.membercombinations = dop.membercombinations;
		// Hierarchy signature needed for max_level_height
		this.hierarchysignature = dop.hierarchysignature;
	}

	/**
	 * ProjectionOp is defined to remove all measures apart from those
	 * mentioned.
	 * 
	 * 
	 */
	public void visit(ProjectionOp o) throws QueryException {
		ProjectionOp po = (ProjectionOp) o;

		this.projectedMeasures = po.projectedMeasures;
	}

	public void visit(BaseCubeOp o) throws QueryException {
		BaseCubeOp so = (BaseCubeOp) o;

		// Cube gives us the URI of the dataset that we want to resolve
		this.cubes = so.cubes;
		this.measures = so.measures;
		this.dimensions = so.dimensions;
		this.hierarchies = so.hierarchies;
		this.levels = so.levels;
		// One problem could be that this may be a huge number of members.
		this.members = so.members;
	}

	@Override
	public void visit(ConvertContextOp op) throws QueryException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException(
				"visit(ConvertContextOp op) not implemented!");
	}

	@Override
	public void visit(Object op) throws QueryException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException(
				"visit(Object op) not implemented!");
	}

	/**
	 * After iteration, create new root. I want to have simple SPARQL query +
	 * load RDF to store.
	 */
	public Object getNewRoot() {

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
		 * result dimensions will be in the ordering of the cube (and not the
		 * MDX query).
		 */

		// Find dimensions not in sliced and not in rolluplevel.
		List<Node[]> basedimensions = new ArrayList<Node[]>();
		// Header
		basedimensions.add(dimensions.get(0));
		for (Node[] dimension : dimensions) {

			// If in rollupslevels, add and continue.
			boolean contained = false;
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
					slicesrollups.add(dimension);
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
			for (Node[] sliceddimension : slicedDimensions) {
				if (dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								sliceddimension[dimensionmap
										.get("?DIMENSION_UNIQUE_NAME")]
										.toString())) {
					contained = true;
				}
			}
			// As usual also check whether Measure dimension.
			if (!contained
					&& !dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
							.toString()
							.equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME)) {
				basedimensions.add(dimension);
			}
		}

		// Find lowest level of basedimensions and add
		boolean first = true;
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
				if (basedimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								level[levelmap.get("?DIMENSION_UNIQUE_NAME")]
										.toString())) {
					if (baselevel == null) {
						baselevel = level;
					}
					int baselevelnumber = new Integer(
							baselevel[levelmap.get("?LEVEL_NUMBER")].toString());
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
		// slicesrollups should contain a dimension for each apart from slices.
		// Remember measure dimension that never gets sliced or rolled-up
		int slicesrollupsshouldbesize = (dimensions.size() - 2)
				- (slicedDimensions.size() - 1);

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

		// Currently, we should have retrieved the data, already, therefore, we
		// only have one node.
		// We use the OLAP-2-SPARQL algorithm.
		PhysicalOlapIterator _root = new Olap2SparqlAlgorithmSesameIterator(
				repo, cubes, measures, dimensions, hierarchies, levels, members, slicesrollups,
				levelheights, projections, membercombinations,
				hierarchysignature);

		return _root;
	}
}
