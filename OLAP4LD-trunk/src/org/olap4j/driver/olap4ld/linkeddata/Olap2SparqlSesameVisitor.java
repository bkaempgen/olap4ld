package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * Converts from relational algebra plans to physical access plans.
 * 
 * Assumes a pre-filled repository on which to compute OLAP-to-SPARQL algorithm.
 * 
 * Also, assumes that query plan for each type of operator only contains one instance (since
 * attributes are simply overridden).
 * 
 * @author benedikt
 */
public class Olap2SparqlSesameVisitor implements
		LogicalOlapOperatorQueryPlanVisitor {

	// Input to OLAP operator
	private List<Node[]> slicedDimensions = new ArrayList<Node[]>();
	private List<Node[]> rollupslevels = new ArrayList<Node[]>();
	private List<Node[]> rollupshierarchies = new ArrayList<Node[]>();
	private List<List<Node[]>> membercombinations = new ArrayList<List<Node[]>>();
	private List<Node[]> hierarchysignature = new ArrayList<Node[]>();
	private List<Node[]> projectedMeasures = new ArrayList<Node[]>();

	// For the moment, we know the repo (we could wrap it also)
	private EmbeddedSesameEngine engine;
	private PhysicalOlapIterator _root;

	/**
	 * Constructor.
	 * 
	 * Assumes that repo is filled, already. 
	 * 
	 * @param repo
	 *            A repository filled with all available cubes.
	 * 
	 */
	public Olap2SparqlSesameVisitor(EmbeddedSesameEngine engine) {
		this.engine = engine;
	}

	/**
	 * For now, we assume exactly two children.
	 */
	public void visit(DrillAcrossOp op) throws QueryException {
		throw new UnsupportedOperationException("Drill-Across is not supported by the Olap2SparqlSesameVisitor!");
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
		// We assume that slice is called only once per query plan.
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
		LogicalOlapOperatorQueryPlanVisitor r2a = new Olap2SparqlSesameDerivedDatasetVisitor(this.engine);
		o.accept(r2a);
		this._root = (PhysicalOlapIterator) r2a.getNewRoot();
	}

	@Override
	public void visit(ConvertCubeOp op) throws QueryException {
		
		LogicalOlapOperatorQueryPlanVisitor r2a = new Olap2SparqlSesameDerivedDatasetVisitor(this.engine);
		op.accept(r2a);
		this._root = (PhysicalOlapIterator) r2a.getNewRoot();
		
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

		try {
		Restrictions restrictions = new Restrictions();

		List<Node[]> measures = _root.getMeasures(restrictions);
		List<Node[]> dimensions = _root.getDimensions(restrictions);
		//List<Node[]> hierarchies = _root.getHierarchies(restrictions);
		List<Node[]> levels = _root.getLevels(restrictions);
		//List<Node[]> members = _root.getMembers(restrictions);

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
					contained = true;
					slicesrollups.add(rolluplevel);
					
					/*
					 * This is how you get levelHeight: HIERARCHY_MAX_LEVEL_NUMBER - LEVEL_NUMBER.
					 * OR: Hierarchy.maxdepth - Level.depth.
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
		this._root = new Olap2SparqlAlgorithmSesameIterator(this._root,
				this.engine, slicesrollups,
				levelheights, projections, membercombinations,
				hierarchysignature);

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return _root;
	}
}
