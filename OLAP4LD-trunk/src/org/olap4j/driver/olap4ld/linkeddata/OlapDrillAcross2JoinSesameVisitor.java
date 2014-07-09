//package org.olap4j.driver.olap4ld.linkeddata;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Converts from relational algebra plans to physical access plans.
// * 
// * Assumes repo to be pre-filled, already.
// * 
// * @author benedikt
// */
//public class OlapDrillAcross2JoinSesameVisitor implements
//		LogicalOlapOperatorQueryPlanVisitor {
//
//	List<PhysicalOlapIterator> iteratorlist;
//
//	// For the moment, we know the repo (we could wrap it also)
//	private EmbeddedSesameEngine engine;
//
//	/**
//	 * Constructor.
//	 * 
//	 * @param repo
//	 *            A repository filled with all available cubes.
//	 * 
//	 */
//	public OlapDrillAcross2JoinSesameVisitor(EmbeddedSesameEngine engine) {
//		this.engine = engine;
//		iteratorlist = new ArrayList<PhysicalOlapIterator>();
//	}
//
//	/**
//	 * For now, we assume exactly two children.
//	 */
//	public void visit(DrillAcrossOp op) throws QueryException {
//		// not supported in this implementation
//
//		if (op.inputop1 instanceof DrillAcrossOp) {
//			// Do nothing, since we will go through the entire tree, anyway.
//			//op.inputop1.accept(this);
//		} else if (op.inputop1 instanceof ConvertCubeOp || op.inputop1 instanceof BaseCubeOp) {
//			LogicalOlapOperatorQueryPlanVisitor r2a = new Olap2SparqlSesameDerivedDatasetVisitor(this.engine);
//			op.inputop1.accept(r2a);
//			iteratorlist.add((PhysicalOlapIterator) r2a.getNewRoot());
//			
//		} else {
//			// Now, we can assume that the subtree does not contain Drill-Across.
//			// Therefore, we can directly create an iterator from that subtree.
//			LogicalOlapOperatorQueryPlanVisitor r2a = new Olap2SparqlSesameVisitor(
//					this.engine);
//			op.inputop1.accept(r2a);
//			iteratorlist.add((PhysicalOlapIterator) r2a.getNewRoot());
//		}
//
//		if (op.inputop2 instanceof DrillAcrossOp) {
//			// Do nothing, since we will go through the entire tree, anyway.
//			//op.inputop1.accept(this);
//		} else if (op.inputop2 instanceof ConvertCubeOp || op.inputop2 instanceof BaseCubeOp) {
//			LogicalOlapOperatorQueryPlanVisitor r2a = new Olap2SparqlSesameDerivedDatasetVisitor(this.engine);
//			op.inputop2.accept(r2a);
//			iteratorlist.add((PhysicalOlapIterator) r2a.getNewRoot());
//			
//		} else {
//			// Now, we can assume that the subtree does not contain Drill-Across.
//			// Therefore, we can directly create an iterator from that subtree.
//			LogicalOlapOperatorQueryPlanVisitor r2a = new Olap2SparqlSesameVisitor(
//					this.engine);
//			// Transform into physical query plan
//			op.inputop2.accept(r2a);
//			iteratorlist.add((PhysicalOlapIterator) r2a.getNewRoot());
//		}
//	}
//
//	@Override
//	public void visit(Object op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//	}
//
//	@Override
//	public void visit(ConvertCubeOp op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//
//	}
//
//	@Override
//	public void visit(RollupOp op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//
//	}
//
//	@Override
//	public void visit(SliceOp op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//
//	}
//
//	@Override
//	public void visit(DiceOp op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//
//	}
//
//	@Override
//	public void visit(ProjectionOp op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//
//	}
//
//	@Override
//	public void visit(BaseCubeOp op) throws QueryException {
//		// Do nothing, we are only interested in Drill-Across operators
//
//	}
//
//	@Override
//	public Object getNewRoot() throws QueryException {
//
//		// We assume at least one iterator
//		PhysicalOlapIterator rootiterator = iteratorlist.get(0);
//
//		// We have to nest this iterator.
//		for (int i = 1; i < iteratorlist.size(); i++) {
//
//			rootiterator = new DrillAcrossNestedLoopJoinSesameIterator(
//					iteratorlist.get(i), rootiterator);
//		}
//
//		return rootiterator;
//	}
//}
