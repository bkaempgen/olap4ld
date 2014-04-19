// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;

/**
 * A visitor has to implement this interface.
 * 
 * @author aharth, bkaempgen
 */
public interface LogicalOlapOperatorQueryPlanVisitor extends Visitor {
    /**
     * Callback method that is performed on a visit.
     *	
     * @param op usually this
     * @param obj a tree node that is the new tree's parent for op
     */
	public void visit(ConvertCubeOp op) throws QueryException;
	public void visit(DrillAcrossOp op) throws QueryException;
    public void visit(RollupOp op) throws QueryException;
    public void visit(SliceOp op) throws QueryException;
    public void visit(DiceOp op) throws QueryException;
    public void visit(ProjectionOp op) throws QueryException;
    public void visit(BaseCubeOp op) throws QueryException;
    
    /**
     * After a visitor has visited all nodes for a transformation of a tree,
     * it here returns the new root of the transformed tree.
     * 
     * @return
     * @throws QueryException
     */
    public Object getNewRoot() throws QueryException;
}
