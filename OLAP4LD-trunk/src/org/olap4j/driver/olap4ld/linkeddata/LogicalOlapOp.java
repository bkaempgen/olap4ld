// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;

/**
 * This is an operator in the relational plan (sometimes called
 * "logical" plan) that can be translated to an access plan 
 * ("physical" plan) la
 * 
 * @author aharth
 */
public interface LogicalOlapOp { //TreeNode {
    /**
     * String representation of the operation.
     */
    public String toString();

    /**
     * Vistor pattern: perform visit operation
     * @param v vistor object
     * @param obj node for the new tree
     */
    public void accept(Visitor v) throws QueryException;
}