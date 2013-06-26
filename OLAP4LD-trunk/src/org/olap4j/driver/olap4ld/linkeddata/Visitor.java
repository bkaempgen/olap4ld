// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;

/**
 * A visitor has to implement this interface.
 * 
 * @author aharth
 */
public interface Visitor {
    /**
     * Callback method that is performed on a visit.
     *	
     * @param op usually this
     * @param obj a tree node that is the new tree's parent for op
     */
    public void visit(Object op) throws QueryException;
    
    /**
     * After a visitor has visited all nodes for a transformation of a tree,
     * it here returns the new root of the transformed tree.
     * 
     * @return
     * @throws QueryException
     */
    public Object getNewRoot() throws QueryException;
}
