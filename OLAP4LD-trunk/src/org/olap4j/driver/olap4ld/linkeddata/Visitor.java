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
    
    public Object getNewRoot() throws QueryException;
}
