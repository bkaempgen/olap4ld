// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;

import java.util.Iterator;
import java.util.List;

import org.olap4j.OlapException;
import org.semanticweb.yars.nx.Node;

/**
 * Interface for all Iterators that return byte arrays.
 */
public interface PhysicalOlapIterator extends Iterator<Object> {
	
	/**
	 * Initialize iterator.
	 */
	public void init() throws Exception;
		
    /**
     * String representation of the operation.
     */
    public String toString();
    
    /**
     * Close iterator.
     */
    public void close() throws Exception;

    /**
     * Vistor pattern: perform visit operation
     * @param v vistor object
     * @param obj node for the new tree
     */
    public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException;
    
    /**
     * Any iterator stores the schema of its output.
     * 
     * Interface same as for Linked Data Cubes Engine.
     * 
     * @param restrictions
     * @return
     * @throws OlapException
     */
	public List<Node[]> getCubes(Restrictions restrictions)
			throws OlapException;

	public List<Node[]> getDimensions(Restrictions restrictions)
			throws OlapException;

	public List<Node[]> getMeasures(Restrictions restrictions)
			throws OlapException;

	public List<Node[]> getHierarchies(Restrictions restrictions)
			throws OlapException;

	public List<Node[]> getLevels(Restrictions restrictions)
			throws OlapException;

	public List<Node[]> getMembers(Restrictions restrictions)
			throws OlapException;
}
