// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;

import java.util.Iterator;

/**
 * Interface for all Iterators that return byte arrays.
 */
public interface ExecIterator extends Iterator<Object> {
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
    public void accept(Visitor v) throws QueryException;
}
