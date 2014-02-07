// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;



/**
 * Encapsulates a physical access plan.
 * 
 * @author aharth
 */
public class PhysicalOlapQueryPlan {
	// root of the plan
	PhysicalOlapIterator _root = null;

	/**
	 * Constructor. XXX FIXME lexicon shouldn't be here at all
	 */
	public PhysicalOlapQueryPlan(PhysicalOlapIterator root) {
		_root = root;
	}

	/**
	 * Add a rule to the program.
	 */
	public void setRoot(PhysicalOlapIterator root) {
		_root = root;
	}

	/**
	 * Get the iterator. This iterator returns query results with byte arrays that
	 * can be cast to Node[].
	 */
	public PhysicalOlapIterator getIterator() {
		return _root;
	}

	/**
	 * String representation.
	 */
	public String toString() {
		return _root.toString();
	}

	/**
	 * Entry method that visits all nodes recursively.
	 */
	public Object visitAll(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		_root.accept(v);

		return v.getNewRoot();
	}

	public String toXML() throws QueryException {
		// Exec2XML acc2xml = new Exec2XML();
		//
		// Olap4ldUtil._log.fine("root is " + _root);
		// _root.accept(acc2xml);
		//
		// return (String)acc2xml.getNewRoot();
		return "";
	}
}
