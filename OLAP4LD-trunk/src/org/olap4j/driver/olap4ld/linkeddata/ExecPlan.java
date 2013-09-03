// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;


/**
 * Encapsulates a physical access plan.
 * 
 * @author aharth
 */
public class ExecPlan {
	// root of the plan
	ExecIterator _root = null;

	/**
	 * Constructor. XXX FIXME lexicon shouldn't be here at all
	 */
	public ExecPlan(ExecIterator root) {
		_root = root;
	}

	/**
	 * Add a rule to the program.
	 */
	public void setRoot(ExecIterator root) {
		_root = root;
	}

	/**
	 * Get the iterator.
	 */
	public ExecIterator getIterator() {
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
