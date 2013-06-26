package org.olap4j.driver.olap4ld.linkeddata;

/**
 * thrown if something goes wrong during query planning
 * (either not a safe query or some snafu).
 * 
 * @author aharth
 *
 */
public class QueryException extends Exception {
	private final static long serialVersionUID = 1l;
	
	public QueryException(String msg) {
		super(msg);
	}
	
	public QueryException(Exception ex) {
		super(ex);
	}
}
