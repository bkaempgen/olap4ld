package org.olap4j.driver.olap4ld.linkeddata;

import org.olap4j.metadata.Cube;

/**
 * This operator reads the data from a Cube.
 * 
 * @author benedikt
 *
 */
public class BaseCubeOp implements LogicalOlapOp {

	private Cube cube;

	public BaseCubeOp(Cube cube) {
		this.cube = cube;
	}
	
	public Cube getCube() {
		return cube;
	}
	
    public String toString() {
    	return "BaseCube ("+cube.getUniqueName()+")";
    }

	public void accept(Visitor v) throws QueryException {
		// There is nothing more to visit.
		;
	}

}
