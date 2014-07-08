package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * This operator reads the data from a Cube.
 * 
 * @author benedikt
 * 
 */
public class BaseCubeOp implements LogicalOlapOp {

	public String dataseturi;

	/**
	 * 
	 * @param list
	 *            with header and one cube
	 */
	public BaseCubeOp(String dataseturi) {
		// We assume that cubes to BaseCubeOp always refer to one single cube
		this.dataseturi = dataseturi;
	}

	public String toString() {

		return "BaseCube (" + dataseturi + ")";
	}

	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		v.visit(this);
		// There is nothing more to visit.
		;
	}

}
