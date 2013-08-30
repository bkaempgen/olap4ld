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
	
	private List<Node[]> cube;

	public BaseCubeOp(List<Node[]> list) {
		this.setCube(list);
	}
	
    public String toString() {
		
    	Map<String, Integer> map = Olap4ldLinkedDataUtil.getNodeResultFields(cube.get(0));
    	
    	Node[] cubeNodes = cube.get(1);
    	int index = map.get("?CUBE_NAME");
    	String cubename = cubeNodes[index].toString();
    	
    	return "BaseCube ("+cubename+")";
    }

	public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException {
		// There is nothing more to visit.
		;
	}

	public List<Node[]> getCube() {
		return cube;
	}

	private void setCube(List<Node[]> cube) {
		this.cube = cube;
	}

}
