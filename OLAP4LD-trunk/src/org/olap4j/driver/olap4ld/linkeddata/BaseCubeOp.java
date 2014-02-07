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

	public List<Node[]> cubes;
	public List<Node[]> measures;
	public List<Node[]> dimensions;
	public List<Node[]> hierarchies;
	public List<Node[]> levels;
	public List<Node[]> members;

	/**
	 * 
	 * @param list
	 *            with header and one cube
	 */
	public BaseCubeOp(List<Node[]> cubes, List<Node[]> measures,
			List<Node[]> dimensions, List<Node[]> hierarchies,
			List<Node[]> levels, List<Node[]> members) {
		// We assume that cubes to BaseCubeOp always refer to one single cube
		assert cubes.size() <= 2;
		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		this.members = members;
	}

	public String toString() {

		Map<String, Integer> map = Olap4ldLinkedDataUtil
				.getNodeResultFields(cubes.get(0));

		Node[] cubeNodes = cubes.get(1);
		int index = map.get("?CUBE_NAME");
		String cubename = cubeNodes[index].toString();

		return "BaseCube (" + cubename + ")";
	}

	public void accept(Visitor v)
			throws QueryException {
		// There is nothing more to visit.
		;
	}

}
