package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;

import org.semanticweb.yars.nx.Node;

public class BaseCubeSparqlIterator implements PhysicalOlapIterator {
	
	private List<Node[]> cubes;
	private List<Node[]> measures;
	private List<Node[]> dimensions;
	private List<Node[]> hierarchies;
	private List<Node[]> levels;
	private List<Node[]> members;
	
	public BaseCubeSparqlIterator(List<Node[]> cubes, List<Node[]> measures,
			List<Node[]> dimensions, List<Node[]> hierarchies,
			List<Node[]> levels, List<Node[]> members) {
		//We assume that cubes to BaseCubeOp always refer to one single cube
		assert cubes.size() <= 2;
		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		this.members = members;
	}
	
	/**
	 * We only return always the same thing.
	 */
	public boolean hasNext() {
		
		return true;
	}

	@Override
	public Object next() {
		List<List<Node[]>> metadata = new ArrayList<List<Node[]>>();
		metadata.add(cubes);
		metadata.add(measures);
		metadata.add(dimensions);
		metadata.add(hierarchies);
		metadata.add(levels);
		metadata.add(members);
		return metadata;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		// TODO Auto-generated method stub

	}

}
