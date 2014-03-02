package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.semanticweb.yars.nx.Node;

public class DrillAcrossSparqlIterator implements PhysicalOlapIterator {
	
	// The different metadata parts
	List<Node[]> cubes;
	List<Node[]> measures;
	List<Node[]> dimensions;
	List<Node[]> hierarchies;
	List<Node[]> levels;
	List<Node[]> members;
	
	private Iterator<Node[]> iterator;

	public DrillAcrossSparqlIterator(PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2, List<Node[]> cubes, List<Node[]> measures, List<Node[]> dimensions,
			List<Node[]> hierarchies, List<Node[]> levels,
			List<Node[]> members) {
		
		this.cubes = cubes;
		this.measures = measures;
		this.dimensions = dimensions;
		this.hierarchies = hierarchies;
		this.levels = levels;
		this.members = members;
		
		/* 
		 * We create our own List<Node[]> result with every item 
		 * 
		 * Every Node[] contains for each dimension in the dimension list of the metadata a
		 * member and for each measure in the measure list a value.
		 */
		List<Node[]> result1 = new ArrayList<Node[]>();
		while (inputiterator1.hasNext()) {
			Object nextObject = inputiterator1.next();
			// Will be Node[]
			Node[] node = (Node[]) nextObject;
			result1.add(node);
		}
		
		List<Node[]> result2 = new ArrayList<Node[]>();
		while (inputiterator2.hasNext()) {
			Object nextObject = inputiterator2.next();
			// Will be Node[]
			Node[] node = (Node[]) nextObject;
			result2.add(node);
		}
		
		// Two datasets are joined. For now, just return the first
		
		this.iterator = result1.iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	/**
	 * Here, we 
	 * 
	 * @return
	 */
	public Object next() {
		
		
//		List<List<Node[]>> metadata = (ArrayList<List<Node[]>>) inputiterator
//				.next();
//
//		// Remove from dimensions all those that are sliced
//		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
//				.getNodeResultFields(slicedDimensions.get(0));
//		List<Node[]> dimensions = new ArrayList<Node[]>();
//		List<Node[]> inputdimensions = metadata.get(2);
//		for (Node[] inputdimension : inputdimensions) {
//			boolean add = true;
//			for (Node[] sliceddimension : slicedDimensions) {
//
//				if (inputdimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
//						.equals(sliceddimension[dimensionmap
//								.get("?DIMENSION_UNIQUE_NAME")])) {
//					add = false;
//				}
//			}
//			if (add) {List<List<Node[]>> results = (List<List<Node[]>>) inputiterator.next();
//				dimensions.add(inputdimension);
//			}
//		}
//		metadata.set(2, dimensions);
		
		return iterator.next();
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
