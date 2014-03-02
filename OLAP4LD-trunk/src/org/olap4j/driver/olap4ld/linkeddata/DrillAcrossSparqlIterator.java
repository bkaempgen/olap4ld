package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

public class DrillAcrossSparqlIterator implements PhysicalOlapIterator {
	
	private PhysicalOlapIterator inputiterator1;
	private PhysicalOlapIterator inputiterator2;

	public DrillAcrossSparqlIterator(PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2) {
		this.inputiterator1 = inputiterator1;
		this.inputiterator2 = inputiterator2;
	}

	@Override
	public boolean hasNext() {
		return true;
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
		
		List<List<Node[]>> results1 = (List<List<Node[]>>) inputiterator1.next();
		
		List<List<Node[]>> results2 = (List<List<Node[]>>) inputiterator2.next();
		
		// Two datasets are joined. For now, just return the first
		
		return results1;
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
