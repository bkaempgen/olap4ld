package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

public class SliceSparqlIterator implements PhysicalOlapIterator {

	private PhysicalOlapIterator inputiterator;
	private List<Node[]> slicedDimensions;

	public SliceSparqlIterator(PhysicalOlapIterator inputiterator,
			List<Node[]> slicedDimensions) {
		this.inputiterator = inputiterator;
		this.slicedDimensions = slicedDimensions;
	}

	/**
	 * Always true.
	 */
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return true;
	}

	public Object next() {
		List<List<Node[]>> metadata = (ArrayList<List<Node[]>>) inputiterator
				.next();

		// Remove from dimensions all those that are sliced
		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(slicedDimensions.get(0));
		List<Node[]> dimensions = new ArrayList<Node[]>();
		List<Node[]> inputdimensions = metadata.get(2);
		for (Node[] inputdimension : inputdimensions) {
			boolean add = true;
			for (Node[] sliceddimension : slicedDimensions) {

				if (inputdimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.equals(sliceddimension[dimensionmap
								.get("?DIMENSION_UNIQUE_NAME")])) {
					add = false;
				}
			}
			if (add) {
				dimensions.add(inputdimension);
			}
		}
		metadata.set(2, dimensions);

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
