package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.olap4j.OlapException;
import org.semanticweb.yars.nx.Node;

public class DrillAcrossSparqlDerivedDatasetIterator implements PhysicalOlapIterator {
	
	// The different metadata parts
	private List<Node[]> cubes;
	private List<Node[]> measures;
	private List<Node[]> dimensions;
	private List<Node[]> hierarchies;
	private List<Node[]> levels;
	private List<Node[]> members;
	
	private Iterator<Node[]> iterator;

	public DrillAcrossSparqlDerivedDatasetIterator(PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2) {
		
		try {
			Restrictions restriction = new Restrictions();
			this.cubes = inputiterator1.getCubes(restriction);
			this.measures = inputiterator1.getMeasures(restriction);
			this.dimensions = inputiterator1.getDimensions(restriction);
			this.hierarchies = inputiterator1.getHierarchies(restriction);
			this.levels = inputiterator1.getLevels(restriction);
			this.members = inputiterator1.getMembers(restriction);
		} catch (OlapException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
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

	@Override
	public List<Node[]> getCubes(Restrictions restrictions)
			throws OlapException {
		return cubes;
	}

	@Override
	public List<Node[]> getDimensions(Restrictions restrictions)
			throws OlapException {
		return dimensions;
	}

	@Override
	public List<Node[]> getMeasures(Restrictions restrictions)
			throws OlapException {
		return measures;
	}

	@Override
	public List<Node[]> getHierarchies(Restrictions restrictions)
			throws OlapException {
		return hierarchies;
	}

	@Override
	public List<Node[]> getLevels(Restrictions restrictions)
			throws OlapException {
		return levels;
	}

	@Override
	public List<Node[]> getMembers(Restrictions restrictions)
			throws OlapException {
		return members;
	}
}
