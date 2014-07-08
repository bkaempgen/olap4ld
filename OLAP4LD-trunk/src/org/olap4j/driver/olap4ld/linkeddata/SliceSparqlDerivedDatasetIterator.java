//package org.olap4j.driver.olap4ld.linkeddata;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.olap4j.OlapException;
//import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
//import org.openrdf.repository.Repository;
//import org.openrdf.repository.sail.SailRepository;
//import org.semanticweb.yars.nx.Node;
//
///**
// * Slice operator.
// * 
// * XXX Not implemented, yet.
// * 
// * @author benedikt
// * 
// */
//public class SliceSparqlDerivedDatasetIterator implements PhysicalOlapIterator {
//
//	// The different metadata parts
//	private List<Node[]> cubes;
//	private List<Node[]> measures;
//	private List<Node[]> dimensions;
//	private List<Node[]> hierarchies;
//	private List<Node[]> levels;
//	private List<Node[]> members;
//
//	private Iterator<Node[]> iterator;
//	private Repository repo;
//
//	public SliceSparqlDerivedDatasetIterator(Repository repo, PhysicalOlapIterator inputiterator,
//			List<Node[]> slicedDimensions) {
//		
//		this.repo = repo;
//		
//		try {
//			Restrictions restriction = new Restrictions();
//			this.cubes = inputiterator.getCubes(restriction);
//			this.measures = inputiterator.getMeasures(restriction);
//			this.dimensions = inputiterator.getDimensions(restriction);
//			this.hierarchies = inputiterator.getHierarchies(restriction);
//			this.levels = inputiterator.getLevels(restriction);
//			this.members = inputiterator.getMembers(restriction);
//		} catch (OlapException e2) {
//			// TODO Auto-generated catch block
//			e2.printStackTrace();
//		}
//		
//		// measure dimension
//		// header
//		int norelevantdimensions = dimensions.size() -1 - 1;
//		
//		int norelevantmeasures = measures.size()-1;
//
//		Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
//				.getNodeResultFields(slicedDimensions.get(0));
//
//		// XXX: Not implemented, yet.
//		
//		// Wanted result
//		List<Node[]> results = new ArrayList<Node[]>();
//		
//		
//		
//
//		// Former way: Working directly on List<Node[]>. However, we decided to use SPARQL construct, since simply to complicated.
////		/*
////		 * Algorithm:
////		 * 
////		 * 1. Create hash table mapping a hash for the dimension value
////		 * combination to an array of dimension values (Node[], without
////		 * measures)
////		 * 
////		 * 2. Create hash table mapping a hash for the dimension value
////		 * combination to a List of measure values (Node[], for each measure)
////		 * 
////		 * 3. Compute aggregation.
////		 * 
////		 * 4. Bring together possible dimension value combinations and
////		 * aggregated measure values.
////		 */
////
////		HashMap<Integer, Node[]> dimensionCombinationmap = new HashMap<Integer, Node[]>();
////		HashMap<Integer, List<List<Node>>> measuresValuesmap = new HashMap<Integer, List<List<Node>>>();
////
////		// Go through iterator and collect combinations and values
////		// XXX We assume no header (but its coming from basecube)
////		while (inputiterator.hasNext()) {
////			Object nextObject = inputiterator.next();
////			// Will be Node[]
////			Node[] node = (Node[]) nextObject;
////			Node[] newnode = new Node[norelevantdimensions
////					- (slicedDimensions.size()-1)];
////			int newnodeindex = 0;
////			// Go through dimensions, add to dimensionCombinationmap
////			// Concatenate for dimensions
////			String concat = "";
////			// First is header
////			for (int i = 0; i < norelevantdimensions; i++) {
////
////				// only if not sliced
////				boolean sliced = false;
////
////				for (int j = 0; j < slicedDimensions.size()-1; j++) {
////
////					Node[] sliceddimension = slicedDimensions.get(j+1);
////					if (dimensions.get(i+1)[dimensionmap
////							.get("?DIMENSION_UNIQUE_NAME")].toString().equals(
////							sliceddimension[dimensionmap
////									.get("?DIMENSION_UNIQUE_NAME")].toString())) {
////						sliced = true;
////					}
////
////				}
////
////				if (!sliced) {
////					// consider header
////					concat += node[i].toString();
////
////					newnode[newnodeindex] = node[i];
////					newnodeindex++;
////				}
////			}
////			int dimensionCombinationHash = concat.hashCode();
////			if (!dimensionCombinationmap.containsKey(dimensionCombinationHash)) {
////				dimensionCombinationmap.put(dimensionCombinationHash, newnode);
////			}
////			if (!measuresValuesmap.containsKey(dimensionCombinationHash)) {
////				// Create lists
////				ArrayList<List<Node>> listlist = new ArrayList<List<Node>>();
////				// consider header
////				for (int k = 0; k < norelevantmeasures; k++) {
////					ArrayList<Node> list = new ArrayList<Node>();
////					listlist.add(list);
////				}
////				measuresValuesmap.put(dimensionCombinationHash, listlist);
////			}
////			for (int k = 0; k < norelevantmeasures; k++) {
////				// consider header
////				// consider 0
////				// consider the measures dimension
////				// consider header of measures
////				measuresValuesmap.get(dimensionCombinationHash).get(k)
////						.add(node[norelevantdimensions + k]);
////			}
////
////		}
////		Iterator<Entry<Integer, Node[]>> combinations = dimensionCombinationmap
////				.entrySet().iterator();
////
////		// Now, for every dimension value combination, a value.
////		while (combinations.hasNext()) {
////			ArrayList<Node> result = new ArrayList<Node>();
////			Entry<Integer, Node[]> next = combinations.next();
////			Integer hash = next.getKey();
////			for (int j = 0; j < norelevantdimensions-slicedDimensions.size()-1; j++) {
////				Node dimensionValue = next.getValue()[j];
////				result.add(dimensionValue);
////			}
////
////			for (int j = 0; j < norelevantmeasures; j++) {
////				// For now, simply the first value;
////				Node value = measuresValuesmap.get(hash).get(j).get(0);
////				result.add(value);
////			}
////			results.add(result.toArray(new Node[0]));
////		}
////		
////		ArrayList<Node[]> dimensionsminussliced = new ArrayList<Node[]>();
////
////		for (int i = 0; i < norelevantdimensions; i++) {
////
////			// only if not sliced
////			boolean sliced = false;
////
////			for (int j = 0; j < slicedDimensions.size()-1; j++) {
////
////				Node[] sliceddimension = slicedDimensions.get(j+1);
////				if (dimensions.get(i)[dimensionmap
////						.get("?DIMENSION_UNIQUE_NAME")].toString().equals(
////						sliceddimension[dimensionmap
////								.get("?DIMENSION_UNIQUE_NAME")].toString())) {
////					sliced = true;
////				}
////				if (!sliced) {
////					dimensionsminussliced.add(dimensions.get(i));
////				}
////			}
////		}
////		this.dimensions = dimensionsminussliced;
//
//		this.iterator = results.iterator();
//	}
//
//	/**
//	 * Always true.
//	 */
//	public boolean hasNext() {
//		return iterator.hasNext();
//	}
//
//	public Object next() {
//		// No metadata is propagated, let us see whether our operators work
//		// still
//		// List<List<Node[]>> metadata = (ArrayList<List<Node[]>>) inputiterator
//		// .next();
//		//
//		// // Remove from dimensions all those that are sliced
//		// Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
//		// .getNodeResultFields(slicedDimensions.get(0));
//		// List<Node[]> dimensions = new ArrayList<Node[]>();
//		// List<Node[]> inputdimensions = metadata.get(2);
//		// for (Node[] inputdimension : inputdimensions) {
//		// boolean add = true;
//		// for (Node[] sliceddimension : slicedDimensions) {
//		//
//		// if (inputdimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
//		// .equals(sliceddimension[dimensionmap
//		// .get("?DIMENSION_UNIQUE_NAME")])) {
//		// add = false;
//		// }
//		// }
//		// if (add) {
//		// dimensions.add(inputdimension);
//		// }
//		// }
//		// metadata.set(2, dimensions);
//
//		return iterator.next();
//	}
//
//	@Override
//	public void remove() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void init() throws Exception {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void close() throws Exception {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
//			throws QueryException {
//		// TODO Auto-generated method stub
//
//	}
//	@Override
//	public List<Node[]> getCubes(Restrictions restrictions)
//			throws OlapException {
//		return cubes;
//	}
//
//	@Override
//	public List<Node[]> getDimensions(Restrictions restrictions)
//			throws OlapException {
//		return dimensions;
//	}
//
//	@Override
//	public List<Node[]> getMeasures(Restrictions restrictions)
//			throws OlapException {
//		return measures;
//	}
//
//	@Override
//	public List<Node[]> getHierarchies(Restrictions restrictions)
//			throws OlapException {
//		return hierarchies;
//	}
//
//	@Override
//	public List<Node[]> getLevels(Restrictions restrictions)
//			throws OlapException {
//		return levels;
//	}
//
//	@Override
//	public List<Node[]> getMembers(Restrictions restrictions)
//			throws OlapException {
//		return members;
//	}
//}
