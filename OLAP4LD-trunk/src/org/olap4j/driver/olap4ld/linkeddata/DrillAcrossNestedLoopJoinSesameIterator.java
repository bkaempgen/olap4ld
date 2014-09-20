package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

/**
 * This iterator simply computes the nested-loop join of input iterators
 * (mostly, the Olap2SparqlAlgorithmSesameIterator).
 * 
 * @author benedikt
 * 
 */
public class DrillAcrossNestedLoopJoinSesameIterator implements
		PhysicalOlapIterator {

	private List<Node[]> cubes;
	private List<Node[]> measures;
	private List<Node[]> dimensions;
	private List<Node[]> hierarchies;
	private List<Node[]> levels;
	private List<Node[]> members;

	private Iterator<Node[]> iterator;
	private List<Node[]> results;
	private PhysicalOlapIterator inputiterator1;
	private PhysicalOlapIterator inputiterator2;

	public DrillAcrossNestedLoopJoinSesameIterator(PhysicalOlapIterator inputiterator1,
			PhysicalOlapIterator inputiterator2) {

		this.inputiterator1 = inputiterator1;
		this.inputiterator2 = inputiterator2;

		// We simply assume two cubes
		// Metadata we do directly.
		createMetadata();

	}

	private void createMetadata() {
		// We assume that there is at least one input iterator that we can use
		// the metadata from.

		cubes = new ArrayList<Node[]>();
		measures = new ArrayList<Node[]>();
		dimensions = new ArrayList<Node[]>();
		hierarchies = new ArrayList<Node[]>();
		levels = new ArrayList<Node[]>();
		members = new ArrayList<Node[]>();

		Restrictions restrictions = new Restrictions();
		try {
			// this.cubes = root1.getCubes(restrictions);
			// this.measures = root1.getMeasures(restrictions);
			// this.dimensions = root1.getDimensions(restrictions);
			// this.hierarchies = root1.getHierarchies(restrictions);
			// this.levels = root1.getLevels(restrictions);
			// // One problem could be that this may be a huge number of
			// members.
			// this.members = root1.getMembers(restrictions);

			// Now, add "virtual cube"

			// Needs to be concatenation of single cubes
			Map<String, Integer> cubemap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getCubes(restrictions).get(0));

			cubes.add(inputiterator1.getCubes(restrictions).get(0));

			// XXX2C is separator in MDX, , in Linked Data
			String cubename = inputiterator1.getCubes(restrictions).get(1)[cubemap
					.get("?CUBE_NAME")].toString()
					+ ","
					+ inputiterator2.getCubes(restrictions).get(1)[cubemap
							.get("?CUBE_NAME")].toString();

			// ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?CUBE_TYPE ?CUBE_CAPTION
			// ?DESCRIPTION
			Node[] newnode = new Node[6];
			newnode[cubemap.get("?CATALOG_NAME")] = inputiterator1
					.getCubes(restrictions).get(1)[cubemap.get("?CATALOG_NAME")];
			newnode[cubemap.get("?SCHEMA_NAME")] = inputiterator1.getCubes(restrictions)
					.get(1)[cubemap.get("?SCHEMA_NAME")];
			newnode[cubemap.get("?CUBE_NAME")] = new Literal(cubename);

			newnode[cubemap.get("?CUBE_TYPE")] = new Literal("CUBE");
			newnode[cubemap.get("?CUBE_CAPTION")] = new Literal("Global Cube");
			newnode[cubemap.get("?DESCRIPTION")] = new Literal(
					"This is the global cube.");

			cubes.add(newnode);

			// Now, add Measures

			// Also add measure to global cube

			// From first cube
			Map<String, Integer> measuremap = Olap4ldLinkedDataUtil
					.getNodeResultFields(inputiterator1.getMeasures(restrictions).get(0));

			// Add to result from first cube
			boolean first = true;
			for (Node[] anIntermediaryresult : inputiterator1.getMeasures(restrictions)) {
				if (first) {
					measures.add(anIntermediaryresult);
					first = false;
					continue;
				}

				// We do not want to have the single datasets returned.
				// result.add(anIntermediaryresult);

				newnode = new Node[10];
				newnode[measuremap.get("?CATALOG_NAME")] = anIntermediaryresult[measuremap
						.get("?CATALOG_NAME")];
				newnode[measuremap.get("?SCHEMA_NAME")] = anIntermediaryresult[measuremap
						.get("?SCHEMA_NAME")];
				newnode[measuremap.get("?CUBE_NAME")] = new Literal(cubename);
				newnode[measuremap.get("?MEASURE_UNIQUE_NAME")] = anIntermediaryresult[measuremap
						.get("?MEASURE_UNIQUE_NAME")];
				newnode[measuremap.get("?MEASURE_NAME")] = anIntermediaryresult[measuremap
						.get("?MEASURE_NAME")];
				newnode[measuremap.get("?MEASURE_CAPTION")] = anIntermediaryresult[measuremap
						.get("?MEASURE_CAPTION")];
				newnode[measuremap.get("?DATA_TYPE")] = anIntermediaryresult[measuremap
						.get("?DATA_TYPE")];
				newnode[measuremap.get("?MEASURE_IS_VISIBLE")] = anIntermediaryresult[measuremap
						.get("?MEASURE_IS_VISIBLE")];
				newnode[measuremap.get("?MEASURE_AGGREGATOR")] = anIntermediaryresult[measuremap
						.get("?MEASURE_AGGREGATOR")];
				newnode[measuremap.get("?EXPRESSION")] = anIntermediaryresult[measuremap
						.get("?EXPRESSION")];

				// Only add if not already contained.
				// For measures, we add them all.

				measures.add(newnode);
			}

			// Do join
			Restrictions emptyrestrictions = new Restrictions();
			List<Node[]> root1_measures = inputiterator1.getMeasures(emptyrestrictions);
			List<Node[]> root2_measures = inputiterator2.getMeasures(emptyrestrictions);

			// We check if all measures are the same
			boolean allmeasuresthesame = areAllMeasuresTheSame(root1_measures,
					root2_measures);

			// If all the same then we do not need to add any more.
			if (!allmeasuresthesame) {
				// From second cube
				measuremap = Olap4ldLinkedDataUtil.getNodeResultFields(inputiterator2
						.getMeasures(restrictions).get(0));

				// Add to result from first cube
				first = true;
				for (Node[] anIntermediaryresult : inputiterator2
						.getMeasures(restrictions)) {
					if (first) {
						// Do not add header twice.
						// measures.add(anIntermediaryresult);
						first = false;
						continue;
					}

					// We do not want to have the single datasets returned.
					// result.add(anIntermediaryresult);

					newnode = new Node[10];
					newnode[measuremap.get("?CATALOG_NAME")] = anIntermediaryresult[measuremap
							.get("?CATALOG_NAME")];
					newnode[measuremap.get("?SCHEMA_NAME")] = anIntermediaryresult[measuremap
							.get("?SCHEMA_NAME")];
					newnode[measuremap.get("?CUBE_NAME")] = new Literal(
							cubename);
					newnode[measuremap.get("?MEASURE_UNIQUE_NAME")] = anIntermediaryresult[measuremap
							.get("?MEASURE_UNIQUE_NAME")];
					newnode[measuremap.get("?MEASURE_NAME")] = anIntermediaryresult[measuremap
							.get("?MEASURE_NAME")];
					newnode[measuremap.get("?MEASURE_CAPTION")] = anIntermediaryresult[measuremap
							.get("?MEASURE_CAPTION")];
					newnode[measuremap.get("?DATA_TYPE")] = anIntermediaryresult[measuremap
							.get("?DATA_TYPE")];
					newnode[measuremap.get("?MEASURE_IS_VISIBLE")] = anIntermediaryresult[measuremap
							.get("?MEASURE_IS_VISIBLE")];
					newnode[measuremap.get("?MEASURE_AGGREGATOR")] = anIntermediaryresult[measuremap
							.get("?MEASURE_AGGREGATOR")];
					newnode[measuremap.get("?EXPRESSION")] = anIntermediaryresult[measuremap
							.get("?EXPRESSION")];

					// Only add if not already contained.
					// For measures, we add them all.

					measures.add(newnode);
				}
			}

			// Assume dimensions / hierarchies / levels to be used from first
			// cube.
			this.dimensions = inputiterator1.getDimensions(restrictions);
			this.hierarchies = inputiterator1.getHierarchies(restrictions);
			this.levels = inputiterator1.getLevels(restrictions);

			// Can we use members?
			// One problem could be that this may be a huge number of members

			this.members = inputiterator1.getMembers(restrictions);

		} catch (OlapException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private void createData(PhysicalOlapIterator root1,
			PhysicalOlapIterator root2) {
		List<Node[]> results = new ArrayList<Node[]>();

		Restrictions emptyrestrictions = new Restrictions();
		try {
			List<Node[]> root1_dimensions = root1
					.getDimensions(emptyrestrictions);
			List<Node[]> root2_dimensions = root2
					.getDimensions(emptyrestrictions);

			// First check whether equal dimensions.
			boolean equal = true;

			// For now, we assume the same ordering of dimensions (probably the
			// case due to the query, anyway)

			Map<String, Integer> dimensionmap = Olap4ldLinkedDataUtil
					.getNodeResultFields(root1_dimensions.get(0));

			for (int i = 0; i < root1_dimensions.size(); i++) {
				Node[] root1_dimension = root1_dimensions.get(i);
				Node[] root2_dimension = root2_dimensions.get(i);

				// Measure dimension should also be the same
				// if
				// (!root1_dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
				// .toString().equals(Olap4ldLinkedDataUtil.MEASURE_DIMENSION_NAME))
				// {
				// continue;
				// }

				// XXX: Maybe consider reasoning
				if (!root1_dimension[dimensionmap.get("?DIMENSION_UNIQUE_NAME")]
						.toString().equals(
								root2_dimension[dimensionmap
										.get("?DIMENSION_UNIQUE_NAME")]
										.toString())) {
					equal = false;
				}
			}

			if (!equal) {
				throw new UnsupportedOperationException(
						"Drill-across only over equally-structured cubes!");
			} else {

				// Do join
				List<Node[]> root1_measures = root1
						.getMeasures(emptyrestrictions);
				List<Node[]> root2_measures = root2
						.getMeasures(emptyrestrictions);

				// We check if all measures are the same
				boolean allmeasuresthesame = areAllMeasuresTheSame(
						root1_measures, root2_measures);

				// Nested-loop
				boolean firstroot1 = true;
				while (root1.hasNext()) {
					Node[] root1_node = (Node[]) root1.next();
					List<Node> result = new ArrayList<Node>();
					try {
						root2.init();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					while (root2.hasNext()) {
						Node[] root2_node = (Node[]) root2.next();

						// Consider measure dimension and header
						equal = false;
						// XXX: Check whether root1_node = root2_node if so,
						// then...
						// Currently we assume same dimension ordering and same
						// members.
						String concat1 = "";
						String concat2 = "";
						for (int i = 0; i < root1_dimensions.size() - 2; i++) {
							concat1 += root1_node[i].toString();
						}
						for (int i = 0; i < root2_dimensions.size() - 2; i++) {
							concat2 += root2_node[i].toString();
						}

						if (concat1.hashCode() == concat2.hashCode()) {
							equal = true;
						}

						if (equal) {
							// Add to result

							// Add dimensions
							for (int i = 0; i < root1_dimensions.size() - 2; i++) {
								result.add(root1_node[i]);
							}

							if (allmeasuresthesame) {
								// Concat measures of both cubes
								// Under assumptions both cubes have the same
								// measures.
								for (int i = 0; i < root1_measures.size() - 1; i++) {
									Resource newnode;
									// Here, header is different, we do not want to concat.
									if (firstroot1 == true) {
										newnode = new Resource(
												root1_node[root1_dimensions
														.size() - 2 + i]
														.toString());
										firstroot1 = false;
									} else {

										// Need to create new node
										newnode = new Resource(
												root1_node[root1_dimensions
														.size() - 2 + i]
														+ " / "
														+ root2_node[root1_dimensions
																.size() - 2 + i]);

									}
									result.add(newnode);
								}

							} else {

								// Here first (header) is same as all other.

								// Add measures of cube one
								for (int i = root1_dimensions.size() - 2; i < root1_dimensions
										.size() - 2 + root1_measures.size() - 1; i++) {
									result.add(root1_node[i]);
								}

								// Add measures of cube two
								for (int i = root1_dimensions.size() - 2
										+ root1_measures.size() - 1; i < root1_dimensions
										.size()
										- 2
										+ root1_measures.size()
										- 1
										+ root2_measures.size() - 1; i++) {
									result.add(root2_node[i
											- root1_measures.size() + 1]);
								}

							}

							results.add(result.toArray(new Node[1]));

							/*
							 * Performance optimisation: Shall we have an outer
							 * loop join? The problem would be that then for the
							 * same dimension members, we would have several
							 * facts. Can we be sure that this really is the
							 * case? Yes, if we have integrity constraint checks
							 * on the original data. Therefore, we assume it.
							 */
							break;

						}

					}
				}

				// Outer join
				if (allmeasuresthesame) {

					// Left outer
					try {
						root1.init();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					while (root1.hasNext()) {
						Node[] root1_node = (Node[]) root1.next();
						List<Node> result = new ArrayList<Node>();
						try {
							root2.init();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						while (root2.hasNext()) {
							Node[] root2_node = (Node[]) root2.next();

							// Consider measure dimension and header
							equal = false;
							// XXX: Check whether root1_node = root2_node if so,
							// then...
							// Currently we assume same dimension ordering and
							// same
							// members.
							String concat1 = "";
							String concat2 = "";
							for (int i = 0; i < root1_dimensions.size() - 2; i++) {
								concat1 += root1_node[i].toString();
							}
							for (int i = 0; i < root2_dimensions.size() - 2; i++) {
								concat2 += root2_node[i].toString();
							}

							if (concat1.hashCode() == concat2.hashCode()) {
								equal = true;
							}

							if (equal) {
								/*
								 * Performance optimisation: Shall we have an
								 * outer loop join? The problem would be that
								 * then for the same dimension members, we would
								 * have several facts. Can we be sure that this
								 * really is the case? Yes, if we have integrity
								 * constraint checks on the original data.
								 * Therefore, we assume it.
								 */
								break;

							}

						}
						if (!equal) {

							// Add dimensions
							for (int i = 0; i < root1_dimensions.size() - 2; i++) {
								result.add(root1_node[i]);
							}

							// Add measures of cube one
							for (int i = root1_dimensions.size() - 2; i < root1_dimensions
									.size() - 2 + root1_measures.size() - 1; i++) {
								result.add(root1_node[i]);
							}

							// Status: Why geo?
							results.add(result.toArray(new Node[1]));
						}
					}

					// Right outer
					try {
						root2.init();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					while (root2.hasNext()) {
						Node[] root2_node = (Node[]) root2.next();
						List<Node> result = new ArrayList<Node>();
						try {
							root1.init();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						while (root1.hasNext()) {
							Node[] root1_node = (Node[]) root1.next();

							// Consider measure dimension and header
							equal = false;
							// XXX: Check whether root1_node = root2_node if so,
							// then...
							// Currently we assume same dimension ordering and
							// same
							// members.
							String concat1 = "";
							String concat2 = "";
							for (int i = 0; i < root1_dimensions.size() - 2; i++) {
								concat1 += root1_node[i].toString();
							}
							for (int i = 0; i < root2_dimensions.size() - 2; i++) {
								concat2 += root2_node[i].toString();
							}

							if (concat1.hashCode() == concat2.hashCode()) {
								equal = true;
							}

							if (equal) {

								/*
								 * Performance optimisation: Shall we have an
								 * outer loop join? The problem would be that
								 * then for the same dimension members, we would
								 * have several facts. Can we be sure that this
								 * really is the case? Yes, if we have integrity
								 * constraint checks on the original data.
								 * Therefore, we assume it.
								 */
								break;

							}

						}
						if (!equal) {

							// Add dimensions
							for (int i = 0; i < root2_dimensions.size() - 2; i++) {
								result.add(root2_node[i]);
							}

							// Add measures of cube two
							for (int i = root1_dimensions.size() - 2
									+ root1_measures.size() - 1; i < root1_dimensions
									.size()
									- 2
									+ root1_measures.size()
									- 1
									+ root2_measures.size() - 1; i++) {
								result.add(root2_node[i - root1_measures.size()
										+ 1]);
							}

							// Status: Why geo?
							results.add(result.toArray(new Node[1]));
						}
					}

				}
			}

		} catch (OlapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.results = results;
	}

	private boolean areAllMeasuresTheSame(List<Node[]> root1_measures,
			List<Node[]> root2_measures) {
		Map<String, Integer> measurenmap = Olap4ldLinkedDataUtil
				.getNodeResultFields(root1_measures.get(0));
		boolean allmeasuresthesame = true;
		if (root1_measures.size() != root2_measures.size()) {
			allmeasuresthesame = false;
		} else {

			for (int i = 1; i < root1_measures.size(); i++) {
				if (!root1_measures.get(i)[measurenmap
						.get("?MEASURE_UNIQUE_NAME")].toString().equals(
						root2_measures.get(i)[measurenmap
								.get("?MEASURE_UNIQUE_NAME")].toString())) {
					allmeasuresthesame = false;
				}
			}
		}
		return allmeasuresthesame;
	}

	@Override
	public boolean hasNext() {
		if (this.results == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return iterator.hasNext();
	}

	@Override
	public Object next() {
		if (this.results == null) {
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return iterator.next();
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public void init() throws Exception {

		if (this.results == null) {
			// Init
			try {

				// Does have input operators, therefore other init necessary.
				inputiterator1.init();
				inputiterator2.init();

				createData(inputiterator1, inputiterator2);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.iterator = results.iterator();
	}

	@Override
	public void close() throws Exception {
		// something to do?
	}

	@Override
	public String toString() {
		return "Nested-Loop over (" + inputiterator1 + "," + inputiterator2 + ")";
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		// Nothing done, yet.
		;
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
