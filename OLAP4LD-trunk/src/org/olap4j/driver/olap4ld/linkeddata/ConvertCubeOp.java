package org.olap4j.driver.olap4ld.linkeddata;

/**
 * This operator implements Convert-Cube as well as the extension for several
 * cubes, Merge-Cubes.
 * 
 * @author benedikt
 * 
 */
public class ConvertCubeOp implements LogicalOlapOp {

	public LogicalOlapOp inputOp1;
	public LogicalOlapOp inputOp2;
	public String domainUri;
	public ReconciliationCorrespondence conversioncorrespondence;

	/**
	 * 
	 * @param inputOp
	 * @param conversionfunction
	 *            Currently, conversion function is a Linked-Data-Fu program.
	 *            The goal is to represent it in terms of multidimensional
	 *            elements.
	 * @param domainUri
	 */
	public ConvertCubeOp(LogicalOlapOp inputOp,
			ReconciliationCorrespondence conversioncorrespondence,
			String domainUri) {
		this.inputOp1 = inputOp;
		this.inputOp2 = null;
		this.conversioncorrespondence = conversioncorrespondence;
		this.domainUri = domainUri;
	}

	public ConvertCubeOp(LogicalOlapOp inputOp1, LogicalOlapOp inputOp2,
			ReconciliationCorrespondence conversioncorrespondence,
			String domainUri) {
		this.inputOp1 = inputOp1;
		this.inputOp2 = inputOp2;
		this.conversioncorrespondence = conversioncorrespondence;
		this.domainUri = domainUri;
	}

	public String toString() {
		if (inputOp2 == null) {
			return "Convert-Cube (" + inputOp1.toString() + ", "
					+ conversioncorrespondence.toString() + ")";
		} else {
			return "Merge-Cubes (" + inputOp1.toString() + ", "
					+ inputOp2.toString() + ", "
					+ conversioncorrespondence.toString() + ")";
		}
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {

		v.visit(this);

		// I do not use visitor pattern any more since depth-first search through lqp suffices.
		// if (v instanceof Olap2SparqlSesameDerivedDatasetVisitor) {
		// // Nothing more to visit;
		// } else {
		// // visit the input op
		//
		// }
		inputOp1.accept(v);

		if (inputOp2 != null) {
			inputOp2.accept(v);
		}
	}
}
