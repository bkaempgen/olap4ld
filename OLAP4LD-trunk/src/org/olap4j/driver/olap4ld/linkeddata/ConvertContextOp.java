package org.olap4j.driver.olap4ld.linkeddata;


/**
 * This operator removes a Dimension from a Cube, a result of a lower
 * LogicalOlapOp.
 * 
 * @author benedikt
 * 
 */
public class ConvertContextOp implements LogicalOlapOp {

	public LogicalOlapOp inputOp1;
	public LogicalOlapOp inputOp2;
	public String conversionfunction;
	public String domainUri;

	public ConvertContextOp(LogicalOlapOp inputOp,
			String conversionfunction, String domainUri) {
		this.inputOp1 = inputOp;
		this.inputOp2 = null;
		this.conversionfunction = conversionfunction;
		this.domainUri = domainUri;
	}

	public ConvertContextOp(LogicalOlapOp inputOp1, LogicalOlapOp inputOp2,
			String conversionfunction, String domainUri) {
		this.inputOp1 = inputOp1;
		this.inputOp2 = inputOp2;
		this.conversionfunction = conversionfunction;
		this.domainUri = domainUri;
	}

	public String toString() {
		if (inputOp2 == null) {
			return "Convert-context (" + inputOp1.toString() + ", "
					+ conversionfunction + ")";
		} else {
			return "Convert-context (" + inputOp1.toString() + ", " + inputOp2.toString() + ", "
					+ conversionfunction + ")";
		}
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {

		v.visit(this);

		if (v instanceof Olap2SparqlSesameDerivedDatasetVisitor) {
			// Nothing more to visit;
		} else {
			// visit the input op
			inputOp1.accept(v);
			
			if (inputOp2 != null) {
				inputOp2.accept(v);
			}
		}
	}
}
