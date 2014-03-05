package org.olap4j.driver.olap4ld.linkeddata;


/**
 * This operator removes a Dimension from a Cube, a result of a lower
 * LogicalOlapOp.
 * 
 * @author benedikt
 * 
 */
public class ConvertContextOp implements LogicalOlapOp {

	public LogicalOlapOp inputOp;
	public int conversionfunction;	
	

	public ConvertContextOp(LogicalOlapOp inputOp, int conversionfunction) {
		this.inputOp = inputOp;
		this.conversionfunction = conversionfunction;
	}

	public String toString() {
			return "Convert-context ("
					+ inputOp.toString()
					+ ", "+conversionfunction+")";
	}

	@Override
	public void accept(Visitor v) throws QueryException {
		v.visit(this);

		if (v instanceof Olap2SparqlSesameDerivedDatasetVisitor) {
			// Nothing more to visit;
		} else {
			// visit the projection input op
			inputOp.accept(v);
		}
	}
}
