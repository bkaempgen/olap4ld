package org.olap4j.driver.olap4ld.linkeddata;


public class DrillAcrossOp implements LogicalOlapOp {

	public LogicalOlapOp inputop1;
	public LogicalOlapOp inputop2;

	public DrillAcrossOp(LogicalOlapOp inputop1, LogicalOlapOp inputop2) {
		this.inputop1 = inputop1;
		this.inputop2 = inputop2;
	}

	public String toString() {
		return "Drill-across(" + inputop1.toString() + ", "
				+ inputop2.toString() + ")";
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException {
		v.visit(this);

		if (v instanceof Olap2SparqlSesameDerivedDatasetVisitor) {
			// nothing more to visit
		} else {
			// visit the projection input op
			inputop1.accept(v);
			inputop2.accept(v);
		}
	}
}
