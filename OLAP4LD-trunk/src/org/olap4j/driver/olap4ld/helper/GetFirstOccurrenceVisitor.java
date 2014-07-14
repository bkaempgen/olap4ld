package org.olap4j.driver.olap4ld.helper;

import org.olap4j.driver.olap4ld.linkeddata.BaseCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.ConvertCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.DiceOp;
import org.olap4j.driver.olap4ld.linkeddata.DrillAcrossOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOperatorQueryPlanVisitor;
import org.olap4j.driver.olap4ld.linkeddata.ProjectionOp;
import org.olap4j.driver.olap4ld.linkeddata.QueryException;
import org.olap4j.driver.olap4ld.linkeddata.RollupOp;
import org.olap4j.driver.olap4ld.linkeddata.SliceOp;

/**
 * Looks for occurence of operator.
 * 
 * @author benedikt
 * 
 */
public class GetFirstOccurrenceVisitor implements
		LogicalOlapOperatorQueryPlanVisitor {

	private LogicalOlapOp operator;

	private LogicalOlapOp firstoccurence = null;

	public GetFirstOccurrenceVisitor(LogicalOlapOp operator) {
		this.operator = operator;
	}

	@Override
	public void visit(Object op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ConvertCubeOp op) throws QueryException {

		if (operator instanceof ConvertCubeOp) {

			if (((ConvertCubeOp) operator).conversioncorrespondence != null) {
				if (op.conversioncorrespondence
						.equals(((ConvertCubeOp) operator).conversioncorrespondence)) {
					this.firstoccurence = op;
				}
			} else {
				this.firstoccurence = op;
			}
		}

	}

	@Override
	public void visit(DrillAcrossOp op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(RollupOp op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SliceOp op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DiceOp op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ProjectionOp op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BaseCubeOp op) throws QueryException {

		if (operator instanceof BaseCubeOp) {

			if (((BaseCubeOp) operator).dataseturi != null) {
				if (((BaseCubeOp) operator).dataseturi.equals(op.dataseturi)) {
					this.firstoccurence = op;
				}
			} else {

				this.firstoccurence = op;
			}

		}

	}

	@Override
	public Object getNewRoot() throws QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	public LogicalOlapOp getFirstOccurence() {
		return this.firstoccurence;
	}

}
