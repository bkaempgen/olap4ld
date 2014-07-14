package org.olap4j.driver.olap4ld.helper;

import org.olap4j.driver.olap4ld.linkeddata.BaseCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.ConvertCubeOp;
import org.olap4j.driver.olap4ld.linkeddata.DiceOp;
import org.olap4j.driver.olap4ld.linkeddata.DrillAcrossOp;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOperatorQueryPlanVisitor;
import org.olap4j.driver.olap4ld.linkeddata.ProjectionOp;
import org.olap4j.driver.olap4ld.linkeddata.QueryException;
import org.olap4j.driver.olap4ld.linkeddata.RollupOp;
import org.olap4j.driver.olap4ld.linkeddata.SliceOp;

/**
 * Creates dot notation of lqp.
 * 
 * @author benedikt
 * 
 */
public class GraphVizVisualisationVisitor implements
		LogicalOlapOperatorQueryPlanVisitor {

	String start = "digraph G { \n";
	String setting = "graph [rankdir=TB,label=\"Logical Query Plan\",labelloc=t] \n";
	String nodedefinitions = "";
	String edgedefinitions = "";
	String end = "} \n";

	@Override
	public void visit(Object op) throws QueryException {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ConvertCubeOp op) throws QueryException {
		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Convert-Cube\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.inputOp1.toString().hashCode()+"; \n";
		edgedefinitions += op.toString().hashCode()+"->"+op.inputOp2.toString().hashCode()+"; \n";
	}

	@Override
	public void visit(DrillAcrossOp op) throws QueryException {
		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Drill-Across\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.inputop1.toString().hashCode()+"; \n";
		edgedefinitions += op.toString().hashCode()+"->"+op.inputop2.toString().hashCode()+"; \n";
	}

	@Override
	public void visit(RollupOp op) throws QueryException {
		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Roll-Up\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.inputOp.toString().hashCode()+"; \n";

	}

	@Override
	public void visit(SliceOp op) throws QueryException {
		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Slice\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.inputOp.toString().hashCode()+"; \n";
	}

	@Override
	public void visit(DiceOp op) throws QueryException {
		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Dice\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.inputOp.toString().hashCode()+"; \n";
	}

	@Override
	public void visit(ProjectionOp op) throws QueryException {
		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Projection\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.inputOp.toString().hashCode()+"; \n";
	}

	@Override
	public void visit(BaseCubeOp op) throws QueryException {

		// nodedefinition
		nodedefinitions += op.toString().hashCode()+"[label=\"Base-Cube\"]; \n";
		nodedefinitions += op.dataseturi.toString().hashCode()+"[label=\""+op.dataseturi+"\"]; \n";
		
		// edgedefinition
		edgedefinitions += op.toString().hashCode()+"->"+op.dataseturi.toString().hashCode()+"; \n";

	}

	@Override
	public Object getNewRoot() throws QueryException {
		return start+setting+nodedefinitions+edgedefinitions+end;
	}


}
