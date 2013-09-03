package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * This operator defines the projected measures.
 * 
 * @author benedikt
 *
 */
public class ProjectionOp implements LogicalOlapOp {
	
	private ArrayList<Node[]> projectedMeasures;
	private LogicalOlapOp inputOp;

	public ProjectionOp(LogicalOlapOp inputOp, ArrayList<Node[]> projections) {
		this.inputOp = inputOp;
		this.projectedMeasures = projections;
	}
	
	public ArrayList<Node[]> getProjectedMeasures() {
		return projectedMeasures;
	}
	
    public String toString() {
    	Map<String, Integer> map = Olap4ldLinkedDataUtil.getNodeResultFields(projectedMeasures.get(0));
    	
    	String measuresStringArray[] = new String[projectedMeasures.size()];
    	// First is the header!
    	for (int i = 1; i < measuresStringArray.length; i++) {
    		measuresStringArray[i] = projectedMeasures.get(i)[map.get("?MEASURE_UNIQUE_NAME")].toString();
    	}
    	
        return "Projection (" + inputOp.toString() + ", "+ Olap4ldLinkedDataUtil.implodeArray(measuresStringArray, ", ") +")";
    }

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException {
		v.visit(this);
        // visit the projection input op
		inputOp.accept(v);
	}

}
