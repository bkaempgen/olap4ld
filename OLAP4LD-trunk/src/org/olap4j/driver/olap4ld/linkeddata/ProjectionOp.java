package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Measure;

/**
 * This operator removes facts from a result of a LogicalOlapOp that does not 
 * comply with a condition.
 * 
 * @author benedikt
 *
 */
public class ProjectionOp implements LogicalOlapOp {
	
	private ArrayList<Measure> projectedMeasures;
	private LogicalOlapOp inputOp;

	public ProjectionOp(LogicalOlapOp inputOp, ArrayList<Measure> projectedMeasures) {
		this.inputOp = inputOp;
		this.projectedMeasures = projectedMeasures;
	}
	
	public ArrayList<Measure> getProjectedMeasures() {
		return projectedMeasures;
	}
	
    public String toString() {
    	String measuresStringArray[] = new String[projectedMeasures.size()];
    	for (int i = 0; i < measuresStringArray.length; i++) {
    		measuresStringArray[i] = projectedMeasures.get(i).getUniqueName();
    	}
    	
        return "Projection (" + inputOp.toString() + ", "+ Olap4ldLinkedDataUtil.implodeArray(measuresStringArray, ", ") +")";
    }

	@Override
	public void accept(Visitor v) throws QueryException {
		v.visit(this);
        // visit the projection input op
		inputOp.accept(v);
	}

}
