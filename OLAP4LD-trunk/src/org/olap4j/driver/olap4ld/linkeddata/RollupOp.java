package org.olap4j.driver.olap4ld.linkeddata;

import java.util.ArrayList;
import java.util.List;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Level;

/**
 * This operator rolls up a Dimension of a Cube (the former LogicalOlapOp) to
 * the next higher Level
 * 
 * @author benedikt
 * 
 */
public class RollupOp implements LogicalOlapOp {

	private LogicalOlapOp inputOp;
	private ArrayList<Level> rollups;

	public RollupOp(LogicalOlapOp slice, ArrayList<Level> rollups) {
		this.inputOp = slice;
		this.rollups = rollups;
	}
	
	public List<Level> getRollups() {
		return rollups;
	}
	
    public String toString() {
    	String levelsStringArray[] = new String[rollups.size()];
    	for (int i = 0; i < levelsStringArray.length; i++) {
    		levelsStringArray[i] = rollups.get(i).getDimension().getUniqueName()+" : "+ rollups.get(i).getUniqueName();
    	}
    	
        return "Rollup (" + inputOp.toString() + ", "+ Olap4ldLinkedDataUtil.implodeArray(levelsStringArray, ", ") +")";
    }

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException {
		v.visit(this);
        // visit the projection input op
		inputOp.accept(v);
	}

}
