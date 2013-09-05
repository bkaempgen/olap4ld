package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * This operator rolls up a Dimension of a Cube (the former LogicalOlapOp) to
 * the next higher Level
 * 
 * @author benedikt
 * 
 */
public class RollupOp implements LogicalOlapOp {

	private LogicalOlapOp inputOp;
	private List<Node[]> rollups;
	private List<Node[]> rollupssignature;

	public RollupOp(LogicalOlapOp slice, List<Node[]> rollupssignature, List<Node[]> rollups) {
		this.inputOp = slice;
		this.rollups = rollups;
		this.rollupssignature = rollupssignature;
	}
	
	public List<Node[]> getRollups() {
		return rollups;
	}
	
	public List<Node[]> getRollupsSignature() {
		return rollupssignature;
	}
	
    public String toString() {
    	String levelsStringArray[] = new String[rollups.size()];
    	for (int i = 0; i < levelsStringArray.length; i++) {
    		Map<String, Integer> map = Olap4ldLinkedDataUtil
					.getNodeResultFields(rollups.get(0));
    		
    		levelsStringArray[i] = rollups.get(i)[map.get("?DIMENSION_UNIQUE_NAME")].toString()+" : "+ rollups.get(i)[map.get("?LEVEL_UNIQUE_NAME")].toString();
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
