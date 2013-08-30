package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;

import org.olap4j.Position;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Member;

/**
 * This operator removes facts from a result of a LogicalOlapOp that does not 
 * comply with a condition.
 * 
 * @author benedikt
 *
 */
public class DiceOp implements LogicalOlapOp {

	private LogicalOlapOp inputOp;
	private List<Position> positions;

	public DiceOp(LogicalOlapOp op, List<Position> positions) {
		this.inputOp = op;
		this.positions = positions;
	}
	
	public List<Position> getPositions() {
		return positions;
	}
	
    public String toString() {
    	String positionsStringArray = "";
    	for (int i = 0; i < positions.size(); i++) {
    		String positionStringArray[] = new String[positions.size()];
    		List<Member> members = positions.get(i).getMembers();
    		for (int j = 0; j < members.size(); j++) {
    			positionStringArray[i] = members.get(i).getUniqueName();
			}
    		positionsStringArray += "("+Olap4ldLinkedDataUtil.implodeArray(positionStringArray, ", ")+")";
    	}
    	
        return "Dice (" + inputOp.toString() + ", "+ positionsStringArray +")";
    }

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException {
		// TODO Auto-generated method stub
		v.visit(this);
        // visit the projection input op
		inputOp.accept(v);
	}

}
