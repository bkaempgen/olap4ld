package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Dimension;

/**
 * This operator removes a Dimension from a Cube, a result of a lower LogicalOlapOp.
 * 
 * @author benedikt
 *
 */
public class SliceOp implements LogicalOlapOp {

	private LogicalOlapOp inputOp;
	private List<Dimension> slicedDimensions;

	public SliceOp(LogicalOlapOp inputOp, List<Dimension> slicedDimensions) {
		this.inputOp = inputOp;
		this.slicedDimensions = slicedDimensions;
	}
	
    public String toString() {
    	String dimensionsStringArray[] = new String[slicedDimensions.size()];
    	for (int i = 0; i < dimensionsStringArray.length; i++) {
    		dimensionsStringArray[i] = slicedDimensions.get(i).getUniqueName();
    	}
    	
        return "Slice (" + inputOp.toString() + ", "+ Olap4ldLinkedDataUtil.implodeArray(dimensionsStringArray, ", ") +")";
    }

	@Override
	public void accept(Visitor v) throws QueryException {
		v.visit(this);
        // visit the projection input op
		inputOp.accept(v);
	}

}
