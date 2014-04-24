package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * This operator removes dimensions from a Cube, a result of a lower
 * LogicalOlapOp.
 * 
 * @author benedikt
 * 
 */
public class SliceOp implements LogicalOlapOp {

	public LogicalOlapOp inputOp;	
	public List<Node[]> slicedDimensions;
	

	public SliceOp(LogicalOlapOp inputOp, List<Node[]> slicedDimensions) {
		this.inputOp = inputOp;
		this.slicedDimensions = slicedDimensions;
	}

	public String toString() {
		if (slicedDimensions != null && !slicedDimensions.isEmpty()) {
			String dimensionsStringArray[] = new String[slicedDimensions.size() - 1];
			// First is header
			for (int i = 1; i <= dimensionsStringArray.length; i++) {
				Map<String, Integer> map = Olap4ldLinkedDataUtil
						.getNodeResultFields(slicedDimensions.get(0));

				dimensionsStringArray[i - 1] = slicedDimensions.get(i)[map
						.get("?DIMENSION_UNIQUE_NAME")].toString();
			}

			return "Slice ("
					+ inputOp.toString()
					+ ", {"
					+ Olap4ldLinkedDataUtil.implodeArray(dimensionsStringArray,
							", ") + "})";
		}
		return "Slice (" + inputOp.toString() + ", {})";
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v) throws QueryException {
		v.visit(this);

		if (v instanceof Olap2SparqlSesameDerivedDatasetVisitor) {
			// Nothing more to visit;
		} else {
			// visit the projection input op
			inputOp.accept(v);
		}
	}
}
