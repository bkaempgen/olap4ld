package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * This operator rolls up dimensions of a Cube (the former LogicalOlapOp) to
 * for each hierarchy a specific level.
 * 
 * 
 * @author benedikt
 * 
 */
public class RollupOp implements LogicalOlapOp {

	public LogicalOlapOp inputOp;
	public List<Node[]> rollupslevels;
	public List<Node[]> rollupshierarchies;

	public RollupOp(LogicalOlapOp slice, List<Node[]> rollupshierarchies,
			List<Node[]> rollupslevels) {
		this.inputOp = slice;
		this.rollupslevels = rollupslevels;
		this.rollupshierarchies = rollupshierarchies;
	}

	public String toString() {
		if (rollupslevels != null && !rollupslevels.isEmpty()) {
			String levelsStringArray[] = new String[rollupslevels.size() - 1];
			// First is header
			for (int i = 1; i < rollupslevels.size(); i++) {
				Map<String, Integer> map = Olap4ldLinkedDataUtil
						.getNodeResultFields(rollupslevels.get(0));
				Map<String, Integer> signaturemap = Olap4ldLinkedDataUtil
						.getNodeResultFields(rollupshierarchies.get(0));

				levelsStringArray[i - 1] = rollupshierarchies.get(i)[signaturemap
						.get("?HIERARCHY_UNIQUE_NAME")].toString()
						+ " : "
						+ rollupslevels.get(i)[map.get("?LEVEL_UNIQUE_NAME")]
								.toString();
			}

			return "Rollup ("
					+ inputOp.toString()
					+ ", {"
					+ Olap4ldLinkedDataUtil.implodeArray(levelsStringArray,
							", ") + "})";
		}
		return "Rollup (" + inputOp.toString() + ", {})";
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		v.visit(this);
		// visit the projection input op
		inputOp.accept(v);
	}

}
