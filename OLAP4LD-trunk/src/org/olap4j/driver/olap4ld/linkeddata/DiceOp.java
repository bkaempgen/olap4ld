package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;
import java.util.Map;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.semanticweb.yars.nx.Node;

/**
 * This operator removes facts from a result of a LogicalOlapOp that do not
 * comply with a condition.
 * 
 * @author benedikt
 * 
 */
public class DiceOp implements LogicalOlapOp {

	public LogicalOlapOp inputOp;
	public List<List<Node[]>> membercombinations;
	public List<Node[]> hierarchysignature;

	/**
	 * 
	 * @param op
	 * @param hierarchysignature A list of hierarchies. For each hierarchy, in the member combinations
	 * a list of members is given.
	 * @param membercombinations A list of members to dice.
	 */
	public DiceOp(LogicalOlapOp op, List<Node[]> hierarchysignature,
			List<List<Node[]>> membercombinations) {
		this.inputOp = op;
		this.membercombinations = membercombinations;
		this.hierarchysignature = hierarchysignature;
	}

	public String toString() {
		String positionsStringArray = "";

		// Every member same schema
		if (membercombinations == null || membercombinations.isEmpty()) {
			positionsStringArray = "{}";
		} else {
			Map<String, Integer> map = Olap4ldLinkedDataUtil
					.getNodeResultFields(membercombinations.get(0).get(0));
			Map<String, Integer> signaturemap = Olap4ldLinkedDataUtil
					.getNodeResultFields(hierarchysignature.get(0));

			// There is no header
			for (int i = 0; i < membercombinations.size(); i++) {
				
				if (i!=0) {
					positionsStringArray += " OR ";
				}
				
				// One possible member combination (inside AND)
				List<Node[]> members = membercombinations.get(i);
				
				String positionStringArray[] = new String[members
				                  						.size()-1];
				// First is header
				for (int j = 1; j < members.size(); j++) {
					positionStringArray[j-1] = this.hierarchysignature.get(j)[signaturemap
							.get("?HIERARCHY_UNIQUE_NAME")]
							+ " = "
							+ members.get(j)[map.get("?MEMBER_UNIQUE_NAME")]
									.toString();
				}
				positionsStringArray += "("
						+ Olap4ldLinkedDataUtil.implodeArray(
								positionStringArray, " AND ") + ")";
			}
		}

		return "Dice (" + inputOp.toString() + ", " + positionsStringArray
				+ ")";
	}

	@Override
	public void accept(LogicalOlapOperatorQueryPlanVisitor v)
			throws QueryException {
		v.visit(this);
		// visit the projection input op
		inputOp.accept(v);
	}

}
