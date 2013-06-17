/**
 * 
 */
package org.olap4j.driver.olap4ld.helper;
import java.util.ArrayList;
import java.util.List;

import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.CubeNode;
import org.olap4j.mdx.DimensionNode;
import org.olap4j.mdx.DrillThroughNode;
import org.olap4j.mdx.HierarchyNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.mdx.LevelNode;
import org.olap4j.mdx.LiteralNode;
import org.olap4j.mdx.MemberNode;
import org.olap4j.mdx.ParameterNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeVisitor;
import org.olap4j.mdx.PropertyValueNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.Syntax;
import org.olap4j.mdx.WithMemberNode;
import org.olap4j.mdx.WithSetNode;

/**
 * 
 * 
 * 
 * @author benedikt
 *
 * @param <Object>
 */
public class PreprocessMdxVisitor<Object> implements ParseTreeVisitor<Object> {

	@Override
	public Object visit(SelectNode selectNode) {
		List<AxisNode> list = selectNode.getAxisList();
		for (AxisNode axisNode : list) {
			axisNode.accept(this);
		}
		return null;
	}

	@Override
	public Object visit(AxisNode axis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(WithMemberNode calcMemberNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(WithSetNode calcSetNode) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * If we have an identifier node ending with members, we do our magic.
	 * 
	 * For now, this is very basic, since we assume that there is only one argument in the call node.
	 */
	public Object visit(CallNode call) {
		List<ParseTreeNode> arglist = call.getArgList();
		ParseTreeNode myParseTreeNode = null;
		CallNode myMembersCallNode = null; 
		for (ParseTreeNode parseTreeNode : arglist) {
			if (parseTreeNode instanceof IdentifierNode) {
				IdentifierNode id = (IdentifierNode) parseTreeNode;
				List<IdentifierSegment> segmentlist = id.getSegmentList();
				
				if (segmentlist.get(segmentlist.size()-1).getName().toLowerCase().equals("members")) {
					// Now, we 1) remove segment from list 2) create a new CallNode "Members"		
					List<IdentifierSegment> newsegmentlist = new ArrayList<IdentifierSegment>();
					for (IdentifierSegment identifierSegment : segmentlist) {
						if (!identifierSegment.getName().toLowerCase().equals("members")) {
							newsegmentlist.add(identifierSegment);
						}
					}
					
					List<ParseTreeNode> args = new ArrayList<ParseTreeNode>();
					IdentifierNode id2 = new IdentifierNode(newsegmentlist);
					args.add(id2);
					myMembersCallNode = new CallNode(null, "Members", Syntax.Function, args);
					myParseTreeNode = parseTreeNode; 
				} 
			} 
		}
		// Replace
		if (myParseTreeNode != null && myMembersCallNode != null) {
			arglist.set(arglist.indexOf(myParseTreeNode), myMembersCallNode);
		}
		return null;
	}

	@Override
	public Object visit(IdentifierNode id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(ParameterNode parameterNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(CubeNode cubeNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(DimensionNode dimensionNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(HierarchyNode hierarchyNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(LevelNode levelNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(MemberNode memberNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(LiteralNode literalNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(PropertyValueNode propertyValueNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visit(DrillThroughNode drillThroughNode) {
		// TODO Auto-generated method stub
		return null;
	}
	
}