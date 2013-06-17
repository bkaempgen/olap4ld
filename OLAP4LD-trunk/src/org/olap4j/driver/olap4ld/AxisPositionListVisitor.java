/*
//
// Licensed to Benedikt Kämpgen under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Benedikt Kämpgen licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package org.olap4j.driver.olap4ld;

import java.util.ArrayList;
import java.util.List;

import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.CubeNode;
import org.olap4j.mdx.DimensionNode;
import org.olap4j.mdx.DrillThroughNode;
import org.olap4j.mdx.HierarchyNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.LevelNode;
import org.olap4j.mdx.LiteralNode;
import org.olap4j.mdx.MemberNode;
import org.olap4j.mdx.ParameterNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeVisitor;
import org.olap4j.mdx.PropertyValueNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.WithMemberNode;
import org.olap4j.mdx.WithSetNode;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.semanticweb.yars.util.Array;

public class AxisPositionListVisitor<T> implements ParseTreeVisitor<T> {

	/*
	 * The positions used in an axis
	 */
	List<Position> positions = new ArrayList<Position>();
	/*
	 * The list of hierarchies used in an axis, the dimensionality in terms of hierarchies
	 */
	List<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();
	/*
	 * The list of measures used in an axis  
	 */
	List<Measure> measureList = new ArrayList<Measure>();
	
	List<Olap4ldCellSetMemberProperty> propertyList = new ArrayList<Olap4ldCellSetMemberProperty>();
	private String operatorName;

	@Override
	public T visit(SelectNode selectNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(AxisNode axis) {
		return null;
	}

	@Override
	public T visit(WithMemberNode calcMemberNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(WithSetNode calcSetNode) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Within the call node I distinguish between different functions, for each,
	 * I run another visitor to create the wanted result.
	 */
	public T visit(CallNode call) {

		this.operatorName = call.getOperatorName();

		return null;
	}

	@Override
	public T visit(IdentifierNode id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(ParameterNode parameterNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(CubeNode cubeNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(DimensionNode dimensionNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(HierarchyNode hierarchyNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(LevelNode levelNode) {

		if (operatorName.equals("Members")) {
			/*
			 * Could I use the members information for creating the sparql
			 * query?
			 */
			try {

				// Fill position list
				List<Member> members = levelNode.getLevel().getMembers();

				for (Member member : members) {
					List<Member> positionMembers = new ArrayList<Member>();
					positionMembers.add(member);
					Olap4ldPosition aPosition = new Olap4ldPosition(
							positionMembers, positions.size());
					positions.add(aPosition);
					
					// Add measure, if members of Measures
					if (levelNode.getLevel().getName().equals("Measures")) {
						measureList.add((Measure) member);
					}
				}
				
				// Fill hierarchy list
				hierarchyList.add(levelNode.getLevel().getHierarchy());

			} catch (OlapException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return null;
	}

	/**
	 * If I visit a member node, I simply add it to the list. 
	 */
	public T visit(MemberNode memberNode) {

		List<Member> positionMembers = new ArrayList<Member>();
		positionMembers.add(memberNode.getMember());
		Olap4ldPosition aPosition = new Olap4ldPosition(positionMembers,
				positions.size());
		positions.add(aPosition);

		// Fill hierarchy list
		hierarchyList.add(memberNode.getMember().getLevel().getHierarchy());
		// Fill measure list
		measureList.add((Measure) memberNode.getMember());
		
		return null;
	}

	@Override
	public T visit(LiteralNode literalNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(PropertyValueNode propertyValueNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T visit(DrillThroughNode drillThroughNode) {
		// TODO Auto-generated method stub
		return null;
	}

}
