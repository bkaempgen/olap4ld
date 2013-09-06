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

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldDimension;
import org.olap4j.driver.olap4ld.Olap4ldHierarchy;
import org.olap4j.driver.olap4ld.Olap4ldLevel;
import org.olap4j.driver.olap4ld.Olap4ldMember;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.olap4j.metadata.Hierarchy}
 * for XML/A providers.
 *
 * @author jhyde, bkaempgen
 * @version $Id: XmlaOlap4jHierarchy.java 464 2011-07-25 21:42:07Z jhyde $
 * @since Dec 4, 2007
 */
class Olap4ldHierarchy
    extends Olap4ldElement
    implements Hierarchy, Named
{
    final Olap4ldDimension olap4jDimension;
    final NamedList<Olap4ldLevel> levels;
    private final boolean all;
    private final String defaultMemberUniqueName;

    Olap4ldHierarchy(
        Olap4ldDimension olap4jDimension,
        String uniqueName,
        String name,
        String caption,
        String description,
        boolean all,
        String defaultMemberUniqueName) throws OlapException
    {
        super(uniqueName, name, caption, description);
        assert olap4jDimension != null;
        this.olap4jDimension = olap4jDimension;
        this.all = all;
        this.defaultMemberUniqueName = defaultMemberUniqueName;

        String[] hierarchyRestrictions = {
            "CATALOG_NAME",
            olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog.getName(),
            "SCHEMA_NAME",
            olap4jDimension.olap4jCube.olap4jSchema.getName(),
            "CUBE_NAME",
            olap4jDimension.olap4jCube.getName(),
            "DIMENSION_UNIQUE_NAME",
            olap4jDimension.getUniqueName(),
            "HIERARCHY_UNIQUE_NAME",
            getUniqueName()
        };

        this.levels = new DeferredNamedListImpl<Olap4ldLevel>(
            Olap4ldConnection.MetadataRequest.MDSCHEMA_LEVELS,
            new Olap4ldConnection.Context(
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData.olap4jConnection,
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData,
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog,
                olap4jDimension.olap4jCube.olap4jSchema,
                olap4jDimension.olap4jCube,
                olap4jDimension,
                this, null),
            new Olap4ldConnection.LevelHandler(olap4jDimension.olap4jCube),
            hierarchyRestrictions);
        //int pop = levels.size();
    }

    public Dimension getDimension() {
        return olap4jDimension;
    }

    public NamedList<Level> getLevels() {
    	Olap4ldUtil._log.info("getLevels()...");
    	
        return Olap4jUtil.cast(levels);
    }

    public boolean hasAll() {
        return all;
    }

    public Member getDefaultMember() throws OlapException {
        if (defaultMemberUniqueName == null) {
            //return null;
        	// Since every hierarchy needs a default member, we simply return the very first member
        	return this.getLevels().get(0).getMembers().get(0);
        }
        return olap4jDimension.olap4jCube.getMetadataReader()
            .lookupMemberByUniqueName(
                defaultMemberUniqueName);
    }

    public NamedList<Member> getRootMembers() throws OlapException {
        final List<Olap4ldMember> memberList =
            olap4jDimension.olap4jCube.getMetadataReader().getLevelMembers(
                levels.get(0));
        final NamedList<Olap4ldMember> list =
            new NamedListImpl<Olap4ldMember>(memberList);
        return Olap4jUtil.cast(list);
    }

    public boolean equals(Object obj) {
        return (obj instanceof Olap4ldHierarchy)
            && this.uniqueName.equals(
                ((Olap4ldHierarchy) obj).getUniqueName());
    }
    
	public List<Node[]> transformMetadataObject2NxNodes(Cube cube) {
		List<Node[]> nodes = new ArrayList<Node[]>();

		// Create header

		/*
		 * ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?MEASURE_UNIQUE_NAME
		 * ?MEASURE_NAME ?MEASURE_CAPTION ?DATA_TYPE ?MEASURE_IS_VISIBLE
		 * ?MEASURE_AGGREGATOR ?EXPRESSION
		 */

		// Create header
				Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
						new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
						new Variable("?DIMENSION_UNIQUE_NAME"),
						new Variable("?HIERARCHY_UNIQUE_NAME"),
						new Variable("?HIERARCHY_NAME"),
						new Variable("?HIERARCHY_CAPTION"),
						new Variable("?DESCRIPTION"),
						new Variable("?HIERARCHY_MAX_LEVEL_NUMBER")};
		nodes.add(header);

		Node[] metadatanode = new Node[] {
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(cube.getSchema().getCatalog().getName())),
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(cube.getSchema().getName())),
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(cube.getUniqueName())),
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(this.getDimension().getUniqueName())), new Literal(Olap4ldLinkedDataUtil
								.convertMDXtoURI(this.getUniqueName())),
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(this.getName())),
				new Literal(this.getCaption()),
				new Literal(this.getDescription()),
				// Not exactly what HIERARCHY_MAX_LEVEL_NUMBER means, but should be sufficient.
				new Literal(this.getLevels().size()+"")};
		nodes.add(metadatanode);

		return nodes;
	}
}

// End XmlaOlap4jHierarchy.java
