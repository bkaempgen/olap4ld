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
import org.olap4j.driver.olap4ld.Olap4ldCube;
import org.olap4j.driver.olap4ld.Olap4ldDimension;
import org.olap4j.driver.olap4ld.Olap4ldHierarchy;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

/**
 * Implementation of {@link org.olap4j.metadata.Dimension} for XML/A providers.
 * 
 * @author jhyde, bkaempgen
 * @version $Id: XmlaOlap4jDimension.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 4, 2007
 */
class Olap4ldDimension extends Olap4ldElement implements Dimension, Named {
	final Olap4ldCube olap4jCube;
	final Type type;
	private final NamedList<Olap4ldHierarchy> hierarchies;
	private final String defaultHierarchyUniqueName;
	private final int ordinal;

	Olap4ldDimension(Olap4ldCube olap4jCube, String uniqueName, String name,
			String caption, String description, Type type,
			String defaultHierarchyUniqueName, int ordinal) {
		super(uniqueName, name, caption, description);
		this.defaultHierarchyUniqueName = defaultHierarchyUniqueName;
		assert olap4jCube != null;
		this.olap4jCube = olap4jCube;
		this.type = type;
		this.ordinal = ordinal;

		String[] dimensionRestrictions = { "CATALOG_NAME",
				olap4jCube.olap4jSchema.olap4jCatalog.getName(), "SCHEMA_NAME",
				olap4jCube.olap4jSchema.getName(), "CUBE_NAME",
				olap4jCube.getName(), "DIMENSION_UNIQUE_NAME", getUniqueName() };

		this.hierarchies = new DeferredNamedListImpl<Olap4ldHierarchy>(
				Olap4ldConnection.MetadataRequest.MDSCHEMA_HIERARCHIES,
				new Olap4ldConnection.Context(
						olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection,
						olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData,
						olap4jCube.olap4jSchema.olap4jCatalog,
						olap4jCube.olap4jSchema, olap4jCube, this, null, null),
				new Olap4ldConnection.HierarchyHandler(olap4jCube),
				dimensionRestrictions);

		// Populate immediately?
		// hierarchies.size();
	}

	public NamedList<Hierarchy> getHierarchies() {
		Olap4ldUtil._log.info("getHierarchies()...");

		return Olap4jUtil.cast(hierarchies);
	}

	public Type getDimensionType() throws OlapException {
		return type;
	}

	public Hierarchy getDefaultHierarchy() {
		for (Olap4ldHierarchy hierarchy : hierarchies) {
			if (hierarchy.getUniqueName().equals(defaultHierarchyUniqueName)) {
				return hierarchy;
			}
		}
		return hierarchies.get(0);
	}

	public boolean equals(Object obj) {
		return (obj instanceof Olap4ldDimension)
				&& this.uniqueName.equals(((Olap4ldDimension) obj)
						.getUniqueName());
	}

	public int getOrdinal() {
		return ordinal;
	}

	public List<Node[]> transformMetadataObject2NxNodes() {
		List<Node[]> nodes = new ArrayList<Node[]>();

		// Create header
		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?DIMENSION_NAME"),
				new Variable("?DIMENSION_UNIQUE_NAME"),
				new Variable("?DIMENSION_CAPTION"),
				new Variable("?DIMENSION_ORDINAL"),
				new Variable("?DIMENSION_TYPE"), new Variable("?DESCRIPTION") };
		nodes.add(header);

		Node[] cubenode = new Node[] {
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(this.olap4jCube.getSchema().getCatalog().getName())),
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(this.olap4jCube.getSchema().getName())),
				new Literal(Olap4ldLinkedDataUtil
						.convertMDXtoURI(this.getUniqueName())), new Literal(this.getCaption()),
				new Literal(this.getOrdinal()+""),
				new Literal(this.type.toString()),
				new Literal(this.getDescription()) };
		nodes.add(cubenode);

		return nodes;
	}
}

// End XmlaOlap4jDimension.java
