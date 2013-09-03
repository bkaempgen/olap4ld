/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.olap4ld;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.olap4j.impl.Named;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Property;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

/**
 * Implementation of {@link org.olap4j.metadata.Measure} for XML/A providers.
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jMeasure.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 4, 2007
 */
class Olap4ldMeasure extends Olap4ldMember implements Measure, Named {
	private final Aggregator aggregator;
	private final Datatype datatype;
	private final boolean visible;

	/**
	 * Creates an XmlaOlap4jMeasure.
	 * 
	 * @param olap4jLevel
	 *            Level
	 * @param uniqueName
	 *            Unique name
	 * @param name
	 *            Name
	 * @param caption
	 *            Caption
	 * @param description
	 *            Description
	 * @param parentMemberUniqueName
	 *            Unique name of parent, or null if no parent
	 * @param aggregator
	 *            Aggregator
	 * @param datatype
	 *            Data type
	 * @param visible
	 *            Whether visible
	 * @param ordinal
	 *            Ordinal in its hierarchy
	 */
	Olap4ldMeasure(Olap4ldLevel olap4jLevel, String uniqueName, String name,
			String caption, String description, String parentMemberUniqueName,
			Aggregator aggregator, ParseTreeNode expression, Datatype datatype,
			boolean visible, int ordinal) {
		super(olap4jLevel, uniqueName, name, caption, description,
				parentMemberUniqueName,
				aggregator == Aggregator.CALCULATED ? Type.FORMULA
						: Type.MEASURE, 0, ordinal, Collections
						.<Property, Object> emptyMap(), expression);
		assert olap4jLevel.olap4jHierarchy.olap4jDimension.type == Dimension.Type.MEASURE;
		this.aggregator = aggregator;
		this.datatype = datatype;
		this.visible = visible;
	}

	public Aggregator getAggregator() {
		return aggregator;
	}

	public Datatype getDatatype() {
		return datatype;
	}

	public boolean isVisible() {
		return visible;
	}

	public List<Node[]> transformMetadataObject2NxNodes(Cube cube) {
		List<Node[]> nodes = new ArrayList<Node[]>();

		// Create header

		/*
		 * ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?MEASURE_UNIQUE_NAME
		 * ?MEASURE_NAME ?MEASURE_CAPTION ?DATA_TYPE ?MEASURE_IS_VISIBLE
		 * ?MEASURE_AGGREGATOR ?EXPRESSION
		 */

		Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
				new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
				new Variable("?MEASURE_UNIQUE_NAME"),
				new Variable("?MEASURE_NAME"),
				new Variable("?MEASURE_CAPTION"), new Variable("?DATA_TYPE"),
				new Variable("?MEASURE_IS_VISIBLE"),
				new Variable("?MEASURE_AGGREGATOR"),
				new Variable("?EXPRESSION") };
		nodes.add(header);

		Node[] cubenode = new Node[] {
				new Literal(cube.getSchema().getCatalog().getName()),
				new Literal(cube.getSchema().getName()),
				new Literal(cube.getUniqueName()),
				new Literal(this.getUniqueName()), new Literal(this.getName()),
				new Literal(this.getCaption()),
				new Literal(this.getDatatype().toString()),
				new Literal(this.isVisible()+""),
				new Literal("http://purl.org/olap#"+this.getAggregator().toString()),
				new Literal(this.getDescription()) };
		nodes.add(cubenode);

		return nodes;
	}
}

// End XmlaOlap4jMeasure.java
