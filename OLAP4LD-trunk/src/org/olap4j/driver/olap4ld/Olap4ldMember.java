/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldCatalog;
import org.olap4j.driver.olap4ld.Olap4ldCube;
import org.olap4j.driver.olap4ld.Olap4ldDimension;
import org.olap4j.driver.olap4ld.Olap4ldHierarchy;
import org.olap4j.driver.olap4ld.Olap4ldLevel;
import org.olap4j.driver.olap4ld.Olap4ldMember;
import org.olap4j.impl.*;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.*;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.metadata.Member}
 * for XML/A providers.
 *
 * <p>TODO:<ol>
 * <li>create members with a pointer to their parent member (not the name)</li>
 * <li>implement a member cache (by unique name, belongs to cube, soft)</li>
 * <li>implement Hierarchy.getRootMembers</li>
 * </ol>
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jMember.java 372 2010-12-02 20:24:17Z jhyde $
 * @since Dec 5, 2007
 */
class Olap4ldMember
    extends Olap4ldElement
    implements Olap4ldMemberBase, Member, Named
{
    private final Olap4ldLevel olap4jLevel;

    // TODO: We would rather have a refernce to the parent member, but it is
    // tricky to populate
    /*
    private final XmlaOlap4jMember parentMember;
    */
    private final String parentMemberUniqueName;
    private final Type type;
    private Olap4ldMember parentMember;
    private final int childMemberCount;
    private final int ordinal;
    private final Map<Property, Object> propertyValueMap;
    // Expression directly as ParseTreeNode
    private final ParseTreeNode expression;

    /**
     * Creates an XmlaOlap4jMember.
     *
     * @param olap4jLevel Level
     * @param uniqueName Unique name
     * @param name Name
     * @param caption Caption
     * @param description Description
     * @param parentMemberUniqueName Unique name of parent, or null if no parent
     * @param type Type
     * @param childMemberCount Number of children
     * @param ordinal Ordinal in its hierarchy
     * @param propertyValueMap Property values
     */
    Olap4ldMember(
        Olap4ldLevel olap4jLevel,
        String uniqueName,
        String name,
        String caption,
        String description,
        String parentMemberUniqueName,
        Type type,
        int childMemberCount,
        int ordinal,
        Map<Property, Object> propertyValueMap, ParseTreeNode expression)
    {
        super(uniqueName, name, caption, description);
        this.ordinal = ordinal;
        assert olap4jLevel != null;
        assert type != null;
        this.olap4jLevel = olap4jLevel;
        this.parentMemberUniqueName = parentMemberUniqueName;
        this.type = type;
        this.childMemberCount = childMemberCount;
        this.propertyValueMap = UnmodifiableArrayMap.of(propertyValueMap);
        this.expression = expression;
    }

    public int hashCode() {
        return uniqueName.hashCode();
    }

    public boolean equals(Object obj) {
        return obj instanceof Olap4ldMember
            && ((Olap4ldMember) obj).uniqueName.equals(uniqueName);
    }

    public NamedList<? extends Member> getChildMembers() throws OlapException {
        final NamedList<Olap4ldMember> list =
            new NamedListImpl<Olap4ldMember>();
        getCube()
            .getMetadataReader()
            .lookupMemberRelatives(
                Olap4jUtil.enumSetOf(TreeOp.CHILDREN),
                uniqueName,
                list);
        return list;
    }

    public int getChildMemberCount() {
        return childMemberCount;
    }

    public Olap4ldMember getParentMember() {
        if (parentMemberUniqueName == null) {
            return null;
        }
        if (parentMember == null) {
            try {
                parentMember =
                    getCube().getMetadataReader()
                        .lookupMemberByUniqueName(parentMemberUniqueName);
            } catch (OlapException e) {
                throw new RuntimeException("We should find this parent member!"); // FIXME
            }
        }
        return parentMember;
    }

    public Olap4ldLevel getLevel() {
        return olap4jLevel;
    }

    public Olap4ldHierarchy getHierarchy() {
        return olap4jLevel.olap4jHierarchy;
    }

    public Olap4ldDimension getDimension() {
        return olap4jLevel.olap4jHierarchy.olap4jDimension;
    }

    public Type getMemberType() {
        return type;
    }

    public boolean isAll() {
        return type == Type.ALL;
    }

    public boolean isChildOrEqualTo(Member member) {
        throw new UnsupportedOperationException();
    }

    public boolean isCalculated() {
        return type == Type.FORMULA;
    }

    public int getSolveOrder() {
        throw new UnsupportedOperationException();
    }

    /**
     * Simply return expression that we have stored with the member.
     */
    public ParseTreeNode getExpression() {
        return expression;
    }

    public List<Member> getAncestorMembers() {
        final List<Member> list = new ArrayList<Member>();
        Olap4ldMember m = getParentMember();
        while (m != null) {
            list.add(m);
            m = m.getParentMember();
        }
        return list;
    }

    public boolean isCalculatedInQuery() {
        throw new UnsupportedOperationException();
    }

    public Object getPropertyValue(Property property) throws OlapException {
        return getPropertyValue(
            property,
            this,
            propertyValueMap);
    }

    /**
     * Helper method to retrieve the value of a property from a member.
     *
     * @param property Property
     * @param member Member
     * @param propertyValueMap Map of property-value pairs
     * @return Property value
     *
     * @throws OlapException if database error occurs while evaluating
     *   CHILDREN_CARDINALITY; no other property throws
     */
    static Object getPropertyValue(
        Property property,
        Olap4ldMemberBase member,
        Map<Property, Object> propertyValueMap)
        throws OlapException
    {
        // If property map contains a value for this property (even if that
        // value is null), that overrides.
        final Object value = propertyValueMap.get(property);
        if (value != null || propertyValueMap.containsKey(property)) {
            return value;
        }
        if (property instanceof Property.StandardMemberProperty) {
            Property.StandardMemberProperty o =
                (Property.StandardMemberProperty) property;
            switch (o) {
            case MEMBER_CAPTION:
                return member.getCaption();
            case MEMBER_NAME:
                return member.getName();
            case MEMBER_UNIQUE_NAME:
                return member.getUniqueName();
            case CATALOG_NAME:
                return member.getCatalog().getName();
            case CHILDREN_CARDINALITY:
                return member.getChildMemberCount();
            case CUBE_NAME:
                return member.getCube().getName();
            case DEPTH:
                return member.getDepth();
            case DESCRIPTION:
                return member.getDescription();
            case DIMENSION_UNIQUE_NAME:
                return member.getDimension().getUniqueName();
            case DISPLAY_INFO:
                // TODO:
                return null;
            case HIERARCHY_UNIQUE_NAME:
                return member.getHierarchy().getUniqueName();
            case LEVEL_NUMBER:
                return member.getLevel().getDepth();
            case LEVEL_UNIQUE_NAME:
                return member.getLevel().getUniqueName();
            case MEMBER_GUID:
                // TODO:
                return null;
            case MEMBER_ORDINAL:
                return member.getOrdinal();
            case MEMBER_TYPE:
                return member.getMemberType();
            case PARENT_COUNT:
            	return member.getParentMember() == null
                ? 0
                : 1;
            case PARENT_LEVEL:
                return member.getParentMember() == null
                    ? 0
                    : member.getParentMember().getLevel().getDepth();
            case PARENT_UNIQUE_NAME:
                return member.getParentMember() == null
                    ? null
                    : member.getParentMember().getUniqueName();
            case SCHEMA_NAME:
                return member.getCube().olap4jSchema.getName();
            case VALUE:
                // TODO:
                return null;
            }
        }
        return null;
    }

    // convenience method - not part of olap4j API
    public Olap4ldCube getCube() {
        return olap4jLevel.olap4jHierarchy.olap4jDimension.olap4jCube;
    }

    // convenience method - not part of olap4j API
    public Olap4ldCatalog getCatalog() {
        return olap4jLevel.olap4jHierarchy.olap4jDimension.olap4jCube
            .olap4jSchema.olap4jCatalog;
    }

    // convenience method - not part of olap4j API
    public Olap4ldConnection getConnection() {
        return olap4jLevel.olap4jHierarchy.olap4jDimension.olap4jCube
            .olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
            .olap4jConnection;
    }

    // convenience method - not part of olap4j API
    public Map<Property, Object> getPropertyValueMap() {
        return propertyValueMap;
    }

    public String getPropertyFormattedValue(Property property)
        throws OlapException
    {
        // FIXME: need to use a format string; but what format string; and how
        // to format the property on the client side?
        return String.valueOf(getPropertyValue(property));
    }

    public void setProperty(Property property, Object value) {
        propertyValueMap.put(property, value);
    }

    public NamedList<Property> getProperties() {
        return olap4jLevel.getProperties();
    }

    public int getOrdinal() {
        return ordinal;
    }

    public boolean isHidden() {
        throw new UnsupportedOperationException();
    }

    public int getDepth() {
        // Since in regular hierarchies members have the same depth as their
        // level, we store depth as a property only where it is different.
        final Object depth =
            propertyValueMap.get(Property.StandardMemberProperty.DEPTH);
        if (depth == null) {
            return olap4jLevel.getDepth();
        } else {
            return toInteger(depth);
        }
    }

    /**
     * Converts an object to an integer value. Must not be null.
     *
     * @param o Object
     * @return Integer value
     */
    static int toInteger(Object o) {
        if (o instanceof Number) {
            Number number = (Number) o;
            return number.intValue();
        }
        return Integer.valueOf(o.toString());
    }

    public Member getDataMember() {
        throw new UnsupportedOperationException();
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
				new Variable("?LEVEL_UNIQUE_NAME"),
				new Variable("?LEVEL_NUMBER"), new Variable("?MEMBER_NAME"),
				new Variable("?MEMBER_UNIQUE_NAME"),
				new Variable("?MEMBER_CAPTION"), new Variable("?MEMBER_TYPE"),
				new Variable("?PARENT_UNIQUE_NAME"),
				new Variable("?PARENT_LEVEL") };
		nodes.add(header);

		Node[] metadatanode = new Node[] {
				new Literal(cube.getSchema().getCatalog().getName()),
				new Literal(cube.getSchema().getName()),
				new Literal(cube.getUniqueName()),
				new Literal(this.getDimension().getUniqueName()), new Literal(this.getHierarchy().getUniqueName()),
				new Literal(this.getLevel().getUniqueName()),
				new Literal(this.getLevel().getDepth()+""),
				new Literal(this.getName()),
				new Literal(this.getUniqueName()),
				// Actually, no direct correspondence; however, not important now.
				new Literal(this.getCaption()),
				new Literal(this.getMemberType().toString()),
				new Literal(this.getParentMember().getUniqueName()),
				new Literal(this.getParentMember().getLevel().getDepth()+"")
				};
		nodes.add(metadatanode);

		return nodes;
	}
}

// End XmlaOlap4jMember.java
