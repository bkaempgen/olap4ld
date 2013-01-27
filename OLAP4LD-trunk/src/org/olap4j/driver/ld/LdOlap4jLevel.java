/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld;

import org.olap4j.OlapException;
import org.olap4j.driver.ld.LdOlap4jHierarchy;
import org.olap4j.driver.ld.LdOlap4jLevel;
import org.olap4j.driver.ld.LdOlap4jMeasure;
import org.olap4j.driver.ld.LdOlap4jMember;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.metadata.Level}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jLevel.java 383 2010-12-20 21:21:45Z lucboudreau $
 * @since Dec 4, 2007
 */
class LdOlap4jLevel
    extends XmlaOlap4jElement
    implements Level, Named
{
    final LdOlap4jHierarchy olap4jHierarchy;
    private final int depth;
    private final Type type;
    private final int cardinality;
    private final NamedList<XmlaOlap4jProperty> propertyList;
    final NamedList<LdOlap4jMember> memberList;
    private final boolean calculated;

    /**
     * Creates an XmlaOlap4jLevel.
     *
     * @param olap4jHierarchy Hierarchy
     * @param uniqueName Unique name
     * @param name Name
     * @param caption Caption
     * @param description Description
     * @param depth Distance to root
     * @param type Level type
     * @param calculated Whether level is calculated
     * @param cardinality Number of members in this level
     */
    LdOlap4jLevel(
        final LdOlap4jHierarchy olap4jHierarchy,
        String uniqueName, String name,
        String caption,
        String description,
        int depth,
        Type type,
        boolean calculated,
        int cardinality)
    {
        super(uniqueName, name, caption, description);
        assert olap4jHierarchy != null;
        this.type = type;
        this.calculated = calculated;
        this.cardinality = cardinality;
        this.depth = depth;
        this.olap4jHierarchy = olap4jHierarchy;

        String[] levelRestrictions = {
                "CATALOG_NAME",
                olap4jHierarchy.olap4jDimension.olap4jCube
                    .olap4jSchema.olap4jCatalog.getName(),
                "SCHEMA_NAME",
                olap4jHierarchy.olap4jDimension.olap4jCube
                    .olap4jSchema.getName(),
                "CUBE_NAME",
                olap4jHierarchy.olap4jDimension.olap4jCube.getName(),
                "DIMENSION_UNIQUE_NAME",
                olap4jHierarchy.olap4jDimension.getUniqueName(),
                "HIERARCHY_UNIQUE_NAME",
                olap4jHierarchy.getUniqueName(),
                "LEVEL_UNIQUE_NAME",
                getUniqueName()
            };

        this.propertyList = new DeferredNamedListImpl<XmlaOlap4jProperty>(
            LdOlap4jConnection.MetadataRequest.MDSCHEMA_PROPERTIES,
            new LdOlap4jConnection.Context(this),
            new LdOlap4jConnection.PropertyHandler(),
            levelRestrictions);

        try {
        	/* 
        	 * If this level is from the measure dimension, then we create a list that will
        	 * be populated by all measures of the cube.
        	 */
            if (olap4jHierarchy.olap4jDimension.getDimensionType()
                == Dimension.Type.MEASURE)
            {
            	// In case of measures, the restrictions only give the cube name
                String[] restrictions = {
                    "CATALOG_NAME",
                    olap4jHierarchy.olap4jDimension.olap4jCube.olap4jSchema
                        .olap4jCatalog.getName(),
                    "SCHEMA_NAME",
                    olap4jHierarchy.olap4jDimension.olap4jCube.olap4jSchema
                        .getName(),
                    "CUBE_NAME",
                    olap4jHierarchy.olap4jDimension.olap4jCube.getName()
                };
                this.memberList = Olap4jUtil.cast(
                    new DeferredNamedListImpl<LdOlap4jMeasure>(
                        LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEASURES,
                        new LdOlap4jConnection.Context(
                            olap4jHierarchy.olap4jDimension
                                .olap4jCube.olap4jSchema
                                .olap4jCatalog.olap4jDatabaseMetaData
                                .olap4jConnection,
                            olap4jHierarchy.olap4jDimension
                                .olap4jCube.olap4jSchema
                                .olap4jCatalog.olap4jDatabaseMetaData,
                            olap4jHierarchy.olap4jDimension.olap4jCube
                                .olap4jSchema.olap4jCatalog,
                            olap4jHierarchy.olap4jDimension.olap4jCube
                                .olap4jSchema,
                            olap4jHierarchy.olap4jDimension.olap4jCube,
                            olap4jHierarchy.olap4jDimension,
                            olap4jHierarchy,
                            this),
                        new LdOlap4jConnection.MeasureHandler(),
                            restrictions));
                //LdOlap4jMember pop = memberList.get(0);
            } else {
            	// If this level is a typical level, then we simply query for its normal members.
            	// Here, the restrictions give the everything up to the level.
                this.memberList = new DeferredNamedListImpl<LdOlap4jMember>(
                    LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEMBERS,
                    new LdOlap4jConnection.Context(
                        olap4jHierarchy.olap4jDimension.olap4jCube.olap4jSchema
                            .olap4jCatalog.olap4jDatabaseMetaData
                            .olap4jConnection,
                        olap4jHierarchy.olap4jDimension.olap4jCube.olap4jSchema
                            .olap4jCatalog.olap4jDatabaseMetaData,
                        olap4jHierarchy.olap4jDimension.olap4jCube.olap4jSchema
                            .olap4jCatalog,
                        olap4jHierarchy.olap4jDimension.olap4jCube.olap4jSchema,
                        olap4jHierarchy.olap4jDimension.olap4jCube,
                        olap4jHierarchy.olap4jDimension,
                        olap4jHierarchy,
                        this),
                    new LdOlap4jConnection.MemberHandler(),
                    levelRestrictions);
                //LdOlap4jMember pop = memberList.get(0);
            }
        } catch (OlapException e) {
            throw new RuntimeException("Programming error", e);
        }
    }

    public int getDepth() {
        return depth;
    }

    public Hierarchy getHierarchy() {
        return olap4jHierarchy;
    }

    public Dimension getDimension() {
        return olap4jHierarchy.olap4jDimension;
    }

    public boolean isCalculated() {
        return calculated;
    }

    public Type getLevelType() {
        return type;
    }

    public NamedList<Property> getProperties() {
        final NamedList<Property> list = new ArrayNamedListImpl<Property>() {
            protected String getName(Property property) {
                return property.getName();
            }

			@Override
			public String getName(Object element) {
				// TODO Auto-generated method stub
				return null;
			}
        };
        // standard properties first
        list.addAll(
            Arrays.asList(Property.StandardMemberProperty.values()));
        // then level-specific properties
        list.addAll(propertyList);
        return list;
    }

    public List<Member> getMembers() throws OlapException {
    	LdOlap4jUtil._log.info("getMembers()...");
    	
        return Olap4jUtil.cast(this.memberList);
    }

    public int getCardinality() {
        return cardinality;
    }

    public boolean equals(Object obj) {
        return (obj instanceof LdOlap4jLevel)
            && this.uniqueName.equals(
                ((LdOlap4jLevel) obj).getUniqueName());
    }
}

// End XmlaOlap4jLevel.java
