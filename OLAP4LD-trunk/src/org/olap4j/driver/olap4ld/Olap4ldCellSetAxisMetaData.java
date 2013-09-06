/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import java.util.List;

import org.olap4j.Axis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Property;

/**
 * Implementation of {@link org.olap4j.CellSetMetaData}
 * for XML/A providers.
 *
 * @author jhyde, bkaempgen
 * @version $Id: XmlaOlap4jCellSetAxisMetaData.java 315 2010-05-29 00:56:11Z jhyde $
* @since Nov 17, 2007
*/
class Olap4ldCellSetAxisMetaData implements CellSetAxisMetaData {
    private final Axis axis;
    private final List<Hierarchy> hierarchyList;
    private final List<Olap4ldCellSetMemberProperty> propertyList;

    Olap4ldCellSetAxisMetaData(
        Olap4ldConnection olap4jConnection,
        Axis axis,
        List<Hierarchy> hierarchyList,
        List<Olap4ldCellSetMemberProperty> propertyList)
    {
        this.axis = axis;
        this.hierarchyList = hierarchyList;
        this.propertyList = propertyList;
    }

    public Axis getAxisOrdinal() {
        return axis;
    }

    public List<Hierarchy> getHierarchies() {
        return hierarchyList;
    }

    public List<Property> getProperties() {
        return Olap4jUtil.cast(propertyList);
    }

    Olap4ldCellSetMemberProperty lookupProperty(
        String hierarchyName,
        String tag)
    {
        for (Olap4ldCellSetMemberProperty property : propertyList) {
            if (property.hierarchy.getName().equals(hierarchyName)
                && property.tag.equals(tag))
            {
                return property;
            }
        }
        return null;
    }
}

// End XmlaOlap4jCellSetAxisMetaData.java
