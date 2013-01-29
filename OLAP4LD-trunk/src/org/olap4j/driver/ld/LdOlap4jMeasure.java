/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld;

import java.util.Collections;

import org.olap4j.impl.Named;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Property;

/**
 * Implementation of {@link org.olap4j.metadata.Measure}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jMeasure.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 4, 2007
 */
class LdOlap4jMeasure
    extends LdOlap4jMember
    implements Measure, Named
{
    private final Aggregator aggregator;
    private final Datatype datatype;
    private final boolean visible;

    /**
     * Creates an XmlaOlap4jMeasure.
     *
     * @param olap4jLevel Level
     * @param uniqueName Unique name
     * @param name Name
     * @param caption Caption
     * @param description Description
     * @param parentMemberUniqueName Unique name of parent, or null if no parent
     * @param aggregator Aggregator
     * @param datatype Data type
     * @param visible Whether visible
     * @param ordinal Ordinal in its hierarchy
     */
    LdOlap4jMeasure(
        LdOlap4jLevel olap4jLevel,
        String uniqueName,
        String name,
        String caption,
        String description,
        String parentMemberUniqueName,
        Aggregator aggregator,
        ParseTreeNode expression,
        Datatype datatype,
        boolean visible,
        int ordinal)
    {
        super(
            olap4jLevel, uniqueName, name, caption, description,
            parentMemberUniqueName,
            aggregator == Aggregator.CALCULATED ? Type.FORMULA : Type.MEASURE,
            0, ordinal, Collections.<Property, Object>emptyMap(), expression);
        assert olap4jLevel.olap4jHierarchy.olap4jDimension.type
            == Dimension.Type.MEASURE;
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
    
    
}

// End XmlaOlap4jMeasure.java