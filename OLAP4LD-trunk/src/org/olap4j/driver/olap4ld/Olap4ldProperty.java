/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import org.olap4j.driver.olap4ld.Olap4ldProperty;
import org.olap4j.impl.Named;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Property;

import java.util.Set;

/**
 * Implementation of {@link org.olap4j.metadata.Property}
 * for properties defined as part of the definition of a level or measure
 * from XML/A providers.
 *
 * @see org.olap4j.driver.olap4ld.Olap4ldCellProperty
 * @see org.olap4j.driver.olap4ld.Olap4ldCellSetMemberProperty
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jProperty.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 9, 2007
 */
class Olap4ldProperty
    extends Olap4ldElement
    implements Property, Named
{
    private final Datatype datatype;
    private final Set<TypeFlag> type;
    private final ContentType contentType;

    Olap4ldProperty(
        String uniqueName,
        String name,
        String caption,
        String description,
        Datatype datatype,
        Set<TypeFlag> type,
        ContentType contentType)
    {
        super(uniqueName, name, caption, description);
        this.contentType = contentType;
        assert datatype != null;
        assert type != null;
        this.datatype = datatype;
        this.type = type;
    }

    public Datatype getDatatype() {
        return datatype;
    }

    public Set<TypeFlag> getType() {
        return type;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public boolean equals(Object obj) {
        return (obj instanceof Olap4ldProperty)
            && this.uniqueName.equals(
                ((Olap4ldProperty) obj).getUniqueName());
    }
}

// End XmlaOlap4jProperty.java
