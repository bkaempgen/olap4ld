/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import org.olap4j.impl.Named;
import org.olap4j.metadata.MetadataElement;

/**
 * Abstract implementation of {@link MetadataElement}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jElement.java 373 2010-12-02 22:06:52Z jhyde $
 * @since Dec 5, 2007
 */
abstract class Olap4ldElement implements MetadataElement, Named {
    protected final String uniqueName;
    protected final String name;
    protected final String caption;
    protected final String description;
    private int hash = 0;

    Olap4ldElement(
        String uniqueName,
        String name,
        String caption,
        String description)
    {
        assert uniqueName != null;
        assert description != null;
        assert name != null;
        assert caption != null;
        this.description = description;
        this.uniqueName = uniqueName;
        this.caption = caption;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Note, getUniqueName is automatically wrapped with squared brackets by the system.
     */
    public String getUniqueName() {
        return uniqueName;
    }

    public String getCaption() {
        return caption;
    }

    public String getDescription() {
        return description;
    }

    public boolean isVisible() {
        return true;
    }

    public int hashCode() {
        // By the book implementation of a hash code identifier.
        if (this.hash == 0) {
            hash = (getClass().hashCode() << 8) ^ getUniqueName().hashCode();
        }
        return hash;
    }

    // Keep this declaration abstract as a reminder to
    // overriding classes.
    abstract public boolean equals(Object obj);
}

// End XmlaOlap4jElement.java
