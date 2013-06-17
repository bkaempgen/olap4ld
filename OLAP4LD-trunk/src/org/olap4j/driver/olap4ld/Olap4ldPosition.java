/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import org.olap4j.Position;
import org.olap4j.metadata.Member;

import java.util.List;

/**
 * Implementation of {@link org.olap4j.Position}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jPosition.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 5, 2007
 */
class Olap4ldPosition implements Position {
    private final int ordinal;
    private final List<Member> members;

    public Olap4ldPosition(List<Member> members, int ordinal) {
        this.members = members;
        this.ordinal = ordinal;
    }

    public List<Member> getMembers() {
        return members;
    }

    public int getOrdinal() {
        return ordinal;
    }
}

// End XmlaOlap4jPosition.java
