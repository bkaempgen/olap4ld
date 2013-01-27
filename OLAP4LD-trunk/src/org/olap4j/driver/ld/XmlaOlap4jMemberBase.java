/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld;

import org.olap4j.driver.ld.LdOlap4jConnection;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Property;

import java.util.Map;

/**
 * Core interface shared by all implementations of {@link Member} in the XMLA
 * driver.
 *
 * <p>This interface is private within the {@code org.olap4j.driver.ld}
 * package. The methods in this interface are NOT part of the public olap4j API.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jMemberBase.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Nov 1, 2008
 */
interface XmlaOlap4jMemberBase
    extends Member
{
    /**
     * Returns the cube this member belongs to.
     */
    LdOlap4jCube getCube();

    /**
     * Returns the connection that created this member.
     */
    LdOlap4jConnection getConnection();

    /**
     * Returns the catalog that this member belongs to.
     */
    LdOlap4jCatalog getCatalog();

    /**
     * Returns the set of property values, keyed by property.
     */
    Map<Property, Object> getPropertyValueMap();
}

// End XmlaOlap4jMemberBase.java
