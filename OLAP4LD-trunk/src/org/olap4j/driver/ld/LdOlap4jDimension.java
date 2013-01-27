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
import org.olap4j.driver.ld.LdOlap4jCube;
import org.olap4j.driver.ld.LdOlap4jDimension;
import org.olap4j.driver.ld.LdOlap4jHierarchy;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

/**
 * Implementation of {@link org.olap4j.metadata.Dimension}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jDimension.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 4, 2007
 */
class LdOlap4jDimension
    extends XmlaOlap4jElement
    implements Dimension, Named
{
    final LdOlap4jCube olap4jCube;
    final Type type;
    private final NamedList<LdOlap4jHierarchy> hierarchies;
    private final String defaultHierarchyUniqueName;
    private final int ordinal;

    LdOlap4jDimension(
        LdOlap4jCube olap4jCube,
        String uniqueName,
        String name,
        String caption,
        String description,
        Type type,
        String defaultHierarchyUniqueName,
        int ordinal)
    {
        super(uniqueName, name, caption, description);
        this.defaultHierarchyUniqueName = defaultHierarchyUniqueName;
        assert olap4jCube != null;
        this.olap4jCube = olap4jCube;
        this.type = type;
        this.ordinal = ordinal;

        String[] dimensionRestrictions = {
            "CATALOG_NAME",
            olap4jCube.olap4jSchema.olap4jCatalog.getName(),
            "SCHEMA_NAME",
            olap4jCube.olap4jSchema.getName(),
            "CUBE_NAME",
            olap4jCube.getName(),
            "DIMENSION_UNIQUE_NAME",
            getUniqueName()
        };

        this.hierarchies = new DeferredNamedListImpl<LdOlap4jHierarchy>(
            LdOlap4jConnection.MetadataRequest.MDSCHEMA_HIERARCHIES,
            new LdOlap4jConnection.Context(
                olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData.olap4jConnection,
                olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData,
                olap4jCube.olap4jSchema.olap4jCatalog,
                olap4jCube.olap4jSchema,
                olap4jCube,
                this, null, null),
            new LdOlap4jConnection.HierarchyHandler(olap4jCube),
            dimensionRestrictions);
        int pop = hierarchies.size();
    }

    public NamedList<Hierarchy> getHierarchies() {
    	LdOlap4jUtil._log.info("getHierarchies()...");
    	
        return Olap4jUtil.cast(hierarchies);
    }

    public Type getDimensionType() throws OlapException {
        return type;
    }

    public Hierarchy getDefaultHierarchy() {
        for (LdOlap4jHierarchy hierarchy : hierarchies) {
            if (hierarchy.getUniqueName().equals(defaultHierarchyUniqueName)) {
                return hierarchy;
            }
        }
        return hierarchies.get(0);
    }

    public boolean equals(Object obj) {
        return (obj instanceof LdOlap4jDimension)
            && this.uniqueName.equals(
                ((LdOlap4jDimension) obj).getUniqueName());
    }

    public int getOrdinal() {
        return ordinal;
    }
}

// End XmlaOlap4jDimension.java
