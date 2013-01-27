/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld;

import org.olap4j.OlapException;
import org.olap4j.driver.ld.LdOlap4jDimension;
import org.olap4j.driver.ld.LdOlap4jHierarchy;
import org.olap4j.driver.ld.LdOlap4jLevel;
import org.olap4j.driver.ld.LdOlap4jMember;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.util.List;

/**
 * Implementation of {@link org.olap4j.metadata.Hierarchy}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jHierarchy.java 464 2011-07-25 21:42:07Z jhyde $
 * @since Dec 4, 2007
 */
class LdOlap4jHierarchy
    extends XmlaOlap4jElement
    implements Hierarchy, Named
{
    final LdOlap4jDimension olap4jDimension;
    final NamedList<LdOlap4jLevel> levels;
    private final boolean all;
    private final String defaultMemberUniqueName;

    LdOlap4jHierarchy(
        LdOlap4jDimension olap4jDimension,
        String uniqueName,
        String name,
        String caption,
        String description,
        boolean all,
        String defaultMemberUniqueName) throws OlapException
    {
        super(uniqueName, name, caption, description);
        assert olap4jDimension != null;
        this.olap4jDimension = olap4jDimension;
        this.all = all;
        this.defaultMemberUniqueName = defaultMemberUniqueName;

        String[] hierarchyRestrictions = {
            "CATALOG_NAME",
            olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog.getName(),
            "SCHEMA_NAME",
            olap4jDimension.olap4jCube.olap4jSchema.getName(),
            "CUBE_NAME",
            olap4jDimension.olap4jCube.getName(),
            "DIMENSION_UNIQUE_NAME",
            olap4jDimension.getUniqueName(),
            "HIERARCHY_UNIQUE_NAME",
            getUniqueName()
        };

        this.levels = new DeferredNamedListImpl<LdOlap4jLevel>(
            LdOlap4jConnection.MetadataRequest.MDSCHEMA_LEVELS,
            new LdOlap4jConnection.Context(
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData.olap4jConnection,
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData,
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog,
                olap4jDimension.olap4jCube.olap4jSchema,
                olap4jDimension.olap4jCube,
                olap4jDimension,
                this, null),
            new LdOlap4jConnection.LevelHandler(olap4jDimension.olap4jCube),
            hierarchyRestrictions);
        int pop = levels.size();
    }

    public Dimension getDimension() {
        return olap4jDimension;
    }

    public NamedList<Level> getLevels() {
    	LdOlap4jUtil._log.info("getLevels()...");
    	
        return Olap4jUtil.cast(levels);
    }

    public boolean hasAll() {
        return all;
    }

    public Member getDefaultMember() throws OlapException {
        if (defaultMemberUniqueName == null) {
            return null;
        }
        return olap4jDimension.olap4jCube.getMetadataReader()
            .lookupMemberByUniqueName(
                defaultMemberUniqueName);
    }

    public NamedList<Member> getRootMembers() throws OlapException {
        final List<LdOlap4jMember> memberList =
            olap4jDimension.olap4jCube.getMetadataReader().getLevelMembers(
                levels.get(0));
        final NamedList<LdOlap4jMember> list =
            new NamedListImpl<LdOlap4jMember>(memberList);
        return Olap4jUtil.cast(list);
    }

    public boolean equals(Object obj) {
        return (obj instanceof LdOlap4jHierarchy)
            && this.uniqueName.equals(
                ((LdOlap4jHierarchy) obj).getUniqueName());
    }
}

// End XmlaOlap4jHierarchy.java
