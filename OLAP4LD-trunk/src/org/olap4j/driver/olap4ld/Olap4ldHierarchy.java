/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldDimension;
import org.olap4j.driver.olap4ld.Olap4ldHierarchy;
import org.olap4j.driver.olap4ld.Olap4ldLevel;
import org.olap4j.driver.olap4ld.Olap4ldMember;
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
class Olap4ldHierarchy
    extends Olap4ldElement
    implements Hierarchy, Named
{
    final Olap4ldDimension olap4jDimension;
    final NamedList<Olap4ldLevel> levels;
    private final boolean all;
    private final String defaultMemberUniqueName;

    Olap4ldHierarchy(
        Olap4ldDimension olap4jDimension,
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

        this.levels = new DeferredNamedListImpl<Olap4ldLevel>(
            Olap4ldConnection.MetadataRequest.MDSCHEMA_LEVELS,
            new Olap4ldConnection.Context(
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData.olap4jConnection,
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog
                    .olap4jDatabaseMetaData,
                olap4jDimension.olap4jCube.olap4jSchema.olap4jCatalog,
                olap4jDimension.olap4jCube.olap4jSchema,
                olap4jDimension.olap4jCube,
                olap4jDimension,
                this, null),
            new Olap4ldConnection.LevelHandler(olap4jDimension.olap4jCube),
            hierarchyRestrictions);
        //int pop = levels.size();
    }

    public Dimension getDimension() {
        return olap4jDimension;
    }

    public NamedList<Level> getLevels() {
    	Olap4ldUtil._log.info("getLevels()...");
    	
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
        final List<Olap4ldMember> memberList =
            olap4jDimension.olap4jCube.getMetadataReader().getLevelMembers(
                levels.get(0));
        final NamedList<Olap4ldMember> list =
            new NamedListImpl<Olap4ldMember>(memberList);
        return Olap4jUtil.cast(list);
    }

    public boolean equals(Object obj) {
        return (obj instanceof Olap4ldHierarchy)
            && this.uniqueName.equals(
                ((Olap4ldHierarchy) obj).getUniqueName());
    }
}

// End XmlaOlap4jHierarchy.java
