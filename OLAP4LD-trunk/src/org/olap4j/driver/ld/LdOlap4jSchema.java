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
import org.olap4j.driver.ld.LdOlap4jCatalog;
import org.olap4j.driver.ld.LdOlap4jCube;
import org.olap4j.driver.ld.LdOlap4jDimension;
import org.olap4j.driver.ld.LdOlap4jSchema;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.metadata.Schema} for XML/A providers.
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jSchema.java 465 2011-07-26 00:59:31Z jhyde $
 * @since May 24, 2007
 */
class LdOlap4jSchema implements Schema, Named {
	final LdOlap4jCatalog olap4jCatalog;
	private final String name;
	/**
	 * This attribute is not final any more, since I want to assign it everytime
	 * getCubes are asked for.
	 */
	private NamedList<LdOlap4jCube> cubes;
	private final NamedList<LdOlap4jDimension> sharedDimensions;

	LdOlap4jSchema(LdOlap4jCatalog olap4jCatalog, String name)
			throws OlapException {
		if (olap4jCatalog == null) {
			throw new NullPointerException("Catalog cannot be null.");
		}
		if (name == null) {
			throw new NullPointerException("Name cannot be null.");
		}

		this.olap4jCatalog = olap4jCatalog;
		this.name = name;
		
		// Dummy cube to own shared dimensions.
		// final XmlaOlap4jCube sharedCube =
		// new XmlaOlap4jCube(this, "", "", "");
		//
		// final XmlaOlap4jConnection.Context context =
		// new XmlaOlap4jConnection.Context(
		// olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection,
		// olap4jCatalog.olap4jDatabaseMetaData,
		// olap4jCatalog,
		// this,
		// sharedCube, null, null, null);

		// For us, the entire cube contains all shared dimensions
		final LdOlap4jConnection.Context context = new LdOlap4jConnection.Context(
				olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection,
				olap4jCatalog.olap4jDatabaseMetaData, olap4jCatalog, this,
				null, null, null, null);

		// Cubes
		/*
		 * TODO: POssibly I need a cube with no name with the shared dimensions.
		 * At least, the dummy cube is in the context, but not in the
		 * restrictions.
		 */

		this.cubes = new DeferredNamedListImpl<LdOlap4jCube>(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_CUBES, context,
				new LdOlap4jConnection.CubeHandler(), null);
		//LdOlap4jCube pop = cubes.get(0);

		// TODO: Shared dimensions are all dimensions that are used by several
		// cubes. For now, we simply return an empty list.
		// The cube that owns all shared dimensions is simply the only one
		// XmlaOlap4jCube sharedCube = (XmlaOlap4jCube) getCubes().get(0);

		// Restriction on cube with shared dimensions only
		// TODO: We only have one cube with shared dimensions
		// String[] restrictions = { "CATALOG_NAME", olap4jCatalog.getName(),
		// "SCHEMA_NAME", getName(), "CUBE_NAME", getCubes().get(0).getName() };

		// Shared Dimensions
		/*
		 * All shared dimensions are given a special cube without a name
		 */
		this.sharedDimensions = new DeferredNamedListImpl<LdOlap4jDimension>(
				null, null, null, null);
		// this.sharedDimensions = new
		// DeferredNamedListImpl<XmlaOlap4jDimension>(
		// XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_DIMENSIONS,
		// context, new XmlaOlap4jConnection.DimensionHandler(sharedCube),
		// restrictions);

	}

	public int hashCode() {
		return name.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj instanceof LdOlap4jSchema) {
			LdOlap4jSchema that = (LdOlap4jSchema) obj;
			return this.name.equals(that.name)
					&& this.olap4jCatalog.equals(that.olap4jCatalog);
		}
		return false;
	}

	public Catalog getCatalog() {
		return olap4jCatalog;
	}

	public String getName() {
		return name;
	}

	public NamedList<Cube> getCubes() throws OlapException {
		LdOlap4jUtil._log.info("Schema.getCubes()...");

		return Olap4jUtil.cast(cubes);
	}

	public NamedList<Dimension> getSharedDimensions() throws OlapException {
		return Olap4jUtil.cast(sharedDimensions);
	}

	public Collection<Locale> getSupportedLocales() throws OlapException {
		return Collections.emptyList();
	}
}

// End XmlaOlap4jSchema.java
