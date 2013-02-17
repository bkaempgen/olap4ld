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
import org.olap4j.driver.ld.LdOlap4jCube;
import org.olap4j.driver.ld.LdOlap4jDimension;
import org.olap4j.driver.ld.LdOlap4jHierarchy;
import org.olap4j.driver.ld.LdOlap4jLevel;
import org.olap4j.driver.ld.LdOlap4jMeasure;
import org.olap4j.driver.ld.LdOlap4jMember;
import org.olap4j.driver.ld.LdOlap4jSchema;
import org.olap4j.impl.*;
import org.olap4j.mdx.*;
import org.olap4j.metadata.*;

import java.util.*;
import java.lang.ref.SoftReference;

/**
 * Implementation of {@link Cube} for XML/A providers.
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jCube.java 448 2011-04-13 00:32:40Z jhyde $
 * @since Dec 4, 2007
 */
class LdOlap4jCube implements Cube, Named {
	final LdOlap4jSchema olap4jSchema;
	private final String name;
	private final String caption;
	private final String description;

	final NamedList<LdOlap4jDimension> dimensions;
	final Map<String, LdOlap4jDimension> dimensionsByUname = new HashMap<String, LdOlap4jDimension>();
	private NamedList<LdOlap4jHierarchy> hierarchies = null;
	final Map<String, LdOlap4jHierarchy> hierarchiesByUname = new HashMap<String, LdOlap4jHierarchy>();
	final Map<String, LdOlap4jLevel> levelsByUname = new HashMap<String, LdOlap4jLevel>();
	final NamedList<LdOlap4jMeasure> measures;
	private final NamedList<XmlaOlap4jNamedSet> namedSets;
	private final MetadataReader metadataReader;

	/**
	 * Creates an XmlaOlap4jCube.
	 * 
	 * @param olap4jSchema
	 *            Schema
	 * @param name
	 *            Name
	 * @param caption
	 *            Caption
	 * @param description
	 *            Description
	 * @throws org.olap4j.OlapException
	 *             on error
	 */
	LdOlap4jCube(LdOlap4jSchema olap4jSchema, String name, String caption,
			String description) throws OlapException {
		assert olap4jSchema != null;
		assert description != null;
		assert name != null;
		this.olap4jSchema = olap4jSchema;
		this.name = name;
		this.caption = caption;
		this.description = description;

		final LdOlap4jConnection.Context context = new LdOlap4jConnection.Context(
				this, null, null, null);

		String[] restrictions = { "CATALOG_NAME",
				olap4jSchema.olap4jCatalog.getName(), "SCHEMA_NAME",
				olap4jSchema.getName(), "CUBE_NAME", getName() };

		this.dimensions = new DeferredNamedListImpl<LdOlap4jDimension>(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_DIMENSIONS,
				context, new LdOlap4jConnection.DimensionHandler(this),
				restrictions);
		// TODO: Why do I need to populate the entire cube, anyway?
		// int pop = dimensions.size();

		// TODO: We simply do not return named sets
		// populate named sets
		namedSets = new DeferredNamedListImpl<XmlaOlap4jNamedSet>(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_SETS, context,
				new LdOlap4jConnection.NamedSetHandler(), restrictions);

		// TODO: I do not want to populate measures up front, since then I need
		// to populate everything.
		// populate measures up front; a measure is needed in every query
		// try {
		// olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection
		// .populateList(
		// measures,
		// context,
		// LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEASURES,
		// new LdOlap4jConnection.MeasureHandler(),
		// restrictions);
		// } catch (OlapException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		/*
		 * A strange concept is that any cube has a list of measures, and also a
		 * dimension-hierarchy-level "Measures", that contains the same
		 * measures.
		 * 
		 * TODO: Currently, special type dimensions are simply also queried from
		 * RDF. What I could do is manually add the special type dimension
		 * "Measures" to any cube. For now, we keep it since it does not make
		 * problems.
		 */
		this.measures = new DeferredNamedListImpl<LdOlap4jMeasure>(
				LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEASURES, context,
				new LdOlap4jConnection.MeasureHandler(), restrictions);

		// MetadataReader which is not part of the olap4j api
		this.metadataReader = new CachingMetadataReader(
				new RawMetadataReader(), measures);
	}

	public Schema getSchema() {
		return olap4jSchema;
	}

	public String getName() {
		return name;
	}

	/**
	 * The unique name of a cube is simply the name;
	 * 
	 * Note, squared brackets are added by the system and not added to a cube
	 * itself.
	 */
	public String getUniqueName() {
		// return "[" + name + "]";
		return name;
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

	public NamedList<Dimension> getDimensions() {
		LdOlap4jUtil._log.info("getDimensions()...");

		return Olap4jUtil.cast(dimensions);
	}

	public NamedList<Hierarchy> getHierarchies() {
		// This is a costly operation. It forces the init
		// of all dimensions and all hierarchies.
		// We defer it to this point.
		// Workaround: I use the normal methods and not the attributes.
		if (this.hierarchies == null) {
			this.hierarchies = new NamedListImpl<LdOlap4jHierarchy>();
			NamedList<Dimension> theDimensions = this.getDimensions();
			for (Dimension dim : theDimensions) {
				LdOlap4jDimension ldDim = (LdOlap4jDimension) dim;
				NamedList<Hierarchy> theHierarchies = ldDim.getHierarchies();
				for (Hierarchy hierarchy : theHierarchies) {
					this.hierarchies.add((LdOlap4jHierarchy) hierarchy);
				}
			}
		}
		return Olap4jUtil.cast(hierarchies);
	}

	public boolean isDrillThroughEnabled() {
		// XMLA does not implement drillthrough yet.
		return false;
	}

	public List<Measure> getMeasures() {

		return Olap4jUtil.cast(measures);
	}

	public NamedList<NamedSet> getSets() {
		return Olap4jUtil.cast(namedSets);
	}

	public Collection<Locale> getSupportedLocales() {
		return Collections.singletonList(Locale.getDefault());
	}

	public Member lookupMember(List<IdentifierSegment> segmentList)
			throws OlapException {
		StringBuilder buf = new StringBuilder();

		// Segments are just concatenated to make up the unique name.
		for (IdentifierSegment segment : segmentList) {
			if (buf.length() > 0) {
				buf.append('.');
			}
			// We are looking for names, not strings with square brackets
			buf.append(segment.getName());
			// buf.append(segment.toString());
		}
		final String uniqueName = buf.toString();
		// MetadataReader is used to find the actual member
		return getMetadataReader().lookupMemberByUniqueName(uniqueName);
	}

	/**
	 * Returns this cube's metadata reader.
	 * 
	 * <p>
	 * Not part of public olap4j API.
	 * 
	 * @return metadata reader
	 */
	MetadataReader getMetadataReader() {
		return metadataReader;
	}

	public List<Member> lookupMembers(Set<Member.TreeOp> treeOps,
			List<IdentifierSegment> nameParts) throws OlapException {
		StringBuilder buf = new StringBuilder();

		// Segments are just concatenated to make up the unique name.
		for (IdentifierSegment namePart : nameParts) {
			if (buf.length() > 0) {
				buf.append('.');
			}
			buf.append(namePart);
		}
		final String uniqueName = buf.toString();
		final List<LdOlap4jMember> list = new ArrayList<LdOlap4jMember>();
		// Metadata reader is used for finding all members
		getMetadataReader().lookupMemberRelatives(treeOps, uniqueName, list);
		return Olap4jUtil.cast(list);
	}

	/**
	 * Abstract implementation of MemberReader that delegates all operations to
	 * an underlying MemberReader.
	 */
	private static abstract class DelegatingMetadataReader implements
			MetadataReader {
		private final MetadataReader metadataReader;

		/**
		 * Creates a DelegatingMetadataReader.
		 * 
		 * @param metadataReader
		 *            Underlying metadata reader
		 */
		DelegatingMetadataReader(MetadataReader metadataReader) {
			this.metadataReader = metadataReader;
		}

		public LdOlap4jMember lookupMemberByUniqueName(String memberUniqueName)
				throws OlapException {
			return metadataReader.lookupMemberByUniqueName(memberUniqueName);
		}

		public void lookupMembersByUniqueName(List<String> memberUniqueNames,
				Map<String, LdOlap4jMember> memberMap) throws OlapException {
			metadataReader.lookupMembersByUniqueName(memberUniqueNames,
					memberMap);
		}

		public void lookupMemberRelatives(Set<Member.TreeOp> treeOps,
				String memberUniqueName, List<LdOlap4jMember> list)
				throws OlapException {
			metadataReader.lookupMemberRelatives(treeOps, memberUniqueName,
					list);
		}

		public List<LdOlap4jMember> getLevelMembers(LdOlap4jLevel level)
				throws OlapException {
			return metadataReader.getLevelMembers(level);
		}
	}

	/**
	 * Implementation of MemberReader that reads from an underlying member
	 * reader and caches the results.
	 * 
	 * <p>
	 * Caches are {@link Map}s containing {@link java.lang.ref.SoftReference}s
	 * to cached objects, so can be cleared when memory is in short supply.
	 */
	private static class CachingMetadataReader extends DelegatingMetadataReader {

		private final Map<String, SoftReference<LdOlap4jMember>> memberMap = new HashMap<String, SoftReference<LdOlap4jMember>>();

		private final Map<LdOlap4jLevel, SoftReference<List<LdOlap4jMember>>> levelMemberListMap = new HashMap<LdOlap4jLevel, SoftReference<List<LdOlap4jMember>>>();

		private NamedList<LdOlap4jMeasure> measures;

		/**
		 * Creates a CachingMetadataReader.
		 * 
		 * @param metadataReader
		 *            Underlying metadata reader
		 * @param measures
		 *            Map of measures by unique name, inherited from the cube
		 *            and used read-only by this reader
		 */
		CachingMetadataReader(MetadataReader metadataReader,
				NamedList<LdOlap4jMeasure> measures) {
			super(metadataReader);
			this.measures = measures;
		}

		public LdOlap4jMember lookupMemberByUniqueName(String memberUniqueName)
				throws OlapException {
			// We do not have a measure map any more, so we look in measures,
			// // First, look in measures map.

			// TODO: I will always recognize a not populated measure list, by it
			// being empty. Possibly, it does not make sense to query for
			// measures here, also, since
			// we have a list of measures for a cube, and we have a list of
			// measures for the measure dimension. Thus, we look here in the
			// cube measure list.
			if (!measures.isEmpty()) {
				LdOlap4jMeasure measure = measures.get(memberUniqueName);
				if (measure != null) {
					return measure;
				}
			}

			// Next, look in cache.
			final SoftReference<LdOlap4jMember> memberRef = memberMap
					.get(memberUniqueName);
			if (memberRef != null) {
				final LdOlap4jMember member = memberRef.get();
				if (member != null) {
					return member;
				}
			}

			// Lastly, ask super class
			final LdOlap4jMember member = super
					.lookupMemberByUniqueName(memberUniqueName);
			if (member != null
					&& member.getDimension().type != Dimension.Type.MEASURE) {
				memberMap.put(memberUniqueName,
						new SoftReference<LdOlap4jMember>(member));
			}
			// Can null
			return member;
		}

		public void lookupMembersByUniqueName(List<String> memberUniqueNames,
				Map<String, LdOlap4jMember> memberMap) throws OlapException {
			final ArrayList<String> remainingMemberUniqueNames = new ArrayList<String>();
			for (String memberUniqueName : memberUniqueNames) {
				// First, look in measures map.
				// TODO: I will always recognize a not populated measure list,
				// by it
				// being empty.
				if (!measures.isEmpty()) {
					LdOlap4jMeasure measure = measures.get(memberUniqueName);
					if (measure != null) {
						memberMap.put(memberUniqueName, measure);
						continue;
					}
				}

				// Next, look in cache.
				final SoftReference<LdOlap4jMember> memberRef = this.memberMap
						.get(memberUniqueName);
				final LdOlap4jMember member;
				if (memberRef != null && (member = memberRef.get()) != null) {
					memberMap.put(memberUniqueName, member);
					continue;
				}

				remainingMemberUniqueNames.add(memberUniqueName);
			}

			// If any of the member names were not in the cache, look them up
			// by delegating.
			if (!remainingMemberUniqueNames.isEmpty()) {
				super.lookupMembersByUniqueName(remainingMemberUniqueNames,
						memberMap);
				// Add the previously missing members into the cache.
				for (String memberName : remainingMemberUniqueNames) {
					LdOlap4jMember member = memberMap.get(memberName);
					if (member != null) {
						if (!(member instanceof Measure)
								&& member.getDimension().type != Dimension.Type.MEASURE) {
							this.memberMap.put(memberName,
									new SoftReference<LdOlap4jMember>(member));
						}
					}
				}
			}
		}

		public List<LdOlap4jMember> getLevelMembers(LdOlap4jLevel level)
				throws OlapException {
			final SoftReference<List<LdOlap4jMember>> memberListRef = levelMemberListMap
					.get(level);
			if (memberListRef != null) {
				final List<LdOlap4jMember> memberList = memberListRef.get();
				if (memberList != null) {
					return memberList;
				}
			}
			final List<LdOlap4jMember> memberList = super
					.getLevelMembers(level);
			if (level.olap4jHierarchy.olap4jDimension.type != Dimension.Type.MEASURE) {
				levelMemberListMap.put(level,
						new SoftReference<List<LdOlap4jMember>>(memberList));
			}
			return memberList;
		}
	}

	/**
	 * Implementation of MetadataReader that reads from the XMLA provider,
	 * without caching.
	 */
	private class RawMetadataReader implements MetadataReader {
		public LdOlap4jMember lookupMemberByUniqueName(String memberUniqueName)
				throws OlapException {
			NamedList<LdOlap4jMember> list = new NamedListImpl<LdOlap4jMember>();

			// So, we are querying for the member self with a specific unique
			// name, that shall be put in an empty list
			lookupMemberRelatives(Olap4jUtil.enumSetOf(Member.TreeOp.SELF),
					memberUniqueName, list);
			switch (list.size()) {
			case 0:
				return null;
			case 1:
				return list.get(0);
			default:
				throw new IllegalArgumentException(
						"more than one member with unique name '"
								+ memberUniqueName + "'");
			}
		}

		public void lookupMembersByUniqueName(List<String> memberUniqueNames,
				Map<String, LdOlap4jMember> memberMap) throws OlapException {
			if (olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection
					.getDatabase().indexOf("Provider=Mondrian") != -1) {
				mondrianMembersLookup(memberUniqueNames, memberMap);
			} else {
				genericMembersLookup(memberUniqueNames, memberMap);
			}
		}

		/**
		 * Looks up members; optimized for Mondrian servers.
		 * 
		 * @param memberUniqueNames
		 *            A list of the members to lookup
		 * @param memberMap
		 *            Output map of members keyed by unique name
		 * @throws OlapException
		 *             Gets thrown for communication errors
		 */
		private void mondrianMembersLookup(List<String> memberUniqueNames,
				Map<String, LdOlap4jMember> memberMap) throws OlapException {
			final LdOlap4jConnection.Context context = new LdOlap4jConnection.Context(
					LdOlap4jCube.this, null, null, null);
			final List<LdOlap4jMember> memberList = new ArrayList<LdOlap4jMember>();
			olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection
					.populateList(
							memberList,
							context,
							LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEMBERS,
							new LdOlap4jConnection.MemberHandler(),
							new Object[] { "CATALOG_NAME",
									olap4jSchema.olap4jCatalog.getName(),
									"SCHEMA_NAME", olap4jSchema.getName(),
									"CUBE_NAME", getName(),
									"MEMBER_UNIQUE_NAME", memberUniqueNames });
			for (LdOlap4jMember member : memberList) {
				if (member != null) {
					memberMap.put(member.getUniqueName(), member);
				}
			}
		}

		/**
		 * Looks up members.
		 * 
		 * @param memberUniqueNames
		 *            A list of the members to lookup
		 * @param memberMap
		 *            Output map of members keyed by unique name
		 * @throws OlapException
		 *             Gets thrown for communication errors
		 */
		private void genericMembersLookup(List<String> memberUniqueNames,
				Map<String, LdOlap4jMember> memberMap) throws OlapException {
			// Iterates through member names
			for (String currentMemberName : memberUniqueNames) {
				// Only lookup if it is not in the map yet
				if (!memberMap.containsKey(currentMemberName)) {
					LdOlap4jMember member = this
							.lookupMemberByUniqueName(currentMemberName);
					// Null members might mean calculated members
					if (member != null) {
						memberMap.put(member.getUniqueName(), member);
					}
				}
			}
		}

		public void lookupMemberRelatives(Set<Member.TreeOp> treeOps,
				String memberUniqueName, List<LdOlap4jMember> list)
				throws OlapException {
			final LdOlap4jConnection.Context context = new LdOlap4jConnection.Context(
					LdOlap4jCube.this, null, null, null);
			int treeOpMask = 0;
			for (Member.TreeOp treeOp : treeOps) {
				treeOpMask |= treeOp.xmlaOrdinal();
			}
			olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection
					.populateList(
							list,
							context,
							LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEMBERS,
							new LdOlap4jConnection.MemberHandler(),
							new Object[] { "CATALOG_NAME",
									olap4jSchema.olap4jCatalog.getName(),
									"SCHEMA_NAME", olap4jSchema.getName(),
									"CUBE_NAME", getName(),
									"MEMBER_UNIQUE_NAME", memberUniqueName,
									"TREE_OP", String.valueOf(treeOpMask) });
		}

		public List<LdOlap4jMember> getLevelMembers(LdOlap4jLevel level)
				throws OlapException {
			assert level.olap4jHierarchy.olap4jDimension.olap4jCube == LdOlap4jCube.this;
			final LdOlap4jConnection.Context context = new LdOlap4jConnection.Context(
					level);
			List<LdOlap4jMember> list = new ArrayList<LdOlap4jMember>();
			// If this is a level in the [Measures] dimension, we want to
			// return objects that implement the Measure interface. During
			// bootstrap, the list will be empty, and we need to return the
			// regular Member objects which have the extra properties that are
			// returned by MSCHEMA_MEMBERS but not MDSCHEMA_MEASURES.

			// TODO: I will always recognize a not populated measure list, by it
			// being empty.
			switch (level.getDimension().getDimensionType()) {
			case MEASURE:
				if (!level.olap4jHierarchy.olap4jDimension.olap4jCube.measures
						.isEmpty()) {
					return Olap4jUtil
							.cast(level.olap4jHierarchy.olap4jDimension.olap4jCube.measures);
				}
				break;
			default:
				olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection
						.populateList(
								list,
								context,
								LdOlap4jConnection.MetadataRequest.MDSCHEMA_MEMBERS,
								new LdOlap4jConnection.MemberHandler(),
								new Object[] {
										"CATALOG_NAME",
										olap4jSchema.olap4jCatalog.getName(),
										"SCHEMA_NAME",
										olap4jSchema.getName(),
										"CUBE_NAME",
										getName(),
										"DIMENSION_UNIQUE_NAME",
										level.olap4jHierarchy.olap4jDimension
												.getUniqueName(),
										"HIERARCHY_UNIQUE_NAME",
										level.olap4jHierarchy.getUniqueName(),
										"LEVEL_UNIQUE_NAME",
										level.getUniqueName() });
				return list;
			}
			throw new UnsupportedOperationException(
					"The right type of dimension should have been found.");
		}
	}
}

// End XmlaOlap4jCube.java
