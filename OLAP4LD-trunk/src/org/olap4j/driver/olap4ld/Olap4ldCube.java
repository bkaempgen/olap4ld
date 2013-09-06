/*
//
// Licensed to Benedikt Kämpgen under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Benedikt Kämpgen licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package org.olap4j.driver.olap4ld;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldCube;
import org.olap4j.driver.olap4ld.Olap4ldDimension;
import org.olap4j.driver.olap4ld.Olap4ldHierarchy;
import org.olap4j.driver.olap4ld.Olap4ldLevel;
import org.olap4j.driver.olap4ld.Olap4ldMeasure;
import org.olap4j.driver.olap4ld.Olap4ldMember;
import org.olap4j.driver.olap4ld.Olap4ldSchema;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.impl.*;
import org.olap4j.mdx.*;
import org.olap4j.metadata.*;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

import java.util.*;
import java.lang.ref.SoftReference;

/**
 * Implementation of {@link Cube} for XML/A providers.
 * 
 * @author jhyde, bkaempgen
 * @version $Id: XmlaOlap4jCube.java 448 2011-04-13 00:32:40Z jhyde $
 * @since Dec 4, 2007
 */
class Olap4ldCube implements Cube, Named {
	final Olap4ldSchema olap4jSchema;
	private final String name;
	private final String caption;
	private final String description;

	final NamedList<Olap4ldDimension> dimensions;
	final Map<String, Olap4ldDimension> dimensionsByUname = new HashMap<String, Olap4ldDimension>();
	private NamedList<Olap4ldHierarchy> hierarchies = null;
	final Map<String, Olap4ldHierarchy> hierarchiesByUname = new HashMap<String, Olap4ldHierarchy>();
	final Map<String, Olap4ldLevel> levelsByUname = new HashMap<String, Olap4ldLevel>();
	final NamedList<Olap4ldMeasure> measures;
	private final NamedList<Olap4ldNamedSet> namedSets;
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
	Olap4ldCube(Olap4ldSchema olap4jSchema, String name, String caption,
			String description) throws OlapException {
		assert olap4jSchema != null;
		assert description != null;
		assert name != null;
		this.olap4jSchema = olap4jSchema;
		this.name = name;
		this.caption = caption;
		this.description = description;

		final Olap4ldConnection.Context context = new Olap4ldConnection.Context(
				this, null, null, null);

		String[] restrictions = { "CATALOG_NAME",
				olap4jSchema.olap4jCatalog.getName(), "SCHEMA_NAME",
				olap4jSchema.getName(), "CUBE_NAME", getName() };

		this.dimensions = new DeferredNamedListImpl<Olap4ldDimension>(
				Olap4ldConnection.MetadataRequest.MDSCHEMA_DIMENSIONS,
				context, new Olap4ldConnection.DimensionHandler(this),
				restrictions);
		// TODO: Why do I need to populate the entire cube, anyway?
		// int pop = dimensions.size();

		// TODO: We simply do not return named sets
		// populate named sets
		namedSets = new DeferredNamedListImpl<Olap4ldNamedSet>(
				Olap4ldConnection.MetadataRequest.MDSCHEMA_SETS, context,
				new Olap4ldConnection.NamedSetHandler(), restrictions);

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
		this.measures = new DeferredNamedListImpl<Olap4ldMeasure>(
				Olap4ldConnection.MetadataRequest.MDSCHEMA_MEASURES, context,
				new Olap4ldConnection.MeasureHandler(), restrictions);

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
		Olap4ldUtil._log.info("getDimensions()...");

		return Olap4jUtil.cast(dimensions);
	}

	public NamedList<Hierarchy> getHierarchies() {
		// This is a costly operation. It forces the init
		// of all dimensions and all hierarchies.
		// We defer it to this point.
		// Workaround: I use the normal methods and not the attributes.
		if (this.hierarchies == null) {
			this.hierarchies = new NamedListImpl<Olap4ldHierarchy>();
			NamedList<Dimension> theDimensions = this.getDimensions();
			for (Dimension dim : theDimensions) {
				Olap4ldDimension ldDim = (Olap4ldDimension) dim;
				NamedList<Hierarchy> theHierarchies = ldDim.getHierarchies();
				for (Hierarchy hierarchy : theHierarchies) {
					this.hierarchies.add((Olap4ldHierarchy) hierarchy);
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
		
		Olap4ldUtil._log.info("getMeasures()...");

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
			// We are looking for names, i.e. strings with square brackets
			// buf.append(segment.getName());
			buf.append(segment.toString());
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
	public MetadataReader getMetadataReader() {
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
		final List<Olap4ldMember> list = new ArrayList<Olap4ldMember>();
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

		public Olap4ldMember lookupMemberByUniqueName(String memberUniqueName)
				throws OlapException {
			return metadataReader.lookupMemberByUniqueName(memberUniqueName);
		}

		public void lookupMembersByUniqueName(List<String> memberUniqueNames,
				Map<String, Olap4ldMember> memberMap) throws OlapException {
			metadataReader.lookupMembersByUniqueName(memberUniqueNames,
					memberMap);
		}

		public void lookupMemberRelatives(Set<Member.TreeOp> treeOps,
				String memberUniqueName, List<Olap4ldMember> list)
				throws OlapException {
			metadataReader.lookupMemberRelatives(treeOps, memberUniqueName,
					list);
		}

		public List<Olap4ldMember> getLevelMembers(Olap4ldLevel level)
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

		private final Map<String, SoftReference<Olap4ldMember>> memberMap = new HashMap<String, SoftReference<Olap4ldMember>>();

		private final Map<Olap4ldLevel, SoftReference<List<Olap4ldMember>>> levelMemberListMap = new HashMap<Olap4ldLevel, SoftReference<List<Olap4ldMember>>>();

		//private NamedList<Olap4ldMeasure> measures;

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
				NamedList<Olap4ldMeasure> measures) {
			super(metadataReader);
			//this.measures = measures;
		}

		public Olap4ldMember lookupMemberByUniqueName(String memberUniqueName)
				throws OlapException {
			// We do not have a measure map any more, so we look in measures,
			// // First, look in measures map.

			// TODO: I will always recognize a not populated measure list, by it
			// being empty. Possibly, it does not make sense to query for
			// measures here, also, since
			// we have a list of measures for a cube, and we have a list of
			// measures for the measure dimension. Thus, we look here in the
			// cube measure list.
//			if (!measures.isEmpty()) {
//				Olap4ldMeasure measure = measures.get(memberUniqueName);
//				if (measure != null) {
//					return measure;
//				}
//			}

			// Next, look in cache.
			final SoftReference<Olap4ldMember> memberRef = memberMap
					.get(memberUniqueName);
			if (memberRef != null) {
				final Olap4ldMember member = memberRef.get();
				if (member != null) {
					return member;
				}
			}

			// Lastly, ask super class
			final Olap4ldMember member = super
					.lookupMemberByUniqueName(memberUniqueName);
			if (member != null
					&& member.getDimension().type != Dimension.Type.MEASURE) {
				memberMap.put(memberUniqueName,
						new SoftReference<Olap4ldMember>(member));
			}
			// Can null
			return member;
		}

		public void lookupMembersByUniqueName(List<String> memberUniqueNames,
				Map<String, Olap4ldMember> memberMap) throws OlapException {
			final ArrayList<String> remainingMemberUniqueNames = new ArrayList<String>();
			for (String memberUniqueName : memberUniqueNames) {
				// First, look in measures map.
				// TODO: I will always recognize a not populated measure list,
				// by it
				// being empty.
//				if (!measures.isEmpty()) {
//					Olap4ldMeasure measure = measures.get(memberUniqueName);
//					if (measure != null) {
//						memberMap.put(memberUniqueName, measure);
//						continue;
//					}
//				}

				// Next, look in cache.
				final SoftReference<Olap4ldMember> memberRef = this.memberMap
						.get(memberUniqueName);
				final Olap4ldMember member;
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
					Olap4ldMember member = memberMap.get(memberName);
					if (member != null) {
						if (!(member instanceof Measure)
								&& member.getDimension().type != Dimension.Type.MEASURE) {
							this.memberMap.put(memberName,
									new SoftReference<Olap4ldMember>(member));
						}
					}
				}
			}
		}

		public List<Olap4ldMember> getLevelMembers(Olap4ldLevel level)
				throws OlapException {
			final SoftReference<List<Olap4ldMember>> memberListRef = levelMemberListMap
					.get(level);
			if (memberListRef != null) {
				final List<Olap4ldMember> memberList = memberListRef.get();
				if (memberList != null) {
					return memberList;
				}
			}
			final List<Olap4ldMember> memberList = super
					.getLevelMembers(level);
			if (level.olap4jHierarchy.olap4jDimension.type != Dimension.Type.MEASURE) {
				levelMemberListMap.put(level,
						new SoftReference<List<Olap4ldMember>>(memberList));
			}
			return memberList;
		}
	}

	/**
	 * Implementation of MetadataReader that reads from the XMLA provider,
	 * without caching.
	 */
	private class RawMetadataReader implements MetadataReader {
		public Olap4ldMember lookupMemberByUniqueName(String memberUniqueName)
				throws OlapException {
			NamedList<Olap4ldMember> list = new NamedListImpl<Olap4ldMember>();

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
				Map<String, Olap4ldMember> memberMap) throws OlapException {
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
				Map<String, Olap4ldMember> memberMap) throws OlapException {
			final Olap4ldConnection.Context context = new Olap4ldConnection.Context(
					Olap4ldCube.this, null, null, null);
			final List<Olap4ldMember> memberList = new ArrayList<Olap4ldMember>();
			olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
					.populateList(
							memberList,
							context,
							Olap4ldConnection.MetadataRequest.MDSCHEMA_MEMBERS,
							new Olap4ldConnection.MemberHandler(),
							new Object[] { "CATALOG_NAME",
									olap4jSchema.olap4jCatalog.getName(),
									"SCHEMA_NAME", olap4jSchema.getName(),
									"CUBE_NAME", getName(),
									"MEMBER_UNIQUE_NAME", memberUniqueNames });
			for (Olap4ldMember member : memberList) {
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
				Map<String, Olap4ldMember> memberMap) throws OlapException {
			// Iterates through member names
			for (String currentMemberName : memberUniqueNames) {
				// Only lookup if it is not in the map yet
				if (!memberMap.containsKey(currentMemberName)) {
					Olap4ldMember member = this
							.lookupMemberByUniqueName(currentMemberName);
					// Null members might mean calculated members
					if (member != null) {
						memberMap.put(member.getUniqueName(), member);
					}
				}
			}
		}

		public void lookupMemberRelatives(Set<Member.TreeOp> treeOps,
				String memberUniqueName, List<Olap4ldMember> list)
				throws OlapException {
			final Olap4ldConnection.Context context = new Olap4ldConnection.Context(
					Olap4ldCube.this, null, null, null);
			int treeOpMask = 0;
			for (Member.TreeOp treeOp : treeOps) {
				treeOpMask |= treeOp.xmlaOrdinal();
			}
			olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
					.populateList(
							list,
							context,
							Olap4ldConnection.MetadataRequest.MDSCHEMA_MEMBERS,
							new Olap4ldConnection.MemberHandler(),
							new Object[] { "CATALOG_NAME",
									olap4jSchema.olap4jCatalog.getName(),
									"SCHEMA_NAME", olap4jSchema.getName(),
									"CUBE_NAME", getName(),
									"MEMBER_UNIQUE_NAME", memberUniqueName,
									"TREE_OP", String.valueOf(treeOpMask) });
		}

		public List<Olap4ldMember> getLevelMembers(Olap4ldLevel level)
				throws OlapException {
			assert level.olap4jHierarchy.olap4jDimension.olap4jCube == Olap4ldCube.this;
			final Olap4ldConnection.Context context = new Olap4ldConnection.Context(
					level);
			List<Olap4ldMember> list = new ArrayList<Olap4ldMember>();
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
				olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
						.populateList(
								list,
								context,
								Olap4ldConnection.MetadataRequest.MDSCHEMA_MEMBERS,
								new Olap4ldConnection.MemberHandler(),
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
	
	public List<Node[]> transformMetadataObject2NxNodes() {
		List<Node[]> nodes = new ArrayList<Node[]>();

			// Create header

			/*
			 * ?CATALOG_NAME ?SCHEMA_NAME ?CUBE_NAME ?CUBE_TYPE ?CUBE_CAPTION
			 * ?DESCRIPTION
			 */

			Node[] header = new Node[] { new Variable("?CATALOG_NAME"),
					new Variable("?SCHEMA_NAME"), new Variable("?CUBE_NAME"),
					new Variable("?CUBE_TYPE"), new Variable("?CUBE_CAPTION"),
					new Variable("?DESCRIPTION") };
			nodes.add(header);
			
			Node[] cubenode = new Node[] {
					new Literal(Olap4ldLinkedDataUtil
							.convertMDXtoURI(this.getSchema().getCatalog().getName())),
					new Literal(Olap4ldLinkedDataUtil
							.convertMDXtoURI(this.getSchema().getName())),
					new Literal(Olap4ldLinkedDataUtil
							.convertMDXtoURI(this.getUniqueName())), new Literal("CUBE"),
					new Literal(this.getCaption()),
					new Literal(this.getDescription()) };
			nodes.add(cubenode);

		return nodes;
	}
}

// End XmlaOlap4jCube.java
