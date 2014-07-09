//package org.olap4j.driver.olap4ld.linkeddata;
//
//
///**
// * This visitor creates a physical query plan. It is different from
// * OlapDrillAcross2SparqlSesameVisitor and Olap2SparqlSesameVisitor that only
// * use two Iterators, Olap2SparqlAlgorithmSesameIterator and
// * OlapDrillAcross2SparqlSesameIterator. They implement the Drill-Across
// * algorithm and the OLAP-to-SPARQL algorithm.
// * 
// * Olap2SparqlSesameDerivedDatasetVisitor uses for every logical olap operator a
// * separate physical olap iterator. Here, the repo also needs to be pre-filled with
// * data cubes, yet, every iterator creates a new derived data cube.
// * 
// * @author benedikt
// */
//public class Olap2SparqlSesameDerivedDatasetVisitor implements
//		LogicalOlapOperatorQueryPlanVisitor {
//	// the new root node
//	PhysicalOlapIterator _root;
//
//	// For the moment, we know the engine
//	private EmbeddedSesameEngine engine;
//
//	/**
//	 * 
//	 * * Design decisions The LogicalOperators only get as input a cube and the
//	 * parameters, but not specifically the metadata of the cube.
//	 * 
//	 * The PhysicalIterators get as input another iterator and the parameters,
//	 * but also the metadata of the iterator. I create PhysicalIterators for
//	 * metadata, they get as input the repo and the parameters (restrictions).
//	 * Every LogicalOperator could be wrapped, get as input the identifier of a
//	 * cube and the parameters and return a new identifier.
//	 * 
//	 * Mioeur2eur is another LogicalOperator that could get wrapped, get as
//	 * input the identifier of a cube and the parameters and return a new
//	 * identifier. Mioeur2eur would also include PhysicalIterators. The
//	 * Mioeur2eur iterator would get as input another iterator,
//	 * BaseCubeIterator, and return the conversion. Example:
//	 * http://olap4ld.googlecode.com/dic/aggreg95#00;
//	 * http://olap4ld.googlecode.com/dic/geo#US;
//	 * http://olap4ld.googlecode.com/dic/indic_na#VI_PPS_EU28_HAB; 2012; 149;
//	 * 
//	 */
//	public Olap2SparqlSesameDerivedDatasetVisitor(EmbeddedSesameEngine engine) {
//		this.engine = engine;
//	}
//
//	/**
//	 * After iteration, create new root. I want to have simple SPARQL query +
//	 * load RDF to store.
//	 */
//	public Object getNewRoot() {
//
//		return _root;
//
//		// Try to normally evaluate all operators apart from drill-across
//
//		// // We collect necessary parts of the SPARQL query.
//		// String selectClause = " ";
//		// String whereClause = " ";
//		// String groupByClause = " group by ";
//		// String orderByClause = " order by ";
//		// if (_root instanceof DrillAcrossSparqlIterator) {
//		// return _root;
//		// } else {
//		// List<List<Node[]>> metadata = (ArrayList<List<Node[]>>) _root
//		// .next();
//		// List<Node[]> cubes = metadata.get(0);
//		// List<Node[]> measures = metadata.get(1);
//		// List<Node[]> dimensions = metadata.get(2);
//		// List<Node[]> hierarchies = metadata.get(3);
//		// List<Node[]> levels = metadata.get(4);
//		// List<Node[]> members = metadata.get(5);
//		//
//		// // BaseCube
//		//
//		// Map<String, Integer> map = Olap4ldLinkedDataUtil
//		// .getNodeResultFields(cubes.get(0));
//		//
//		// // Cube gives us the URI of the dataset that we want to resolve
//		// String ds = cubes.get(1)[map.get("CUBE_NAME")].toString();
//		//
//		// whereClause += "?obs qb:dataSet" + ds + ". ";
//		//
//		// // Projections
//		// List<Node[]> projections = measures;
//		//
//		// // Now, for each measure, we create a measure value column.
//		// // Any measure should be contained only once, unless calculated
//		// // measures
//		// HashMap<Integer, Boolean> measureMap = new HashMap<Integer,
//		// Boolean>();
//		// map = Olap4ldLinkedDataUtil.getNodeResultFields(projections.get(0));
//		// boolean first = true;
//		//
//		// for (Node[] measure : projections) {
//		//
//		// if (first) {
//		// first = false;
//		// continue;
//		// }
//		//
//		// // For formulas, this is different
//		// // XXX: Needs to be put before creating the Logical Olap
//		// // Operator
//		// // Tree
//		// // Will probably not work since MEASURE_AGGREGATOR
//		// if (measure[map.get("?MEASURE_AGGREGATOR")].toString().equals(
//		// "http://purl.org/olap#calculated")) {
//		//
//		// /*
//		// * For now, hard coded. Here, I also partly evaluate a query
//		// * string, thus, I could do the same with filters.
//		// */
//		// // Measure measure1 = (Measure) ((MemberNode) ((CallNode)
//		// // measure
//		// // .getExpression()).getArgList().get(0)).getMember();
//		// // Measure measure2 = (Measure) ((MemberNode) ((CallNode)
//		// // measure
//		// // .getExpression()).getArgList().get(1)).getMember();
//		// //
//		// // String operatorName = ((CallNode)
//		// // measure.getExpression())
//		// // .getOperatorName();
//		//
//		// // Measure
//		// // Measure property has aggregator attached to it at the
//		// // end,
//		// // e.g., "AVG".
//		// // String measureProperty1 = Olap4ldLinkedDataUtil
//		// // .convertMDXtoURI(measure1.getUniqueName()).replace(
//		// // "AGGFUNC" + measure.getAggregator().name(), "");
//		// // We do not encode the aggregation function in the measure,
//		// // any
//		// // more.
//		// // String measurePropertyVariable1 =
//		// // makeUriToParameter(measure1
//		// // .getUniqueName());
//		// //
//		// // String measureProperty2 = Olap4ldLinkedDataUtil
//		// // .convertMDXtoURI(measure2.getUniqueName()).replace(
//		// // "AGGFUNC" + measure.getAggregator().name(), "");
//		// // String measurePropertyVariable2 =
//		// // makeUriToParameter(measure2
//		// // .getUniqueName());
//		//
//		// // We take the aggregator from the measure
//		// // selectClause += " " + measure1.getAggregator().name() +
//		// // "(?"
//		// // + measurePropertyVariable1 + " " + operatorName + " "
//		// // + "?" + measurePropertyVariable2 + ")";
//		// //
//		// // // I have to select them only, if we haven't inserted any
//		// // // before.
//		// // if (!measureMap.containsKey(measureProperty1.hashCode()))
//		// // {
//		// // whereClause += " ?obs <" + measureProperty1 + "> ?"
//		// // + measurePropertyVariable1 + ". ";
//		// // measureMap.put(measureProperty1.hashCode(), true);
//		// // }
//		// // if (!measureMap.containsKey(measureProperty2.hashCode()))
//		// // {
//		// // whereClause += " ?obs <" + measureProperty2 + "> ?"
//		// // + measurePropertyVariable2 + ". ";
//		// // measureMap.put(measureProperty2.hashCode(), true);
//		// // }
//		//
//		// } else {
//		//
//		// // As always, remove Aggregation Function from Measure Name
//		// String measureProperty = measure[map
//		// .get("?MEASURE_UNIQUE_NAME")].toString().replace(
//		// "AGGFUNC"
//		// + measure[map.get("?MEASURE_AGGREGATOR")]
//		// .toString()
//		// .replace("http://purl.org/olap#",
//		// ""), "");
//		//
//		// // We also remove aggregation function from Measure Property
//		// // Variable so
//		// // that the same property is not selected twice.
//		// String measurePropertyVariable = makeUriToParameter(measure[map
//		// .get("?MEASURE_UNIQUE_NAME")].toString().replace(
//		// "AGGFUNC"
//		// + measure[map.get("?MEASURE_AGGREGATOR")]
//		// .toString()
//		// .replace("http://purl.org/olap#",
//		// ""), ""));
//		//
//		// // Unique name for variable
//		// String uniqueMeasurePropertyVariable = makeUriToParameter(measure[map
//		// .get("?MEASURE_UNIQUE_NAME")].toString());
//		//
//		// // We take the aggregator from the measure
//		// // Since we use OPTIONAL, there might be empty columns,
//		// // which is why we need
//		// // to convert them to decimal.
//		// selectClause += " ("
//		// + measure[map.get("?MEASURE_AGGREGATOR")]
//		// .toString().replace(
//		// "http://purl.org/olap#", "") + "(?"
//		// + measurePropertyVariable + ") as ?"
//		// + uniqueMeasurePropertyVariable + ")";
//		//
//		// // According to spec, every measure needs to be set for
//		// // every
//		// // observation only once
//		// if (!measureMap.containsKey(measureProperty.hashCode())) {
//		// whereClause += "?obs <" + measureProperty + "> ?"
//		// + measurePropertyVariable + ".";
//		// measureMap.put(measureProperty.hashCode(), true);
//		// }
//		// }
//		// }
//		//
//		// }
//		//
//		// // Dice
//		// // Not possible currently
//		//
//		// // Slice
//		//
//		// // Rollup
//		//
//		// // Initialise triple store with dataset URI
//		//
//		// // Query triple store with SPARQL query
//		// // Now that we consider DSD in queries, we need to consider DSD
//		// // locations
//		// String query = Olap4ldLinkedDataUtil.getStandardPrefixes() +
//		// "select "
//		// + selectClause + askForFrom(false) + askForFrom(true)
//		// + "where { " + whereClause + "}" + groupByClause
//		// + orderByClause;
//		//
//		// // Currently, we should have retrieved the data, already, therefore,
//		// we
//		// // only have one node.
//		// _root = new SparqlSesameIterator(repo, query);
//		//
//		// return _root;
//	}
//
//	@Override
//	public void visit(DrillAcrossOp op) throws QueryException {
//		
//		throw new UnsupportedOperationException(
//				"visit(DrillAcrossOp op) not implemented!");
//		
////		DrillAcrossOp so = (DrillAcrossOp) op;
////
////		// Most probable, the _root will be null
////		so.inputop1.accept(this);
////		PhysicalOlapIterator root1 = _root;
////
////		so.inputop2.accept(this);
////		PhysicalOlapIterator root2 = _root;
////
////		DrillAcrossSparqlDerivedDatasetIterator resultiterator = new DrillAcrossSparqlDerivedDatasetIterator(
////				root1, root2);
////
////		_root = resultiterator;
//	}
//
//	@Override
//	public void visit(RollupOp op) throws QueryException {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void visit(SliceOp op) throws QueryException {
//		
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException(
//				"visit(SliceOp op) not implemented!");
//		
////		SliceOp so = (SliceOp) op;
////
////		so.inputOp.accept(this);
////
////		SliceSparqlDerivedDatasetIterator slicecube = new SliceSparqlDerivedDatasetIterator(engine, _root,
////				so.slicedDimensions);
////
////		_root = slicecube;
//	}
//
//	@Override
//	public void visit(DiceOp op) throws QueryException {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void visit(ProjectionOp op) throws QueryException {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void visit(BaseCubeOp op) throws QueryException {
//		BaseCubeOp so = (BaseCubeOp) op;
//
//		// TODO One possibility is it to directly create the tree
//		// Add physical operator that retrieves data from URI and stores it
//		// in triple store
//		// ExecIterator bi = null;
//		// _root = bi;
//
//		// We do not need to change those metadata.
//
//		BaseCubeSparqlDerivedDatasetIterator basecube = new BaseCubeSparqlDerivedDatasetIterator(engine, so.dataseturi);
//		_root = basecube;
//	}
//
//	@Override
//	public void visit(ConvertCubeOp op) throws QueryException {
//		ConvertCubeOp so = (ConvertCubeOp) op;
//
//		ConvertSparqlDerivedDatasetIterator convertcontextcube;
//		if (so.inputOp2 == null) {
//			
//			so.inputOp1.accept(this);
//			PhysicalOlapIterator root = _root;
//			convertcontextcube = new ConvertSparqlDerivedDatasetIterator(engine, root,
//					null, so.conversioncorrespondence, so.domainUri);
//		} else if (so.inputOp1 == so.inputOp2) {
//			// If both operators are the same, we can reuse the iterator.
//			// Unfortunately, this does not work for further nested equal
//			// operators.
//			// If inside a logical olap query plan the same function is run on
//			// the same dataset, we
//			// still have a problem that it is run twice. we could have a
//			// hashmap storing
//			// previous operators and reusing them if needed.
//			so.inputOp1.accept(this);
//			PhysicalOlapIterator root = _root;
//			convertcontextcube = new ConvertSparqlDerivedDatasetIterator(engine, root,
//					root, so.conversioncorrespondence, so.domainUri);
//		} else {
//			so.inputOp1.accept(this);
//			PhysicalOlapIterator root1 = _root;
//			so.inputOp2.accept(this);
//			PhysicalOlapIterator root2 = _root;
//
//			convertcontextcube = new ConvertSparqlDerivedDatasetIterator(engine, root1,
//					root2, so.conversioncorrespondence, so.domainUri);
//		}
//
//		_root = convertcontextcube;
//	}
//
//	@Override
//	public void visit(Object op) throws QueryException {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException(
//				"visit(Object op) not implemented!");
//	}
//}
