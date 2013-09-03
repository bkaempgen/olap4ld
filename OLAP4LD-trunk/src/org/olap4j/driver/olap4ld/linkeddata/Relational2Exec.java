//// (c) 2005 Andreas Harth
//package org.semanticweb.yars.engine;
//
//import java.util.ArrayList;
//import java.util.SortedMap;
//import java.util.logging.Logger;
//
//import org.semanticweb.yars.api.n3.Quad;
//import org.semanticweb.yars.api.n3.Variable;
//import org.semanticweb.yars.api.rdf.Resource;
//import org.semanticweb.yars.engine.exec.local.DifferenceIterator;
//import org.semanticweb.yars.engine.exec.local.DistinctIterator;
//import org.semanticweb.yars.engine.exec.local.ExecIterator;
//import org.semanticweb.yars.engine.exec.local.FilterIneq;
//import org.semanticweb.yars.engine.exec.local.JoinINLIterator;
//import org.semanticweb.yars.engine.exec.local.KeywordIterator;
//import org.semanticweb.yars.engine.exec.local.OIDIterator;
//import org.semanticweb.yars.engine.exec.local.ProjectBAIterator;
//import org.semanticweb.yars.engine.exec.local.SelectIterator;
//import org.semanticweb.yars.engine.exec.local.TemplateIterator;
//import org.semanticweb.yars.engine.exec.local.ValuesIterator;
//import org.semanticweb.yars.engine.exec.local.ValuesListIterator;
//import org.semanticweb.yars.engine.relational.ConstructOp;
//import org.semanticweb.yars.engine.relational.DifferenceOp;
//import org.semanticweb.yars.engine.relational.JoinOp;
//import org.semanticweb.yars.engine.relational.ProjectOp;
//import org.semanticweb.yars.engine.relational.SelectOp;
//import org.semanticweb.yars.index.DataStore;
//
///**
// * Converts from relational algebra plans to physical access plans.
// * 
// * @author aharth
// */
//public class Relational2Exec implements LogicalOlapOperatorQueryPlanVisitor {
//	// the new root node
//	ExecIterator _root;
//	// the physical access plans need access to the data store component
//	DataStore _ds;
//    // logging
//    private static Logger _log = Logger.getLogger("org.semanticweb.yars.engine.Relational2Exec");
//    // the select clause...
//    private ExecIterator _result;
//    // list of variables for return values
//    private ArrayList _boundvars;
//    // select variables
//    private ArrayList _selectvars;
//    // 
//    private ArrayList _template;
//    //
//    private ArrayList _ineqfilter;
//    // check whether the thing is distinct
//    private boolean _distinct = false;
//    // 
//    private int _resultformat;
//    
//    final private static int PROJECT = 1;
//    final private static int PROJECTFILTER = 2;
//    final private static int CONSTRUCT = 4;
//    
//	/**
//	 * Constructor.
//	 * 
//	 * @param ds
//	 */
//	public Relational2Exec(DataStore ds) {	
//		_ds = ds;
//		_boundvars = new ArrayList();
//		_resultformat = 0;
//	}
//	
//	/**
//	 * A visitor object.
//	 * 
//	 * takes an node in the relational algebra tree and
//	 * converts it to a node in the physical access tree.
//	 * 
//	 * @param o
//	 */
//	public void visit(Object o) throws QueryException {
//		if (o instanceof SelectOp) {
//			ExecIterator bi = null;
//			SelectOp so = (SelectOp)o;
//			
//			// remote context? no, not yet, not here
//			/*
//			if (!(so._q.getContext() instanceof Variable)) {
//				_root = new org.semanticweb.yars.engine.remote.SelectIterator(so._q);
//				_variables.addAll(so._q.getVariables());
//			} else {
//			*/
//			
//			// keyword query?
//			if (so._q.isKeywordQuad()) {				
//				// generate the keyword iterator
//				KeywordIterator keyit = new KeywordIterator(_ds.getTextIndex(), so._q.getObject().toString());
//				
//				// build variable list as we go
//				_boundvars.addAll(getVariables(so._q, DataStore.TEXT));
//				
//				_root = keyit;
//			} else {
//				// find out index
//				int indexNo = whichIndex(so._q);
//				// construct the appropriate key for the query
//				SortedMap bt = _ds.getQuadIndex().getIndex(indexNo);
//
//				OIDIterator oit = new OIDIterator(_ds.getLexicon(), so._q);
//
//				_boundvars.addAll(getVariables(so._q, indexNo));
//			
//				bi = new SelectIterator(bt, indexNo, oit);
//				//ProjectIterator pi = new ProjectIterator(bi, getMask(indexNo));
//				_root = bi;
//			}
//		} else if (o instanceof ConstructOp) {
//			// if there's a construct operator, just visit the children
//			ConstructOp cop = (ConstructOp)o;
//			
//			_template = cop._template;
//			//_result = new TemplateIterator(null, cop._template);
//			_resultformat = CONSTRUCT;
//			
//			cop._o.accept(this);
//		} else if (o instanceof ProjectOp) {
//			// if there's a project operator, just visit the children
//			ProjectOp po = (ProjectOp)o;
//			
//			_selectvars = po.getVariableList();
//			
//			_ineqfilter = po.getIneqFilter();
//			_distinct = po.getDistinct();
//			
//			if (_ineqfilter != null) {
//				_resultformat = PROJECTFILTER;
//			} else
//				_resultformat = PROJECT;
//
//			po._o.accept(this);
//		} else if (o instanceof JoinOp) {
//			ExecIterator bi = null;
//			JoinOp jo = (JoinOp)o;
//			long rpos = 0;
//			long spos = 0;
//			
//			jo._left.accept(this);
//			
//			// replace the join condition with a non-variable
//			// that we can figure out the right index
//			Quad s = new Quad(jo._right._q);
//			
//			if (s.getSubject() == jo._var) {
//				s.setSubject(new Resource("joinie"));
//				spos = 0l;
//			} else if (s.getPredicate() == jo._var) {
//				s.setPredicate(new Resource("joinie"));
//				spos = 1l;
//			} else if (s.getObject() == jo._var) {
//				s.setObject(new Resource("joinie"));
//				spos = 2l;
//			} else if (s.getContext() == jo._var) {
//				s.setContext(new Resource("joinie"));
//				spos = 3l;
//			}
//			
//			int indexNo = whichIndex(s);
//			
//			SelectIterator si = new SelectIterator(_ds.getQuadIndex().getIndex(indexNo),
//											indexNo,
//											new OIDIterator(_ds.getLexicon(), jo._right._q));
//			
//			rpos = _boundvars.indexOf(jo._var);
//			
//			// construct join iterator, note that spos and rpos are not masks
//			// by merely the position in the array
//			bi = new JoinINLIterator(_root, si, rpos, spos);
//			
//			// build variable list as we go
//			_boundvars.addAll(getVariables(s, indexNo));
//			
//			_root = bi;
//		} else if (o instanceof DifferenceOp) {
//			ExecIterator bi = null;
//			DifferenceOp dio = (DifferenceOp)o;
//			long rpos = 0;
//			long spos = 0;
//			
//			dio._left.accept(this);
//			
//			// replace the join condition with a non-variable
//			// that we can figure out the right index
//			Quad s = new Quad(dio._right._q);
//			
//			if (s.getSubject() == dio._var) {
//				s.setSubject(new Resource("diffie"));
//				spos = 0l;
//			} else if (s.getPredicate() == dio._var) {
//				s.setPredicate(new Resource("diffie"));
//				spos = 1l;
//			} else if (s.getObject() == dio._var) {
//				s.setObject(new Resource("diffie"));
//				spos = 2l;
//			} else if (s.getContext() == dio._var) {
//				s.setContext(new Resource("diffie"));
//				spos = 3l;
//			}
//			
//			int indexNo = whichIndex(s);
//			
//			SelectIterator si = new SelectIterator(_ds.getQuadIndex().getIndex(indexNo),
//					indexNo,
//					new OIDIterator(_ds.getLexicon(), dio._right._q));
//			
//			rpos = _boundvars.indexOf(dio._var);
//			
//			// construct join iterator, note that spos and rpos are not masks
//			// by merely the position in the array
//			bi = new DifferenceIterator(_root, si, rpos, spos);
//			
//			// build variable list as we go
//			// no, not for difference _variables.addAll(getVariables(s, indexNo));
//			
//			_root = bi;
//		}
//	}
//	
//	/**
//	 * Return a new root node.
//	 */
//	public Object getNewRoot() {
//		// XXX the iterators should be stupid, all transformations should
//		// be made here (e.g. constructing the template, constructing the
//		// mapping etc.
//		
//		System.out.println("bound variables " + _boundvars);
//		System.out.println("select variables " + _selectvars);
//		
//		if (_resultformat == PROJECT || _resultformat == PROJECTFILTER) {
//			// mapping has a mapping from the _variables to the select specification
//			// i.e. which indexes of the _variables should be used in the real result
//			// mapping from _variables to po._varlist
//
//			int mapping[] = new int[_selectvars.size()];
//			
//			for (int i=0; i<_selectvars.size(); i++) {
//				mapping[i] = _boundvars.indexOf(_selectvars.get(i));
//			}
//
//			if (_resultformat == PROJECT) {
//				if (_distinct == true) {
//					_result = new ValuesListIterator(_ds.getLexicon(), new DistinctIterator(new ProjectBAIterator(_root, mapping, _selectvars)), _boundvars);
//				} else {
//					_result = new ValuesListIterator(_ds.getLexicon(), new ProjectBAIterator( _root, mapping, _selectvars), _boundvars);
//				}
//			} else {
//				int[] ineq = new int[2];
//
//				ineq[0] = _boundvars.indexOf(_ineqfilter.get(0));
//				ineq[1] = _boundvars.indexOf(_ineqfilter.get(1));
//				
//				_result = new ValuesListIterator(_ds.getLexicon(), new ProjectBAIterator(new FilterIneq(_root, ineq), mapping, _selectvars), _boundvars);
//			}
//		} else if (_resultformat == CONSTRUCT) {
//			// FIXME XXX should project out stuff earlier to 
//			// not do the unncesssary join with the lexicon for 
//			// nodes that are projected out lateron anyways
//			
//			// get the variables from the template and provide a mapping
//			_selectvars = new ArrayList();
//			
//			for (int i=0; i < _template.size(); i++) {
//				if (_template.get(i) instanceof Quad) {
//					Quad q = (Quad)_template.get(i);
//					//Variable v = (Variable)_template.get(i);
//					_selectvars.add(q.getVariables());
//				}
//			}
//			
//			_log.info("selectvars " + _selectvars + " tempalte " + _template + " boundvars " + _boundvars);
//			
//			// mapping has a mapping from the _variables to the select specification
//			// i.e. which indexes of the _variables should be used in the real result
//			// mapping from _variables to po._varlist
//
//			int mapping[] = new int[_selectvars.size()];
//			
//			for (int i=0; i<_selectvars.size(); i++) {
//				mapping[i] = _boundvars.indexOf(_selectvars.get(i));
//			}
//			
//			if (_distinct == true) {
//				throw new ClassCastException("distinct not yet supported for triples");
//			} else {
//				_result = new TemplateIterator(new ValuesIterator(_ds.getLexicon(), _root, _boundvars), _template);
//				//_result = new TemplateIterator(new ValuesIterator(_ds.getLexicon(), new ProjectBAIterator(_root, mapping, _selectvars), _selectvars), _template);
//			}
//			//_result = new TemplateIterator(new ValuesIterator(_ds.getLexicon(), _root, _boundvars), _template);
//		} else {
//			throw new ClassCastException("result iterator is neither project nor template!");
//		}
//		
//		return _result;
//	}
//	
//	/**
//	 * Get variables out of the adornments and index no
//	 * @param adornments
//	 * @return
//	 */
//	private ArrayList getVariables(Quad stmt, int indexNo) {
//		ArrayList variables = new ArrayList();
//		
//    	switch (indexNo) {
//    	case DataStore.NIL:
//    		variables.add(stmt.getSubject());
//    		variables.add(stmt.getPredicate());
//    		variables.add(stmt.getObject());
//    		variables.add(stmt.getContext());
//    		break;
//    	case DataStore.S:
//    		variables.add(stmt.getPredicate());
//    		variables.add(stmt.getObject());
//    		variables.add(stmt.getContext());
//    		break;
//    	case DataStore.SP:
//    		variables.add(stmt.getObject());
//    		variables.add(stmt.getContext());
//    		break;
//    	case DataStore.SPO:
//    		variables.add(stmt.getContext());
//    		break;
//    	case DataStore.SPOC:
//    		break;
//    	case DataStore.O:
//    		variables.add(stmt.getContext());
//    		variables.add(stmt.getSubject());
//    		variables.add(stmt.getPredicate());
//    		break;
//    	case DataStore.OC:
//    		variables.add(stmt.getSubject());
//    		variables.add(stmt.getPredicate());
//    		break;
//    	case DataStore.OCS:
//    		variables.add(stmt.getPredicate());
//    		break;
//    	case DataStore.P:
//    		variables.add(stmt.getObject());
//    		variables.add(stmt.getContext());
//    		variables.add(stmt.getSubject());
//    		break;
//    	case DataStore.PO:
//    		variables.add(stmt.getContext());
//    		variables.add(stmt.getSubject());
//    		break;
//    	case DataStore.POC:
//    		variables.add(stmt.getSubject());
//    		break;	
//    	case DataStore.C:
//    		variables.add(stmt.getPredicate());
//    		variables.add(stmt.getObject());
//    		variables.add(stmt.getSubject());
//    		break;
//    	case DataStore.CS:
//    		variables.add(stmt.getPredicate());
//    		variables.add(stmt.getObject());
//    		break;
//    	case DataStore.CSP:
//    		variables.add(stmt.getObject());
//    		break;
//    	case DataStore.CP:
//    		variables.add(stmt.getObject());
//    		variables.add(stmt.getSubject());
//    		break;
//    	case DataStore.OS:
//    		variables.add(stmt.getContext());
//    		variables.add(stmt.getPredicate());
//    		break;
//    	case DataStore.TEXT:
//    		variables.add(stmt.getSubject());
//    		break;
//    	}
//    	
//    	return variables;
//	}
//	
//    // project out the variables based on index no
//	// XXX FIXME that's strange, shouldn't project mask specify what's left, not what's removed?
//	// that seems inconsistent with the join stuff where equality is determined on the masks
//	// that stay
//	/*
//	private long _mask[] = {15l, 14l, 14l, 12l, 14l, 12l, 12l, 8l, 14l, 12l, 12l, 8l, 12l, 8l, 8l, 0l};
//	
//	private long getMask(int indexNo) {
//		return _mask[indexNo];
//	}
//	*/
//	
//    /**
//     * Figure out what index to use for retrieval
//     * of a non-ground quad (or spoc for ground quads).
//     */
//    private int whichIndex(Quad query) {
//        // check which things are variables
//    	boolean s = query.getSubject() instanceof Variable;
//    	boolean p = query.getPredicate() instanceof Variable;
//    	boolean o = query.getObject() instanceof Variable;
//    	boolean c = query.getContext() instanceof Variable;
//
//    	_log.fine("s var: " + s + " p var: " + p + " o var: " + o + " c var: " + c);
// 
//    	// calculate what index to use
//    	int indexNo = c ? 0 : 1;
//    	indexNo += o ? 0 : 2;
//    	indexNo += p ? 0 : 4;
//    	indexNo += s ? 0 : 8;
//    	
//    	return indexNo;
//    }
//}
