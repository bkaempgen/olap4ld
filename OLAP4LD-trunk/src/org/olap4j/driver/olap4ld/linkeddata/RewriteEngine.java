//// (c) 2005 Andreas Harth
//package org.semanticweb.yars.engine;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.logging.Logger;
//
//import org.semanticweb.yars.api.n3.List;
//import org.semanticweb.yars.api.n3.Quad;
//import org.semanticweb.yars.api.n3.QuadSet;
//import org.semanticweb.yars.api.n3.Query;
//import org.semanticweb.yars.api.rdf.NodeSet;
//import org.semanticweb.yars.api.rdf.Resource;
//import org.semanticweb.yars.engine.datalog.Answer;
//import org.semanticweb.yars.engine.datalog.DatalogPlan;
//import org.semanticweb.yars.engine.datalog.FilterIneq;
//import org.semanticweb.yars.engine.datalog.NafOp;
//import org.semanticweb.yars.engine.datalog.NotOp;
//import org.semanticweb.yars.engine.datalog.OrOp;
//import org.semanticweb.yars.engine.datalog.Subgoal;
//import org.semanticweb.yars.engine.exec.local.ExecIterator;
//import org.semanticweb.yars.engine.exec.local.ExecPlan;
//import org.semanticweb.yars.engine.relational.RelationalOp;
//import org.semanticweb.yars.engine.relational.RelationalPlan;
//import org.semanticweb.yars.index.DataStore;
//
///**
// * Here are static methods that perform the compilation steps
// * from the logic program to relational algebra to access
// * operators.
// * 
// * see http://hake.stanford.edu/~widom/cs346/qpnotes.html
// * 
// * @author aharth
// */
//public class RewriteEngine {
//    // logging
//    private static Logger _log = Logger.getLogger("org.semanticweb.yars.engine.RewriteEngine");
//    
//    /**
//     * get the plans in XML format.
//     * 
//     * @param query
//     * @param ds
//     * @return
//     */
//    public static String getPlansXML(Query rule, DataStore ds, Resource context) throws QueryException {
//    	StringBuffer buf = new StringBuffer();
//    	long timeStart = System.currentTimeMillis();
//    	
//    	buf.append("<?xml version='1.0' encoding='utf-8'?>\n");
//        
//        buf.append("<query><n3>");
//        buf.append(rule.toN3());
//        /*
//        for (int i = 0; i < query.size(); i++) {
//            buf.append("<quad><![CDATA[\n");
//            buf.append(query.get(i));
//            buf.append("]]></quad>\n");
//        }
//        */
//        buf.append("</n3>");
//        
//        DatalogPlan p = RewriteEngine.queryToLogic(rule, context);
//        buf.append("<log>");
//        buf.append(p.toXML());
//        buf.append("</log>");
//        LogicalOlapQueryPlan rp = RewriteEngine.logicToRelational(p);
//        buf.append("<rp>");
//        buf.append(rp.toXML());
//        buf.append("</rp>");
//        buf.append("<acc>");
//        ExecPlan ap = RewriteEngine.relationalToExec(rp, ds);
//        buf.append(ap.toXML());
//        buf.append("</acc>");
//        
//        long timeEnd = System.currentTimeMillis();
//        
//        buf.append("<time>");
//        buf.append(timeEnd-timeStart);
//        buf.append(" ms</time>");
//        buf.append("</query>");
//        
//        return new String(buf);
//    }
//    
//    /**
//     * Get a complete physical access plan.
//     * 
//     * @param context the context which should be the default context for this query
//     */
//    public static ExecPlan getAccessPlan(Query query, DataStore ds, Resource context) throws QueryException {
//    	DatalogPlan log = queryToLogic(query, context);
//    	LogicalOlapQueryPlan rp = logicToRelational(log);
//    	
//    	return relationalToExec(rp, ds);
//    }
//    
//    /**
//     * Rewrite a query to a program.
//     * This is done here and not in a Query2Datalog object using the visitor pattern (XXX why not?)
//     * 
//     * @param query the query object
//     * @param context the context where the query was posed
//     */
//    public static DatalogPlan queryToLogic(Query query, Resource context) {
//    	_log.fine("rewriting query " + query);
//        
//    	NodeSet select = query.getSelect();
//    	QuadSet where = query.getWhere();
//
//    	where.setDefaultContext(context);
//    	
//        OrOp root = new OrOp();
//        Iterator it = null;
//        
//        // select part can be either a set of quads (template) or a list
//        if (select instanceof QuadSet) {
//        	it = select.iterator();
//        
//        	while (it.hasNext()) {
//        		root.add(new Subgoal((Quad)it.next()));
//        	}
//        } else if (select instanceof List) {
//        	ArrayList list = ((List)select).getList();
//        	root.add(new Answer(list, query.getDistinct()));
//        }
//
//        // optimize (sort statements based on number of variables)
//        // that's resembling a join reordering optimization
//        // keyword quads have weight 0, that is, they
//        // are evaluated first
//        Collections.sort(where);
//        
//        it = where.iterator();
//        
//        // the keyword stuff is only evaluated in 
//        // raltional2exec, because the keywords things 
//        // amounts to a join
//        while (it.hasNext()) {
//            NotOp not = new NotOp();
//            Quad quad = (Quad)it.next();
//            if (quad.isNafQuad()) {
//            	NafOp naf = new NafOp();
//            	naf.add(new Subgoal(quad));
//            	not.add(naf);
//            } else if (quad.isIneqQuad()) {
//            	FilterIneq ineq = new FilterIneq(quad);
//            	not.add(ineq);
//            } else {
//            	not.add(new Subgoal(quad));
//            }
//            
//            root.add(not);
//        }
//        
//        return new DatalogPlan(root);
//    }
//    
//    /**
//     * Rewrite a program to a plan.
//     * 
//     * Basically, do a union-find to detect unions and joins.
//     */
//    public static LogicalOlapQueryPlan logicToRelational(DatalogPlan p) throws QueryException {
//        RelationalOp newRoot = (RelationalOp)p.visitAll(new Datalog2Relational());
//
//        return new LogicalOlapQueryPlan(newRoot);
//    }
//    
//    /**
//     * Rewrite a relational algebra plan to a physical access plan.
//     */
//    public static ExecPlan relationalToExec(LogicalOlapQueryPlan rp, DataStore ds) throws QueryException {
//    	Relational2Exec r2a = new Relational2Exec(ds);
//    	ExecIterator newRoot = (ExecIterator)rp.visitAll(r2a);
//    	
//    	_log.finer("bytes iterator " + newRoot);
//    	
//    	ExecPlan ap = new ExecPlan(newRoot);
//
//    	return ap;
//    }
//}
