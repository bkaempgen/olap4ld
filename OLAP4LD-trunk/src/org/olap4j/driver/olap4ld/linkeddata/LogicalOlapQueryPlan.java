// (c) 2005 Andreas Harth
package org.olap4j.driver.olap4ld.linkeddata;

/**
 * A query plan.  How to encode the plan is not yet clear
 * is a plan constructed out of a program or a rule?
 * What happens to the program that is the answer to a query
 * and what to the program that is the "background knowledge"?
 * 
 * @author aharth
 */
public class LogicalOlapQueryPlan {
    // root of the plan (might be arraylist in case there's a forrest/union/cart product)
    LogicalOlapOp _root = null;
    
    /**
     * Constructor.
     */
    public LogicalOlapQueryPlan(LogicalOlapOp root) {
        _root = root;
    }
    
    public String toString() {
    	if (_root != null)
    		return _root.toString();
    	
    	return null;
    }
    
    /**
     * Generate the query plan (cascading iterators) based
     * on the plan and the Lexicon and the QuadIndex.
     * 
     * Return a list of variable bindings in clear text.
     * Maybe should have something to return a list of statements
     * as well (need to find out why conjunctions are not allowed
     * in the head).
     */
    /*
    public ValuesIterator generateIterator(Lexicon lex, QuadIndex quads) throws IOException {
        return new ValuesIterator(lex, _root.iterator(lex, quads));
    }
    */
    
    public String toXML() throws QueryException {
    	LogicalOlapQueryPlan2Xml rel2xml = new LogicalOlapQueryPlan2Xml();
    	_root.accept(rel2xml);
            
    	return (String)rel2xml.getNewRoot();
    }
    
    /**
     * Entry method that visits all nodes recursively and returns the root of the transformed tree.
     */
    public Object visitAll(Visitor v) throws QueryException {
        _root.accept(v);

        return v.getNewRoot();
    }
}
