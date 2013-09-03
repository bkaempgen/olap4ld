//// (c) 2005 Andreas Harth
//package org.olap4j.driver.olap4ld.linkeddata;
//
//import org.semanticweb.yars.engine.QueryException;
//import org.semanticweb.yars.engine.Visitor;
//
///**
// * A visitor to translate a program tree to an XML tree
// * suitable for displaying.
// * 
// * @author aharth
// */
//public class Exec2XML implements LogicalOlapOperatorQueryPlanVisitor {
//    StringBuffer _buf = null;
//    
//    /**
//     * Constructor.
//     */
//    public Exec2XML() {
//    	_buf = new StringBuffer();
//    }
//    
//    /**
//     * Visitor to traverse the logical operator tree and
//     * spit out XML.
//     */
//    public void visit(Object op) throws QueryException {
//    	if (op instanceof ProjectIterator) {
//            _buf.append("<project><![CDATA[");
//            _buf.append(op.toString());
//            _buf.append("]]>");
//            ((ProjectIterator)op)._bi.accept(this);
//            _buf.append("</project>");
//    	} else if (op instanceof SelectIterator) {
//            _buf.append("<select><![CDATA[");
//            _buf.append(op.toString());
//            _buf.append("]]>");
//            ((SelectIterator)op)._oit.accept(this);
//            _buf.append("</select>");
//        } else if (op instanceof JoinINLIterator) {
//        	_buf.append("<joininl><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]>");
//        	_buf.append("<r>");
//        	((JoinINLIterator)op)._ri.accept(this);
//        	_buf.append("</r>");
//        	_buf.append("<s>");
//        	((JoinINLIterator)op)._si.accept(this);
//        	_buf.append("</s>");
//        	_buf.append("</joininl>");
//        } else if (op instanceof KeywordIterator) {
//        	_buf.append("<keyword><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]></keyword>");
//        } else if (op instanceof ValuesIterator) {
//        	_buf.append("<values><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]>");
//        	try {
//        		((ValuesIterator)op)._bit.accept(this);
//        	} catch (NullPointerException ex) {
//        		; // FIXME XXX @@@
//        	}
//        	_buf.append("</values>");
//        } else if (op instanceof ValuesListIterator) {
//        	_buf.append("<valueslist><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]>");
//        	try {
//        		((ValuesListIterator)op)._bit.accept(this);
//        	} catch (NullPointerException ex) {
//        		; // FIXME XXX @@@
//        	}
//        	_buf.append("</valueslist>");
//        } else if (op instanceof OIDIterator) {
//        	_buf.append("<oids><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]></oids>");
//        } else if (op instanceof TemplateIterator) {
//        	_buf.append("<template><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]>");
//        	((TemplateIterator)op)._vi.accept(this);
//        	_buf.append("</template>");
//        } else if (op instanceof DifferenceIterator) {
//        	_buf.append("<difference><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]>");
//        	_buf.append("<r>");
//        	((DifferenceIterator)op)._ri.accept(this);
//        	_buf.append("</r>");
//        	_buf.append("<s>");
//        	((DifferenceIterator)op)._si.accept(this);
//        	_buf.append("</s>");
//        	_buf.append("</difference>");
//        } else if (op instanceof UnionIterator) {
//        	_buf.append("<union><![CDATA[");
//        	_buf.append(op.toString());
//        	_buf.append("]]>");
//        	_buf.append("</union>");
//        } else {
//        	_buf.append("<unspec>");
//        	_buf.append(op.toString());
//        	_buf.append("</unspec>");
//        }
//    }
//    
//    public Object getNewRoot() {
//        return new String(_buf);
//    }
//}
