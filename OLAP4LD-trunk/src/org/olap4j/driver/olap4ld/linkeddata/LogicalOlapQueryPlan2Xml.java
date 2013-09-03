package org.olap4j.driver.olap4ld.linkeddata;

/**
 * A visitor to translate a program tree to an XML tree
 * suitable for displaying.
 * 
 * @author aharth
 */
public class LogicalOlapQueryPlan2Xml implements LogicalOlapOperatorQueryPlanVisitor {
    StringBuffer _buf = new StringBuffer();
    
    /**
     * Visitor to traverse the logical operator tree and
     * spit out XML.
     */
    public void visit(Object op) throws QueryException {
        if (op instanceof BaseCubeOp) {
            _buf.append("<basecube><![CDATA[");
            _buf.append(op.toString());
            _buf.append("]]></basecube>\n");
        } else if (op instanceof ProjectionOp) {
        	ProjectionOp pop = (ProjectionOp)op;
        	
            _buf.append("<projection>\n");
            _buf.append("<![CDATA[");
            _buf.append(pop);
            _buf.append("]]>");
            pop.getInputOp().accept(this);
            _buf.append("</projection>\n");
        } else if (op instanceof DiceOp) {
        	DiceOp cop = (DiceOp)op;
        	
            _buf.append("<construct>\n");
            _buf.append("<![CDATA[");
            _buf.append(cop);
            _buf.append("]]>");
            cop.getInputOp().accept(this);
            _buf.append("</construct>\n");
//        } else if (op instanceof JoinOp) {
//        	JoinOp jop = (JoinOp)op;
//        	
//        	_buf.append("<join var='");
//        	_buf.append(jop._var);
//        	_buf.append("'>\n");
//        	jop._left.accept(this);
//            jop._right.accept(this);
//            _buf.append("</join>\n");
//        } else if (op instanceof DifferenceOp) {
//        	DifferenceOp dop = (DifferenceOp)op;
//        	
//        	_buf.append("<difference var='");
//        	_buf.append(dop._var);
//        	_buf.append("'>\n");
//        	dop._left.accept(this);
//            dop._right.accept(this);
//            _buf.append("</difference>\n");
        } else {
        	_buf.append("<unspec>");
        	_buf.append(toString());
        	_buf.append("</unspec>");
        }
    }
    
    public Object getNewRoot() {
        return new String(_buf);
    }

	@Override
	public void visit(RollupOp op) throws QueryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SliceOp op) throws QueryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DiceOp op) throws QueryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ProjectionOp op) throws QueryException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BaseCubeOp op) throws QueryException {
		// TODO Auto-generated method stub
		
	}
}
