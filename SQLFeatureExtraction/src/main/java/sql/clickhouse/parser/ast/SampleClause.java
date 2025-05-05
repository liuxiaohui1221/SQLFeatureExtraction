package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.RatioExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class SampleClause extends INode {

    private RatioExpr ratio;

    private RatioExpr offset;

    public SampleClause(RatioExpr ratio, RatioExpr offset) {
        this.ratio = ratio;
        this.offset = offset;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitSampleClause(this);
    }
}
