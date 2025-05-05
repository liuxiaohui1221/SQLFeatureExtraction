package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.ColumnExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class WhereClause extends SimpleClause {

    private ColumnExpr whereExpr;

    public WhereClause(ColumnExpr whereExpr) {
        this.whereExpr = whereExpr;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitWhereClause(this);
    }
}
