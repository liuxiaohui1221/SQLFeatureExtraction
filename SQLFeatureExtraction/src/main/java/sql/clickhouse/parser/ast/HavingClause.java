package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.ColumnExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class HavingClause extends SimpleClause {

    private ColumnExpr havingExpr;

    public HavingClause(ColumnExpr havingExpr) {
        this.havingExpr = havingExpr;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitHavingClause(this);
    }
}
