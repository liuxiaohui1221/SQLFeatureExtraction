package sql.clickhouse.visitor;

import sql.clickhouse.parser.ast.expr.ColumnExpr;
import sql.clickhouse.parser.ast.expr.LiteralColumnExpr;

public class RewrittenDateSqlBuilder extends BaseSqlBuilder {

    private ComparedResult comparedResult;

    public RewrittenDateSqlBuilder(ComparedResult comparedResult) {
        this.comparedResult = comparedResult;
    }

    @Override
    public String visitLiteralColumnExpr(ColumnExpr expr) {
        if (null != expr && expr instanceof LiteralColumnExpr) {
            LiteralColumnExpr literalColumnExpr = (LiteralColumnExpr) expr;
            String value = visit(literalColumnExpr.getLiteral());
            if (value.equals(comparedResult.getSecondValueLowerBound())) {
                value = comparedResult.getSecondValueUpperBound();
            } else if (value.equals(comparedResult.getSecondValueUpperBound())) {
                value = comparedResult.getFirstValueUpperBound();
            }
            return value;
        }
        return null;
    }

}
