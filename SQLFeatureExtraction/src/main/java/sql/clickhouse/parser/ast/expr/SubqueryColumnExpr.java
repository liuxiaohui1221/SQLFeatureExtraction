package sql.clickhouse.parser.ast.expr;

import sql.clickhouse.parser.ast.SelectUnionQuery;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=true)
public class SubqueryColumnExpr extends ColumnExpr {

    private SelectUnionQuery query;

    protected SubqueryColumnExpr(SelectUnionQuery query) {
        super(ExprType.SUBQUERY);
        this.query = query;
    }

    public SelectUnionQuery getQuery() {
        return query;
    }

}
