package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.ast.expr.ColumnExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class OrderByAlterTableClause extends AlterTableClause {

    private ColumnExpr expr;

    public OrderByAlterTableClause(ColumnExpr expr) {
        this.clauseType = ClauseType.ORDER_BY;
        this.expr = expr;
    }
}
