package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.ast.expr.ColumnExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class DeleteAlterTableClause extends AlterTableClause {

    private ColumnExpr expr;

    public DeleteAlterTableClause(ColumnExpr expr) {
        this.clauseType = ClauseType.DELETE;
        this.expr = expr;
    }
}
