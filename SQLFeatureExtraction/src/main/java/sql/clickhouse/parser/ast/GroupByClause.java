package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.ColumnExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=true)
public class GroupByClause extends SimpleClause {

    private List<ColumnExpr> groupByExprs;

    public GroupByClause(List<ColumnExpr> groupByExprs) {
        this.groupByExprs = groupByExprs;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitGroupByClause(this);
    }
}
