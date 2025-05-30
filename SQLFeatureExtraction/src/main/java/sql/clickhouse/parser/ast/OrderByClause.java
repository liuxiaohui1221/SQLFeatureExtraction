package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.OrderExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=true)
public class OrderByClause extends SimpleClause {

    private List<OrderExpr> orderExprs;

    public OrderByClause(List<OrderExpr> orderExprs) {
        this.orderExprs = orderExprs;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitOrderByClause(this);
    }
}
