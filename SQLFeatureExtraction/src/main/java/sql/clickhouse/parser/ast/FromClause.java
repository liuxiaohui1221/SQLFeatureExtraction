package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.JoinExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class FromClause extends INode {

    private JoinExpr expr;

    public FromClause(JoinExpr expr) {
        this.expr = expr;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitFromClause(this);
    }
}
