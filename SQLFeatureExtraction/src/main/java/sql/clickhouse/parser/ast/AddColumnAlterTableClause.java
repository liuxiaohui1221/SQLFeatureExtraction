package sql.clickhouse.parser.ast;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.TableElementExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class AddColumnAlterTableClause extends AlterTableClause {

    private boolean ifNotExists;

    private TableElementExpr element;

    private Identifier after;

    public AddColumnAlterTableClause(boolean ifNotExists, TableElementExpr element, Identifier after) {
        this.clauseType = ClauseType.ADD_COLUMN;
        this.ifNotExists = ifNotExists;
        this.element = element;
        this.after = after;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitAddColumnAlterTableClause(this);
    }
}
