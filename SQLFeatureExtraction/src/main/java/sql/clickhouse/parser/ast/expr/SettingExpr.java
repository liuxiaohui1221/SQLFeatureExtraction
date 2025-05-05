package sql.clickhouse.parser.ast.expr;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.INode;
import sql.clickhouse.parser.ast.Identifier;
import sql.clickhouse.parser.ast.Literal;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class SettingExpr extends INode {

    private Identifier name;

    private Literal value;

    public SettingExpr(Identifier name, Literal value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitSettingExpr(this);
    }
}
