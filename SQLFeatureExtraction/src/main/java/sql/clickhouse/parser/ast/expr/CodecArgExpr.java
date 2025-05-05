package sql.clickhouse.parser.ast.expr;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.INode;
import sql.clickhouse.parser.ast.Identifier;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=true)
public class CodecArgExpr extends INode {

    private Identifier identifier;
    List<ColumnExpr> list;

    public CodecArgExpr(Identifier identifier, List<ColumnExpr> list) {
        this.identifier = identifier;
        this.list = list;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitCodecArgExpr(this);
    }
}
