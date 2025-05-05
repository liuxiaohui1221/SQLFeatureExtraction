package sql.clickhouse.parser.ast.expr;

import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.INode;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=true)
public class CodecExpr extends INode {

    private List<CodecArgExpr> codeArgExprList;

    public CodecExpr(List<CodecArgExpr> codeArgExprList) {
        this.codeArgExprList = codeArgExprList;
    }

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitCodecExpr(this);
    }
}
