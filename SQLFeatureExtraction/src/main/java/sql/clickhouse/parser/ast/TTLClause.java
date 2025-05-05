package sql.clickhouse.parser.ast;


import sql.clickhouse.parser.AstVisitor;
import sql.clickhouse.parser.ast.expr.TTLExpr;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=true)
public class TTLClause extends SimpleClause {

    private List<TTLExpr> ttlExprList;

    @Override
    public <T> T accept(AstVisitor<T> astVisitor) {
        return astVisitor.visitTTLClause(this);
    }
}
