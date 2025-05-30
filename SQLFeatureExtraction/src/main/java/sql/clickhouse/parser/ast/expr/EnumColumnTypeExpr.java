package sql.clickhouse.parser.ast.expr;

import sql.clickhouse.parser.ast.EnumValue;
import sql.clickhouse.parser.ast.Identifier;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper=true)
public class EnumColumnTypeExpr extends ColumnTypeExpr {

    private List<EnumValue> enumValues;

    public EnumColumnTypeExpr(Identifier name, List<EnumValue> enumValues) {
        super(ExprType.ENUM, name);
        this.enumValues = enumValues;
    }

    public List<EnumValue> getEnumValues() {
        return enumValues;
    }

}
