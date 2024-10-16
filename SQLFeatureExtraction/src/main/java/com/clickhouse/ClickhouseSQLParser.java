package com.clickhouse;

import com.clickhouse.parser.AstParser;
import com.clickhouse.parser.ast.SelectStatement;
import com.clickhouse.parser.ast.SelectUnionQuery;
import com.clickhouse.parser.ast.expr.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
@Slf4j
public class ClickhouseSQLParser {
    public static final AtomicInteger successCount = new AtomicInteger(0);
    public static final AtomicInteger failCount = new AtomicInteger(0);
    private volatile String query;
//    public static void main(String[] args) {
//        CalciteSQLParser calciteSQLParser = new CalciteSQLParser();
//        String tsvFilePath = "input/ApmQuerys.tsv"; // TSV 文件路径
//        try (Reader reader = Files.newBufferedReader(Paths.get(tsvFilePath))) {
//            CSVParser csvParser = new CSVParser(reader, CSVFormat.TDF.withFirstRecordAsHeader());
//            Iterable<CSVRecord> records = csvParser.getRecords();
//
//            // 处理标题行
//            List<String> headers = csvParser.getHeaderNames();
//            System.out.println("Headers: " + headers);
//            int count=0;
//            for (CSVRecord record : records) {
//                String query = record.get("query");
//                if(!ExcelReader.filterSql(query)){
//                    continue;
//                }
//                count++;
//                System.out.println("origin query:"+query);
//                calciteSQLParser.createQueryVector(query);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("successCount:"+successCount.get());
//        System.out.println("failCount:"+failCount.get());
//    }

    public void createQueryVector(String query){
        this.query=query;
        try {
            /*SqlParser.Config config = SqlParser.configBuilder()
                    .setLex(Lex.JAVA) // ClickHouse方言可能需要LEX设置为Lex.JAVA
                    .setParserFactory(SqlParserImpl.FACTORY)
                    .setConformance(SqlConformanceEnum.BABEL)
                    .build();
            // 创建解析器
            SqlParser parser = SqlParser.create(query, config);
            // 解析sql
            SqlNode sqlNode = parser.parseQuery(query);
            SqlDialect.Context MY_CONTEXT = SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE)
                    .withIdentifierQuoteString("`")
                    .withNullCollation(NullCollation.LOW);
            // 还原某个方言的SQL
            SqlString sqlString = sqlNode.toSqlString(new ClickHouseSqlDialect(MY_CONTEXT));
            query = sqlString.getSql();
            System.out.println("com.clickhouse query:"+query);*/
            AstParser astParser = new AstParser();
            Object parsedResult = astParser.parse(query);
            if(parsedResult instanceof SelectUnionQuery){
                extractedSelectQuery((SelectUnionQuery) parsedResult);
            }else{
                System.out.println("not select query:"+query);
            }
            successCount.incrementAndGet();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("parser error:"+query);
            failCount.incrementAndGet();
        }
    }

    private void extractedSelectQuery(SelectUnionQuery parsedResult) {
        List<SelectStatement> statements = parsedResult.getStatements();
        for (SelectStatement statement : statements) {
            extractedSelectQuery(statement);
        }
    }

    private void extractedSelectQuery(SelectStatement statement) {
        //抽取select字段
        List<ColumnExpr> exprs = statement.getExprs();
        for (ColumnExpr expr : exprs) {
            extractedColumnExpr(expr);
        }
    }

    private void extractedColumnExpr(ColumnExpr expr) {
        if(expr instanceof IdentifierColumnExpr){
            IdentifierColumnExpr colExpr = (IdentifierColumnExpr)expr;
            String name = colExpr.getIdentifier().getName();
            System.out.println("column:"+name);
        }else if(expr instanceof AliasColumnExpr){
            AliasColumnExpr colExpr = (AliasColumnExpr)expr;
            ColumnExpr expr1 = colExpr.getExpr();
            extractedColumnExpr(expr1);
        }else if(expr instanceof FunctionColumnExpr){
            FunctionColumnExpr colExpr = (FunctionColumnExpr)expr;
            List<ColumnExpr> args = colExpr.getArgs();
            if(args!=null){
                for(ColumnExpr arg:args){
                    extractedColumnExpr(arg);
                }
            }else if("count".equalsIgnoreCase(colExpr.getName().getName())){

            }else{
                System.out.println("not supported FunctionColumnExpr:"+expr);
            }
        } else if(expr instanceof LiteralColumnExpr){
        }else{
            System.out.println("not supported columnExpr:"+expr);
        }
    }
}
