package com.clickhouse;

import com.clickhouse.parser.AstParser;
import com.clickhouse.parser.ast.*;
import com.clickhouse.parser.ast.expr.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
@Slf4j
@Data
public class ClickhouseSQLParser {
    public static final AtomicInteger successCount = new AtomicInteger(0);
    public static final AtomicInteger failCount = new AtomicInteger(0);
    private Integer[] queryGranularitysList;
    private volatile String query;
    private SchemaParser schParse;
    private HashSet<String> selectionColumns = new HashSet<>();
    private List<String> fromTables = new ArrayList<>();
    private HashSet<String> whereColumns=new HashSet<>();
    private HashSet<String> groupByColumns= new HashSet<>();
//    private HashSet<String> havingColumns=new HashSet<>();
    private HashSet<String> orderByColumns=new HashSet<>();
    private HashMap<String,ColumnExpr> colAliases=new HashMap<>();
    private HashSet<String> sumColumns=new HashSet<>();
    private HashSet<String> maxColumns=new HashSet<>();
    private HashSet<String> minColumns=new HashSet<>();
    private HashSet<String> avgColumns=new HashSet<>();
    private Integer timeOffsetWhere=null;
    private Integer timeRangeWhere=null;

    private long eventTime;
    private long tsStartSecond;
    private long tsEndSecond;
    private boolean[] queryGranularitys;//分别对分钟，小时，天，月，年等粒度取整后的值取0或1，当sql中没有时间聚合粒度时，全为0

    public ClickhouseSQLParser(SchemaParser schParse, Integer[] queryGranularitysList){
        this.schParse=schParse;
        this.queryGranularitysList=queryGranularitysList;
        queryGranularitys=new boolean[queryGranularitysList.length];
    }
    public ClickhouseSQLParser(SchemaParser schParse){
        this.schParse=schParse;
    }

    public void createQueryVector(String query,long eventTime){
        this.eventTime=eventTime;
        this.query=query;
        try {
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
        //抽取from中表
        FromClause fromClause = statement.getFromClause();
        if(fromClause==null){
            return;
        }
        if(fromClause.getExpr().getTableExpr().getIdentifier()==null){
            return;
        }
        String name = fromClause.getExpr().getTableExpr().getIdentifier().getName();
        if(name.endsWith("_cluster")){
            name = name.substring(0,name.length()-8);
        }
        if(name.endsWith("_view")){
            name = name.substring(0,name.length()-5);
        }
        System.out.println("table:"+name);
        fromTables.add(name);
        //抽取select字段，聚合函数
        List<ColumnExpr> exprs = statement.getExprs();
        for (ColumnExpr expr : exprs) {
            extractedColumnExpr(expr,selectionColumns);
        }

        //抽取where条件字段
        WhereClause whereClause = statement.getWhereClause();
        if(whereClause!=null){
            extractedWhereClause(whereClause);
        }
        timeRangeWhere=(int) (tsEndSecond-tsStartSecond);
        //抽取group by字段
        GroupByClause groupByClause = statement.getGroupByClause();
        if(groupByClause!=null){
            List<ColumnExpr> groupByExprs = groupByClause.getGroupByExprs();
            if(groupByExprs != null){
                for (ColumnExpr expr : groupByExprs)
                    extractedColumnExpr(expr,groupByColumns);
            }
        }
        //抽取order by字段
        OrderByClause orderByClause = statement.getOrderByClause();
        if(orderByClause!=null){
            List<OrderExpr> orderByExprs = orderByClause.getOrderExprs();
            if(orderByExprs != null)
                for (OrderExpr expr : orderByExprs)
                    extractedColumnExpr(expr.getExpr(),orderByColumns);
        }
    }

    private void extractedWhereClause(WhereClause whereClause) {
        ColumnExpr whereExpr = whereClause.getWhereExpr();
        extractedColumnExpr(whereExpr,whereColumns);
    }

    private void extractedColumnExpr(ColumnExpr expr,HashSet<String> selectionColumns) {
        if(expr instanceof IdentifierColumnExpr){
            IdentifierColumnExpr colExpr = (IdentifierColumnExpr)expr;
            String name = colExpr.getIdentifier().getName();
            //System.out.println("column:"+name);
            if(!colAliases.containsKey(name)){
                selectionColumns.add(name);
            }else{
                //别名替换
                ColumnExpr expr1 = colAliases.get(name);
                extractedColumnExpr(expr1,selectionColumns);
            }
        }else if(expr instanceof AliasColumnExpr){
            AliasColumnExpr colExpr = (AliasColumnExpr)expr;
            //在同一个SELECT语句的WHERE、ORDER BY、GROUP BY等子句中引用别名。
            String colAlias = colExpr.getAlias().getName();
            colAliases.put(colAlias,colExpr.getExpr());
            ColumnExpr expr1 = colExpr.getExpr();
            extractedColumnExpr(expr1,selectionColumns);
        }else if(expr instanceof FunctionColumnExpr){
            FunctionColumnExpr colExpr = (FunctionColumnExpr)expr;
            if(!"equals".equals(colExpr.getName().getName())){
                List<ColumnExpr> args = colExpr.getArgs();
                if(args!=null) {
                    if(args.get(0) instanceof IdentifierColumnExpr){
                        IdentifierColumnExpr aggColExpr=(IdentifierColumnExpr)args.get(0);
                        if("sum".equals(colExpr.getName().getName())){
                            sumColumns.add(aggColExpr.getIdentifier().getName());
                        }else if("max".equals(colExpr.getName().getName())){
                            maxColumns.add(aggColExpr.getIdentifier().getName());
                        }else if("min".equals(colExpr.getName().getName())){
                            minColumns.add(aggColExpr.getIdentifier().getName());
                        }else if("avg".equals(colExpr.getName().getName())){
                            avgColumns.add(aggColExpr.getIdentifier().getName());
                        }else if("ts".equals(aggColExpr.getIdentifier().getName())){
                            if("greaterOrEquals".equals(colExpr.getName().getName())||"lessOrEquals".equals(colExpr.getName().getName())){
                                if(args.get(1) instanceof FunctionColumnExpr){
                                    FunctionColumnExpr funcExpr = (FunctionColumnExpr)args.get(1);
                                    if("todatetime64".equals(funcExpr.getName().getName())) {
                                        ColumnExpr columnExpr = funcExpr.getArgs().get(0);
                                        if (columnExpr instanceof LiteralColumnExpr) {
                                            LiteralColumnExpr literalColumnExpr = (LiteralColumnExpr) columnExpr;
                                            String startStr = literalColumnExpr.getLiteral().asStringWithoutQuote();
                                            if("greaterOrEquals".equals(colExpr.getName().getName())){
                                                tsStartSecond=Long.parseLong(startStr.substring(0,10));
                                            }else{
                                                tsEndSecond=Long.parseLong(startStr.substring(0,10));
                                                if(eventTime!=0){
                                                    timeOffsetWhere=(int) (eventTime-tsEndSecond);
                                                }
                                            }

                                        }
                                    }
                                }
                            }else if("tostartofinterval".equalsIgnoreCase(colExpr.getName().getName())){
                                if(args.get(1) instanceof FunctionColumnExpr){
                                    FunctionColumnExpr funcExpr = (FunctionColumnExpr)args.get(1);
                                    if("tointervalday".equalsIgnoreCase(funcExpr.getName().getName())) {
                                        LiteralColumnExpr columnExpr = (LiteralColumnExpr) funcExpr.getArgs().get(0);
                                        Integer num= Integer.valueOf(columnExpr.getLiteral().asStringWithoutQuote());
                                        if(num>=1){
                                            queryGranularitys[4]=true;//1day
                                        }
                                        if(num/7>=1){
                                            queryGranularitys[5]=true;//1 week
                                        }
                                        if(num/30>=1){
                                            queryGranularitys[6]=true;//1 month
                                        }
                                        if (num/(3*30)>=1){
                                            queryGranularitys[7]=true;//3 month
                                        }
                                        if(num/(365)>=1){
                                            queryGranularitys[8]=true;//1 year
                                        }
                                    }else if ("tointervalhour".equalsIgnoreCase(funcExpr.getName().getName())) {
                                        queryGranularitys[3]=true;
                                    }else if ("tointervalminute".equalsIgnoreCase(funcExpr.getName().getName())) {
                                        queryGranularitys[0]=true;
                                    }else if ("tointervalmonth".equalsIgnoreCase(funcExpr.getName().getName())) {
                                        queryGranularitys[6]=true;
                                    }else if ("tointervalyear".equalsIgnoreCase(funcExpr.getName().getName())){
                                        queryGranularitys[8]=true;
                                    }
                                }

                            }
                        }
                    }
                    for (ColumnExpr arg : args) {
                        extractedColumnExpr(arg, selectionColumns);
                    }
                }else if("count".equalsIgnoreCase(colExpr.getName().getName())){
                }else{
                    System.out.println("not supported FunctionColumnExpr:"+expr);
                }
            }
        } else if(expr instanceof LiteralColumnExpr){
        }else{
            System.out.println("not supported columnExpr:"+expr);
        }
    }
}
