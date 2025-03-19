import com.clickhouse.SchemaParser;
import com.google.common.collect.Maps;
import toolsForMetrics.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class APMWindowFragmentIntent {

    // 配置参数
    private final Duration windowSize;
    private final int topK;
    private final int topN;
    private final SlideMode slideMode;

    public enum SlideMode {
        FIXED, PER_QUERY
    }

    public APMWindowFragmentIntent(Duration windowSize, int topK, int topN, SlideMode slideMode) {
        this.windowSize = windowSize;
        this.topK = topK;
        this.topN = topN;
        this.slideMode = slideMode;
    }

    // 查询记录结构
    static class QueryRecord {
        private final long eventTimeMills;
        LocalDateTime eventTime;
        long duration;
        String sql;
        String table;

        public QueryRecord(LocalDateTime eventTime,long eventTimeMills, long duration, String sql, String table) {
            this.eventTime = eventTime;
            this.eventTimeMills = eventTimeMills;
            this.duration = duration;
            this.sql = sql;
            this.table = table;
        }

        public String getTable() {
            return this.table;
        }
    }

    // 主处理流程
    public List<String> process(Path tsvFile, SchemaParser schParse, boolean includeSelOpConst, boolean isTableGroupSequence) throws Exception {
        List<String> sqlEncodedList=new ArrayList<>();
        List<QueryRecord> records = loadAndParse(tsvFile);
        records.sort(Comparator.comparing(r -> r.eventTime));

        switch (slideMode) {
            case FIXED -> sqlEncodedList=processFixedWindows(records, schParse, includeSelOpConst, isTableGroupSequence);
            case PER_QUERY -> sqlEncodedList=processPerQueryWindows(records, schParse, includeSelOpConst, isTableGroupSequence);
        }
        return sqlEncodedList;
    }

    // 加载并解析TSV文件
    private List<QueryRecord> loadAndParse(Path file) throws IOException {
        List<QueryRecord> records = new ArrayList<>();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/M/d H:mm");
        HashMap<String, Integer> DictAllTables = candidateTopTables;
        try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length < 3) continue;
                System.out.println(parts[1]);
                LocalDateTime eventTime = LocalDateTime.parse(parts[1], dtf);
                long duration = Long.parseLong(parts[3]);
                String sql = parts[2];
                String table = extractTable(parts[0]);
                if (!DictAllTables.containsKey(table)) {
                    continue;
                }
                records.add(new QueryRecord(eventTime, APMFragmentIntent.getTimeMills(parts[1]), duration, sql, table));
            }
        }
        return records;
    }
    public String createBitVectorForTables(SchemaParser schParse,List<String> tables,Map<Integer,String> posTables) throws Exception
    {
        HashMap<String, Integer> MINCTables = schParse.fetchMINCTables();
        BitSet b = new BitSet(MINCTables.size());
        for (String tableName : tables) {
            try {
                int tableIndex = MINCTables.get(tableName);
                b.set(tableIndex);
                posTables.put(tableIndex,tableName);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return toString(b, MINCTables.size());
    }
    public String toString(BitSet b, int size) throws Exception
    {
        String to_return = "";
        for (int i = 0; i < size; i++) {
            if (b.get(i)) {
                to_return += "1";
            } else {
                to_return += "0";
            }
        }
        return to_return;
    }

    // 从SQL提取表名（需根据实际SQL格式调整）
    private String extractTable(String tables) {
        String table="";
        // 去掉字符串两端的方括号和单引号
        tables = tables.replace("[", "").replace("]", "").replace("'", "");

        // 按逗号分隔字符串，得到表名数组
        String[] tableNames = tables.split(",");

        // 输出提取的表名
        for (String tab : tableNames) {
//            如果表名以_cluster结尾，则去掉_cluster部分
            if (tab.endsWith("_cluster")) {
                tab = tab.substring(0, tab.length() - 8);
            }
            tab = tab.split("\\.")[1];
//            System.out.println(tab.trim()); // 使用 trim() 去掉可能的空格
            table=tab;
        }
        return table;
    }

    // 处理固定时间窗口
    private List<String> processFixedWindows(List<QueryRecord> records, SchemaParser schParse, boolean includeSelOpConst, boolean isTableGroupSequence) throws Exception {
        List<String> sqlList= new ArrayList<>();
        if (records.isEmpty()) return sqlList;

        LocalDateTime start = records.get(0).eventTime;
        LocalDateTime end = records.get(records.size()-1).eventTime;

        LocalDateTime windowStart = start;
        while (windowStart.isBefore(end)) {
            LocalDateTime windowEnd = windowStart.plus(windowSize);
            List<QueryRecord> windowData = getRecordsInWindow(records, windowStart, windowEnd);
            String encoded=processWindow(windowData, windowStart, windowEnd,schParse, includeSelOpConst);
            if(encoded!=null){
                sqlList.add(encoded);
            }
            windowStart = windowEnd;
        }
        return sqlList;
    }

    // 处理逐条滑动窗口
    private List<String> processPerQueryWindows(List<QueryRecord> records, SchemaParser schParse, boolean includeSelOpConst, boolean isTableGroupSequence) throws Exception {
        List<String> sqlList= new ArrayList<>();
        Map<String, AtomicInteger> tableAndCount = new HashMap<>();
        if(isTableGroupSequence){
            for (String table : candidateTopTables.keySet()) {
                tableAndCount.put(table, new AtomicInteger(0));
            }
        }
        int total=0;
        int seq=0;
        LocalDateTime beforeEnd=null;
        for (QueryRecord record : records) {
            LocalDateTime windowEnd = record.eventTime;
            if(beforeEnd!=null&&beforeEnd.equals(windowEnd)){
                continue;
            }
            beforeEnd=windowEnd;
            LocalDateTime windowStart = windowEnd.minus(windowSize);
            List<QueryRecord> windowData = getRecordsInWindow(records, windowStart, windowEnd);

            //根据表分组成多个窗口
            Map<String,List<QueryRecord>> tableGroup = windowData.stream().collect(Collectors.groupingBy(QueryRecord::getTable));
            //合并等价子查询
            Map<String,List<QueryRecord>> mergedQueryRecords = combineQuerysInWindow(tableGroup,schParse,includeSelOpConst);
            List<QueryRecord> windowDataMerged = new ArrayList<>();
            for (Map.Entry<String, List<QueryRecord>> entry : mergedQueryRecords.entrySet()) {
                windowDataMerged.addAll(entry.getValue());
            }
            if(isTableGroupSequence){
                for (Map.Entry<String, List<QueryRecord>> entry : mergedQueryRecords.entrySet()) {
                    if(entry.getValue().isEmpty()){
                        continue;
                    }
                    List<QueryRecord> queryWindowOfSingleTable = entry.getValue();
                    String encoded=processWindow(queryWindowOfSingleTable, windowStart, windowEnd, schParse, includeSelOpConst);
                    if(encoded!=null){
                        int count=tableAndCount.get(record.table).getAndIncrement();
                        seq = candidateTopTables.get(record.table);
                        sqlList.add("Session "+seq+", Query " + count + "; OrigQuery:" + record.sql + ";" + encoded);
                    }
                }
            }else{
                String encoded=processWindow(windowDataMerged, windowStart, windowEnd, schParse, includeSelOpConst);
                if(encoded!=null){
                    sqlList.add("Session 0, Query " + total + "; OrigQuery:" + record.sql + ";" + encoded);
                    total+=1;
                }
            }
        }
        return sqlList;
    }

    private Map<String, List<QueryRecord>> combineQuerysInWindow(Map<String, List<QueryRecord>> tableGroup, SchemaParser schParse, boolean includeSelOpConst) throws Exception {
        //todo: 合并等价子查询结构: one-hot编码后相同
        HashMap<String,QueryRecord> intentAndOriginSql=new HashMap<>();
        Map<String, List<QueryRecord>> mergedQueryRecords = new HashMap<>();
        for (Map.Entry<String, List<QueryRecord>> entry : tableGroup.entrySet()) {
            for (QueryRecord record : entry.getValue()) {
                String queryIntent = APMFragmentIntent.getQueryIntent(record.sql, record.eventTimeMills, schParse, includeSelOpConst, true);
                if(queryIntent!=null){
                    intentAndOriginSql.put(queryIntent, record);
                }
            }
        }

        for (Map.Entry<String, QueryRecord> entry : intentAndOriginSql.entrySet()) {
            mergedQueryRecords.computeIfAbsent(entry.getValue().table,k->new ArrayList<>()).add(entry.getValue());
        }
        return mergedQueryRecords;
    }

    // 获取窗口内记录
    private List<QueryRecord> getRecordsInWindow(List<QueryRecord> records,
                                                 LocalDateTime start,
                                                 LocalDateTime end) {
        return records.stream()
                .filter(r -> !r.eventTime.isBefore(start) && (r.eventTime.isEqual(end) || r.eventTime.isBefore(end)))
                .collect(Collectors.toList());
    }

    // 处理单个窗口
    private String processWindow(List<QueryRecord> windowData,
                                 LocalDateTime start,
                                 LocalDateTime end, SchemaParser schParse, boolean includeSelOpConst) throws Exception {
        if (windowData.isEmpty()) return null;
//        HashMap<String, Integer> DictAllTables;
//        if(isTableGroupSequence){
//            DictAllTables = candidateTopTables;
//        }else{
//            DictAllTables = schParse.fetchMINCTables();
//        }

        // Step 1: 按表统计总耗时
        Map<String, Long> tableDurations = windowData.stream()
//                .filter(record->DictAllTables.containsKey(record.table))
                .collect(Collectors.groupingBy(
                        r -> r.table,
                        Collectors.summingLong(r -> r.duration)
                ));
        if(tableDurations.isEmpty()){
            return null;
        }
        // Step 2: 获取TopK表
        List<String> topTables = tableDurations.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(topK)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Step 3: 收集每个表的TopN查询
        List<QueryRecord> selectedQueries = new ArrayList<>();

        for (String table : topTables) {
            List<QueryRecord> tableQueries = windowData.stream()
                    .filter(r -> r.table.equals(table))
                    .sorted(Comparator.comparingLong(r -> -r.duration))
                    .limit(topN)
                    .collect(Collectors.toList());

            selectedQueries.addAll(tableQueries);
        }

        // 或有窗口内所有tables和对应编码位置，并按table编码位置对对应的query编码进行存放
        HashMap<Integer,String> posTables=new HashMap<>();
        String windowTablesIntent=createBitVectorForTables(schParse,topTables, posTables);

        // Step 4: One-Hot编码
        String encodedQuerysInWindow = oneHotEncodeForQuerys(selectedQueries, schParse, includeSelOpConst,windowTablesIntent,posTables);
        if(encodedQuerysInWindow==null){
            return null;
        }
        // 输出结果
        System.out.printf("Window: %s - %s%n", start, end);
        System.out.println("Top Tables: " + topTables);
        System.out.println("Encoded Queries:"+encodedQuerysInWindow.length());
        System.out.println(encodedQuerysInWindow);
        System.out.println("-----------------------");
        return encodedQuerysInWindow;
    }


    /**
     * 返回窗口内的表名列表编码+查询列表编码(忽略表名)
     * @param queries
     * @param schParse
     * @param includeSelOpConst
     * @param windowTablesIntent
     * @param posTables
     * @return
     * @throws Exception
     */
    private String oneHotEncodeForQuerys(List<QueryRecord> queries, SchemaParser schParse, boolean includeSelOpConst, String windowTablesIntent, HashMap<Integer, String> posTables) throws Exception {
        HashMap<String, String> tableAndQueryIntent = new HashMap<>();
        StringBuilder encoded = new StringBuilder();
        encoded.append(windowTablesIntent);
        int queryIntentLen = 0;
        for (QueryRecord queryRecord : queries) {
            String queryIntent = APMFragmentIntent.getQueryIntent(queryRecord.sql, queryRecord.eventTimeMills, schParse, includeSelOpConst, true);
            if (queryIntent == null) continue;
            queryIntentLen = queryIntent.length();
            tableAndQueryIntent.put(queryRecord.table, queryIntent);
        }
        if(tableAndQueryIntent.isEmpty()){
            return null;
        }

        for (int i = 0; i < windowTablesIntent.length(); i++) {
            char c = windowTablesIntent.charAt(i);
            if (c == '1') {
                String queryIntent = tableAndQueryIntent.get(posTables.get(i));
                encoded.append(queryIntent);
            } else {
                //添加queryIntentLen长度个0
                for (int j = 0; j < queryIntentLen; j++) {
                    encoded.append("0");
                }
            }
        }
        return encoded.toString();
    }

    private static final HashMap<String,Integer> candidateTopTables = Maps.newHashMap();

    // 使用示例
    public static void main(String[] args) throws Exception {
        String configFile = "input/ApmJavaConfig.txt";
//        String tsvFilePath = "input/testQuerys.tsv"; // TSV 文件路径
        String tsvFilePath = "input/0318_ApmQuerys.tsv";
        Duration window_size = Duration.ofMinutes(5);
        boolean isTableGroupSequence = true;//是否按表分组序列
        candidateTopTables.put("dwm_request",0);
        candidateTopTables.put("dwm_exception",1);
        candidateTopTables.put("dwm_user",2);

        int topTabN = 3;
        int topQueryN = 2;
        String outputDir="output/0319";
        String outputFile = "querys_"+isTableGroupSequence+"_"+window_size.getSeconds()+"_"+topTabN+topQueryN+".txt"; // 输出文件路径
        SchemaParser schParse = new SchemaParser();
        schParse.fetchSchema(configFile);
        HashMap<String, String> configDict = schParse.getConfigDict();
        boolean includeSelOpConst = Boolean.parseBoolean(configDict.get("MINC_SEL_OP_CONST"));

        APMWindowFragmentIntent analyzer = new APMWindowFragmentIntent(
                window_size,  // 窗口大小
                topTabN,                      // TopK表
                topQueryN,                      // 每个表的TopN查询
                SlideMode.PER_QUERY         // 滑动模式
        );
        List<String> sqlList = analyzer.process(Path.of(tsvFilePath),schParse,includeSelOpConst,isTableGroupSequence);
        System.out.println("Saved size:"+sqlList.size());
        Util.writeSQLListToFile(sqlList, outputDir,outputFile);
    }
}