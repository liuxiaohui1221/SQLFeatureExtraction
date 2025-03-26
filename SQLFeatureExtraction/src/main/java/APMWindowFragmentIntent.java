import com.clickhouse.SchemaParser;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import tools.IOUtil;
import toolsForMetrics.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class APMWindowFragmentIntent {

    private final int seed=999;
    // 配置参数
    private final Duration windowSize;
    private final int topK;
    private final int topN;
    private final SlideMode slideMode;
    private int encoded_length=0;

    public enum SlideMode {
        FIXED, SLIDING
    }

    public APMWindowFragmentIntent(Duration windowSize, int topK, int topN, SlideMode slideMode) {
        this.windowSize = windowSize;
        this.topK = topK;
        this.topN = topN;
        this.slideMode = slideMode;
    }

    // 查询记录结构
    static class QueryRecord {
        private final long eventTimeSec;
        LocalDateTime eventTime;
        long duration;
        String sql;
        String table;
        long cost;

        public QueryRecord(LocalDateTime eventTime,long eventTimeSec, long duration, String sql, String table) {
            this.eventTime = eventTime;
            this.eventTimeSec = eventTimeSec;
            this.duration = duration;
            this.sql = sql;
            this.table = table;
            this.cost=duration;
        }

        public void sumDuration(long duration) {
            this.cost += duration;
        }

        public String getTable() {
            return this.table;
        }

        public String getSql() {
            return this.sql;
        }
    }

    // 主处理流程
    public List<String> process(Path tsvFile, SchemaParser schParse, boolean includeSelOpConst, boolean isTableGroupSequence,String combineMethod) throws Exception {
        List<String> sqlEncodedList=new ArrayList<>();
        List<QueryRecord> records = loadAndParse(tsvFile);
        records.sort(Comparator.comparing(r -> r.eventTime));

        switch (slideMode) {
            case FIXED -> sqlEncodedList=processFixedWindows(records, schParse, includeSelOpConst, isTableGroupSequence,combineMethod);
            case SLIDING -> sqlEncodedList=processPerQueryWindows(records, schParse, includeSelOpConst, isTableGroupSequence,combineMethod);
        }
        return sqlEncodedList;
    }

    // 加载并解析TSV文件
    private List<QueryRecord> loadAndParse(Path file) throws IOException {
        List<QueryRecord> records = new ArrayList<>();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/M/d H:mm:ss.SSS");
        HashMap<String, Integer> DictAllTables = candidateTopTables;
        try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
            String line;
            br.readLine();
            Random random=new Random(seed);
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length < 3) continue;
                int seconds=random.nextInt(60);
                int micseconds=random.nextInt(1000);
                String secStr=seconds+"";
                if(seconds<10){
                    secStr="0"+seconds;
                }
                String micsecStr=micseconds+"";
                if(micseconds<10){
                    micsecStr="00"+micseconds;
                }else if(micseconds<100){
                    micsecStr="0"+micseconds;
                }
                LocalDateTime eventTime = LocalDateTime.parse(parts[1]+":"+secStr+"."+micsecStr, dtf);
                long duration = Long.parseLong(parts[3]);
                String sql = parts[2];
                String table = extractTable(parts[0]);
                if (!DictAllTables.containsKey(table)) {
                    continue;
                }
                records.add(new QueryRecord(eventTime, APMFragmentIntent.getTimeSec(parts[1]), duration, sql, table));
            }
        }
        return records;
    }
    public String createBitVectorForTables(SchemaParser schParse,List<String> tables,Map<Integer,String> posTables) throws Exception
    {
        HashMap<String, Integer> MINCTables = candidateTopTables;
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

    private List<String> encodedWindowQueryRecords(List<QueryRecord> windowData, LocalDateTime windowStart,
                                                   LocalDateTime windowEnd, SchemaParser schParse, boolean includeSelOpConst,
                                                   boolean isTableGroupSequence, String combineMethod, AtomicInteger usedEmptyWindows) throws Exception {
        //根据表分组成多个窗口
        Map<String,List<QueryRecord>> tableGroup = windowData.stream().collect(Collectors.groupingBy(QueryRecord::getTable));
        //合并等价子查询：计算合并后查询的总耗时+查询频率=cost
        Map<String,List<QueryRecord>> mergedQueryRecords = combineQuerysInWindow(tableGroup,schParse,includeSelOpConst);
        List<QueryRecord> windowDataMerged = new ArrayList<>();
        for (Map.Entry<String, List<QueryRecord>> entry : mergedQueryRecords.entrySet()) {
            windowDataMerged.addAll(entry.getValue());
        }

        List<String> windowSqlList= new ArrayList<>();

        if(isTableGroupSequence){
            if(mergedQueryRecords.isEmpty()){
                //非凌晨时段的窗口0-9点空窗口
                if(windowStart.getHour()>9){
                    for(Map.Entry<String, Integer> entry : candidateTopTables.entrySet()){
                        QueryRecord emptyRecord = getEmptyWindowEncoded(windowStart, entry.getKey());
                        String emptyWindowEncoded=processWindow(Arrays.asList(emptyRecord), windowStart, windowEnd, schParse, includeSelOpConst,combineMethod);
                        if(emptyWindowEncoded!=null){
                            usedEmptyWindows.incrementAndGet();
                            int seqId = entry.getValue();
                            windowSqlList.add("Session "+seqId+", Query " + windowStart.toEpochSecond(ZoneOffset.UTC) + "; OrigQuery:" + "-" + ";" + emptyWindowEncoded);
                        }
                    }
                }
            }else{
                for (Map.Entry<String, List<QueryRecord>> entry : mergedQueryRecords.entrySet()) {
                    if(entry.getValue().isEmpty()){
                        continue;
                    }
                    String table=entry.getKey();
                    List<QueryRecord> queryWindowOfSingleTable = entry.getValue();
                    String encoded=processWindow(queryWindowOfSingleTable, windowStart, windowEnd, schParse, includeSelOpConst,combineMethod);
                    if(encoded!=null){
                        int seqId = candidateTopTables.get(table);
                        windowSqlList.add("Session "+seqId+", Query " + windowStart.toEpochSecond(ZoneOffset.UTC) + "; OrigQuery:" + concatSqls(queryWindowOfSingleTable) + ";" + encoded);
                    }
                }
            }
        }else{
            String encoded=processWindow(windowDataMerged, windowStart, windowEnd, schParse, includeSelOpConst,combineMethod);
            if(encoded!=null){
                windowSqlList.add("Session 0, Query " + windowStart.toEpochSecond(ZoneOffset.UTC) + "; OrigQuery:" + windowDataMerged.get(0).sql + ";" + encoded);
            }else{
                //非凌晨时段的窗口0-9点空窗口
                if(windowStart.getHour()>9){
                    usedEmptyWindows.incrementAndGet();
                    QueryRecord emptyRecord= getEmptyWindowEncoded(windowStart,"dwm_request");
                    String emptyEncoded=processWindow(Arrays.asList(emptyRecord), windowStart, windowEnd, schParse, includeSelOpConst,combineMethod);
                    windowSqlList.add("Session 0, Query " + windowStart.toEpochSecond(ZoneOffset.UTC) + "; OrigQuery:" + "-" + ";" + emptyEncoded);
                }
            }
        }
        return windowSqlList;
    }

    private String concatSqls(List<QueryRecord> queryWindowOfSingleTable) {
        return queryWindowOfSingleTable.stream().map(QueryRecord::getSql).collect(Collectors.joining("->"));
    }

    private QueryRecord getEmptyWindowEncoded(LocalDateTime time,String table) {
        return new QueryRecord(time,time.toEpochSecond(ZoneOffset.UTC),0,"select count() from "+table,table);
    }

    // 处理固定时间窗口
    private List<String> processFixedWindows(List<QueryRecord> records, SchemaParser schParse, boolean includeSelOpConst,
                                             boolean isTableGroupSequence,String combineMethod) throws Exception {
        List<String> sqlList= new ArrayList<>();
        if (records.isEmpty()) return sqlList;

        LocalDateTime start = records.get(0).eventTime;
        LocalDateTime end = records.get(records.size()-1).eventTime;

        LocalDateTime windowStart = start;
        int totalWindows=0;
        int emptyWindows=0;
        AtomicInteger usedEmptyWindows=new AtomicInteger(0);
        List<String> windowMetric=new ArrayList<>();
        while (windowStart.isBefore(end)) {
            LocalDateTime windowEnd = windowStart.plus(windowSize);
            List<QueryRecord> windowData = getRecordsInWindow(records, windowStart, windowEnd);
            if(windowStart.getHour()>=0 && windowStart.getHour()<=9){
//                System.out.println("skip window:"+windowStart);
                windowStart = windowEnd;
                continue;
            }
            List<String> querysEncoded = encodedWindowQueryRecords(windowData, windowStart, windowEnd, schParse, includeSelOpConst, isTableGroupSequence,combineMethod,usedEmptyWindows);
            if(querysEncoded.isEmpty()){
                //空窗口时段
                emptyWindows++;
                windowMetric.add(windowStart+","+windowEnd.toString()+",0");

            }else{
                //扩充数据：在有查询的窗口处滑动获得多个重叠窗口
                sqlList.addAll(querysEncoded);
                totalWindows+=querysEncoded.size();
                long deltaTime= windowSize.getSeconds()/expasionFactor;
                for(int i=1;i<expasionFactor;i++){
                    LocalDateTime newWindowEnd=windowStart.plusSeconds(deltaTime*i);
                    LocalDateTime newWindowStart=newWindowEnd.minus(windowSize);
                    List<QueryRecord> newWindowData = getRecordsInWindow(records, newWindowStart, newWindowEnd);
                    List<String> newQuerysEncoded = encodedWindowQueryRecords(newWindowData, newWindowStart, newWindowEnd, schParse, includeSelOpConst, isTableGroupSequence,combineMethod, usedEmptyWindows);
                    if(newQuerysEncoded.isEmpty()){
                        //空窗口时段
                        continue;
                    }
                    sqlList.addAll(newQuerysEncoded);
                    totalWindows+=newQuerysEncoded.size();
                }
                windowMetric.add(windowStart+","+windowEnd.toString()+",1");
            }
            windowStart = windowEnd;
        }
        //windowMetric保存到文件中
        IOUtil.writeToFile(windowMetric, "windowMetric.csv");

        System.out.println("Total used windows: "+totalWindows+",used empty windows: "+usedEmptyWindows.get()+"skip emptyWindows:"+emptyWindows+", empty rate: "+(double)usedEmptyWindows.get()/(totalWindows));
        return sqlList;
    }

    // 处理逐条滑动窗口
    private List<String> processPerQueryWindows(List<QueryRecord> records, SchemaParser schParse,
                                                boolean includeSelOpConst, boolean isTableGroupSequence,String combineMethod) throws Exception {
        List<String> sqlList= new ArrayList<>();
        AtomicInteger usedEmptyWindows=new AtomicInteger(0);
        Map<String, AtomicInteger> tableAndCount = new HashMap<>();
        if(isTableGroupSequence){
            for (String table : candidateTopTables.keySet()) {
                tableAndCount.put(table, new AtomicInteger(0));
            }
        }
        int totalWindows=0;
        int emptyWindows=0;
        LocalDateTime beforeEnd=null;
        for (QueryRecord record : records) {
            LocalDateTime windowEnd = record.eventTime;
            if(beforeEnd!=null&&beforeEnd.toString().equals(windowEnd.toString())){
                continue;
            }
            beforeEnd=windowEnd;
            LocalDateTime windowStart = windowEnd.minus(windowSize);

            if(windowStart.getHour()>=0 && windowStart.getHour()<=9){
                continue;
            }
            List<QueryRecord> windowData = getRecordsInWindow(records, windowStart, windowEnd);

            List<String> querysEncoded = encodedWindowQueryRecords(windowData, windowStart, windowEnd, schParse, includeSelOpConst, isTableGroupSequence,combineMethod, usedEmptyWindows);
            if(querysEncoded.isEmpty()){
                //空窗口时段
                emptyWindows++;
                continue;
            }
            sqlList.addAll(querysEncoded);
            totalWindows++;
        }
        System.out.println("Total windows: "+totalWindows+", used empty windows: "+usedEmptyWindows.get()+", empty rate: "+(double)usedEmptyWindows.get()/(totalWindows));

        return sqlList;
    }

    private Map<String, List<QueryRecord>> combineQuerysInWindow(Map<String, List<QueryRecord>> tableGroup, SchemaParser schParse, boolean includeSelOpConst) throws Exception {
        //合并等价子查询结构: one-hot编码后相同,cost=sum duration
        HashMap<String,QueryRecord> intentAndOriginSql=new HashMap<>();
        Map<String, List<QueryRecord>> mergedQueryRecords = new HashMap<>();
        for (Map.Entry<String, List<QueryRecord>> entry : tableGroup.entrySet()) {
            for (QueryRecord record : entry.getValue()) {
                String queryIntent = APMFragmentIntent.getQueryIntent(record.sql, record.eventTime,record.eventTimeSec, schParse, includeSelOpConst, true,false);
                if(queryIntent!=null){
                    if(intentAndOriginSql.containsKey(queryIntent)){
                        QueryRecord originRecord = intentAndOriginSql.get(queryIntent);
                        originRecord.sumDuration(record.duration);
                    }else{
                        intentAndOriginSql.put(queryIntent, record);
                    }
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
    private String processWindow(List<QueryRecord> windowTemplateQuerys,
                                 LocalDateTime start,
                                 LocalDateTime end, SchemaParser schParse, boolean includeSelOpConst,String combineMethod) throws Exception {
        // Step 1: 按表统计总查询模板成本
        Map<String, Long> tableDurations = windowTemplateQuerys.stream()
//                .filter(record->DictAllTables.containsKey(record.table))
                .collect(Collectors.groupingBy(
                        r -> r.table,
                        Collectors.summingLong(r -> r.cost)
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

        // Step 3: 选择topN个查询成本最高的查询模板
        List<QueryRecord> selectedQueries = new ArrayList<>();
        for (String table : topTables) {
            List<QueryRecord> tableQueries = windowTemplateQuerys.stream()
                    .filter(r -> r.table.equals(table))
                    .sorted(Comparator.comparingLong(r -> -r.cost))
                    .limit(topN)
                    .collect(Collectors.toList());

            selectedQueries.addAll(tableQueries);
        }

        // 窗口内所有tables和对应编码位置，并按table编码位置对对应的query编码进行存放
        HashMap<Integer,String> posTables=new HashMap<>();
        String windowTablesIntent=createBitVectorForTables(schParse,topTables, posTables);

        // Step 4: One-Hot编码
        String encodedQuerysInWindow = oneHotEncodeForQuerys(selectedQueries, schParse, includeSelOpConst,windowTablesIntent,posTables,combineMethod);
        if(encodedQuerysInWindow==null){
            //默认这个窗口内没有对应表的查询
            return null;
        }
        encoded_length=encodedQuerysInWindow.length();
        // 输出结果
        System.out.printf("Window: %s - %s%n", start, end);
        System.out.println("Top Tables: " + topTables);
        System.out.println("Encoded Windows queries length:" + encodedQuerysInWindow.length());
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
    private String oneHotEncodeForQuerys(List<QueryRecord> queries, SchemaParser schParse, boolean includeSelOpConst,
                                         String windowTablesIntent, HashMap<Integer, String> posTables,String combineMethod) throws Exception {
        HashMap<String, String> tableAndQueryIntents = new HashMap<>();
        StringBuilder encoded = new StringBuilder();
        encoded.append(windowTablesIntent);
        int queryIntentLen = 0;
        for (QueryRecord queryRecord : queries) {
            String queryIntent = APMFragmentIntent.getQueryIntent(queryRecord.sql, queryRecord.eventTime, queryRecord.eventTimeSec, schParse, includeSelOpConst, true,false);
            if (queryIntent == null) continue;
            queryIntentLen = queryIntent.length();
            log.info("queryIntent len:{}", queryIntent.length());
            if (combineMethod.equals(COMBINE_METHOD_MERGE)) {
                //todo 合并sql编码
                log.error("combineMethod merge not implemented yet!!!");
            } else if (combineMethod.equals(COMBINE_METHOD_CONCAT)) {
                //拼接方式合并sql编码
                String mergedQueryIntents=queryIntent;
                if(tableAndQueryIntents.containsKey(queryRecord.table)){
                    String oldQueryIntent = tableAndQueryIntents.get(queryRecord.table);
                    mergedQueryIntents = oldQueryIntent + mergedQueryIntents;
                }
                tableAndQueryIntents.put(queryRecord.table, mergedQueryIntents);
            }else {
                throw new RuntimeException("combineMethod must be concat or merge");
            }

        }
        if(tableAndQueryIntents.isEmpty()){
            return null;
        }
        log.info("windowTablesIntent len:{}", windowTablesIntent.length());
        for (int i = 0; i < windowTablesIntent.length(); i++) {
            char c = windowTablesIntent.charAt(i);
            if (c == '1') {
                String queryIntents = tableAndQueryIntents.get(posTables.get(i));
                encoded.append(queryIntents);
                if(queryIntents.length()<queryIntentLen*topQueryN){
                    //补充长度
                    for (int j = 0; j < queryIntentLen*topQueryN-queryIntents.length(); j++) {
                        encoded.append("0");
                    }
                }
            } else {
                //添加queryIntentLen长度个0
                for (int j = 0; j < queryIntentLen*topQueryN; j++) {
                    encoded.append("0");
                }
            }
        }
        log.info("windowTablesIntent[{}]+windowTablesIntent[{}]*topQueryN[{}]*queryIntent[{}]={} = encoded len:{}",
                 windowTablesIntent.length(), windowTablesIntent.length(), topQueryN, queryIntentLen,
                 windowTablesIntent.length() + windowTablesIntent.length() * topQueryN * queryIntentLen,
                 encoded.length()
        );
        return encoded.toString();
    }

    private static final HashMap<String,Integer> candidateTopTables = Maps.newHashMap();
    public static final String COMBINE_METHOD_CONCAT="concat";
    public static final String COMBINE_METHOD_MERGE="merge";
    public static final int topTabN = 1;//1-candidateTopTables
    public static final int topQueryN = 2;
    public static final int expasionFactor=5;
    // 使用示例
    public static void main(String[] args) throws Exception {
        String configFile = "input/table/" + topTabN + "/ApmJavaConfig.txt";
//        String tsvFilePath = "input/testQuerys.tsv"; // TSV 文件路径
        String tsvFilePath = "input/0318_ApmQuerys.tsv";
        String outputDir = "table";
        Duration window_size = Duration.ofMinutes(5);
        boolean isTableGroupSequence = true;//是否按表分组序列
        if (topTabN == 1) {
            candidateTopTables.put("dwm_request", 0);
        } else if (topTabN == 3) {
            candidateTopTables.put("dwm_request", 0);
            candidateTopTables.put("dwm_exception", 1);
            candidateTopTables.put("dwm_user", 2);
        }


        String combineMethod = COMBINE_METHOD_CONCAT;//concat or merge


        String outputFile =
            "window_querys_"
            + window_size.getSeconds()
            + "s_"
            + topTabN
            + "tab_"
            + topQueryN
            + "q.txt"; // 输出文件路径
        SchemaParser schParse = new SchemaParser();
        schParse.fetchSchema(configFile);
        IOUtil.writeFile(candidateTopTables, outputDir, "ApmWindowTables.txt");
        HashMap<String, String> configDict = schParse.getConfigDict();
        boolean includeSelOpConst = Boolean.parseBoolean(configDict.get("MINC_SEL_OP_CONST"));

        APMWindowFragmentIntent analyzer = new APMWindowFragmentIntent(
                window_size,  // 窗口大小
                topTabN,                      // TopK表
                topQueryN,                      // 每个表的TopN查询
                SlideMode.SLIDING         // 滑动模式 FIXED or SLIDING
        );
        long startT = System.currentTimeMillis();
        List<String> sqlList = analyzer.process(Path.of(tsvFilePath),schParse,includeSelOpConst,isTableGroupSequence,combineMethod);
        System.out.println("sqlList size:"
                           + sqlList.size()
                           + ",cost:"
                           + (System.currentTimeMillis() - startT) / 1000
                           + "s");

        //按固定窗口重新组织查询session
        List<String> newsqlList = reorganizeSqlList(sqlList, window_size.getSeconds());
        Util.writeSQLListToFile(newsqlList, outputDir, outputFile);
        System.out.println("Saved new sqlList size:" + newsqlList.size());

    }

    public static List<String> reorganizeSqlList(List<String> sqlList, long winSeconds)
    {
//        "Session 0, Query " + windowStart.toEpochSecond(ZoneOffset.UTC) + "; OrigQuery:"

        ArrayList<String> windowTablesIntent = new ArrayList<>(sqlList);

        List<String> newSqlList = new ArrayList<>();
        HashMap<Integer, List<String>> sessSqls = Maps.newHashMap();
        for (int i = 0; i < windowTablesIntent.size(); i++) {
            List<String> curSeesionSqls = new ArrayList<>();
            sessSqls.put(i, curSeesionSqls);

            //add cursql
            //new sessId,queryId
            String sessId = "Session " + i;
            String queryId = ", Query " + curSeesionSqls.size();
            String truncateSqlElement = windowTablesIntent.get(i).split("; OrigQuery:")[1];
            curSeesionSqls.add(sessId + queryId + "; OrigQuery:" + truncateSqlElement);

            long firstWindowStartEventTime = Long.parseLong(windowTablesIntent.get(i)
                                                                              .split(";")[0].split("Query")[1].trim());
            long firstWindowEndEventTime = firstWindowStartEventTime + winSeconds;
            int endIndex = findFirstEndIndex(windowTablesIntent, firstWindowEndEventTime);

            //add nextsql
            long nextWindowEndEventTime = firstWindowEndEventTime;
            while (endIndex > -1) {
                //add nextsql
                queryId = ", Query " + curSeesionSqls.size();
                truncateSqlElement = windowTablesIntent.get(endIndex).split("; OrigQuery:")[1];
                curSeesionSqls.add(sessId + queryId + "; OrigQuery:" + truncateSqlElement);

//                nextWindowEndEventTime += winSeconds;
                nextWindowEndEventTime = Math.max(
                    nextWindowEndEventTime + winSeconds,
                    Long.parseLong(windowTablesIntent.get(endIndex).split(";")[0].split("Query")[1].trim())
                );
                endIndex = findFirstEndIndex(windowTablesIntent, nextWindowEndEventTime);
            }
        }
        //sessSqls中values存放到newSqlList中
        for (List<String> sessSql : sessSqls.values()) {
            newSqlList.addAll(sessSql);
        }
        log.info("Total sessions:{},new sqlList:{}", sessSqls.size(), newSqlList.size());
        return newSqlList;
    }

    private static int findFirstEndIndex(List<String> sqlList, long firstWindowEndEventTime)
    {
        for (int i = 0; i < sqlList.size(); i++) {
            long eventTime = Long.parseLong(sqlList.get(i).split(";")[0].split("Query")[1].trim());
            if (eventTime > firstWindowEndEventTime) {
                return i;
            }
        }
        return -1;
    }
}