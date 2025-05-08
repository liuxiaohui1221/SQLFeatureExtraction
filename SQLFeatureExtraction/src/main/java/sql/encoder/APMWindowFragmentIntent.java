package sql.encoder;

import sql.clickhouse.SchemaParser;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import sql.pojo.QueryRecord;
import sql.tools.IOUtil;
import sql.toolsForMetrics.Util;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static sql.tools.IOUtil.loadAndParse;

@Slf4j
public class APMWindowFragmentIntent {


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

    // 主处理流程
    public List<String> process(Path tsvFile, SchemaParser schParse, boolean includeSelOpConst, boolean isTableGroupSequence,String combineMethod) throws Exception {
        List<String> sqlEncodedList=new ArrayList<>();
        List<QueryRecord> records = loadAndParse(tsvFile, candidateTopTables);

        switch (slideMode) {
            case FIXED -> sqlEncodedList=processFixedWindows(records, schParse, includeSelOpConst, isTableGroupSequence,combineMethod);
            case SLIDING -> sqlEncodedList=processPerQueryWindows(records, schParse, includeSelOpConst, isTableGroupSequence,combineMethod);
        }
        return sqlEncodedList;
    }

    // 加载并解析TSV文件

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
                        String emptyWindowEncoded = processWindow(Arrays.asList(emptyRecord), windowStart, windowEnd,
                                                                  schParse, includeSelOpConst, combineMethod,
                                                                  mergedQueryRecords.isEmpty()
                        );
                        if(emptyWindowEncoded!=null){
                            usedEmptyWindows.incrementAndGet();
                            int seqId = entry.getValue();
                            long queryId = windowStart.toEpochSecond(ZoneOffset.UTC);
                            addSqlWindowEncoded(windowSqlList, seqId, queryId, "-", emptyWindowEncoded);
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
                    String encoded = processWindow(
                        queryWindowOfSingleTable,
                        windowStart,
                        windowEnd,
                        schParse,
                        includeSelOpConst,
                        combineMethod,
                        mergedQueryRecords.isEmpty()
                    );
                    if(encoded!=null){
                        int seqId = candidateTopTables.get(table);
                        addSqlWindowEncoded(
                            windowSqlList,
                            seqId,
                            windowStart.toEpochSecond(ZoneOffset.UTC),
                            concatSqls(queryWindowOfSingleTable),
                            encoded
                        );
                    }
                }
            }
        }else{
            String encoded = processWindow(
                windowDataMerged,
                windowStart,
                windowEnd,
                schParse,
                includeSelOpConst,
                combineMethod,
                mergedQueryRecords.isEmpty()
            );
            if(encoded!=null){
                addSqlWindowEncoded(windowSqlList, 0, windowStart.toEpochSecond(ZoneOffset.UTC),
                                    concatSqls(windowDataMerged), encoded
                );
            }else{
                //非凌晨时段的窗口0-9点空窗口
                if(windowStart.getHour()>9){
                    usedEmptyWindows.incrementAndGet();
                    QueryRecord emptyRecord= getEmptyWindowEncoded(windowStart,"dwm_request");
                    String emptyEncoded = processWindow(
                        Arrays.asList(emptyRecord),
                        windowStart,
                        windowEnd,
                        schParse,
                        includeSelOpConst,
                        combineMethod,
                        mergedQueryRecords.isEmpty()
                    );
                    addSqlWindowEncoded(windowSqlList, 0, windowStart.toEpochSecond(ZoneOffset.UTC), "-", emptyEncoded);
                }
            }
        }
        return windowSqlList;
    }

    private void addSqlWindowEncoded(
        List<String> windowSqlList, int seqId, long queryId,
        String origSqls, String windowSqlEncoded
    )
    {
        String sqlIntent = "Session "
                           + seqId
                           + ", Query "
                           + queryId
                           + "; OrigQuery:"
                           + origSqls
                           + ";"
                           + windowSqlEncoded;
        windowSqlList.add(sqlIntent);
        List<String> sqlIds = sessIdAndSqlIntentIndexes.computeIfAbsent(seqId, k -> new ArrayList<>());
        sqlIds.add(sqlIntent);
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

        LocalDateTime start = records.get(0).getEventTime();
        LocalDateTime end = records.get(records.size() - 1).getEventTime();

        LocalDateTime windowStart = start;
        int totalWindows=0;
        int emptyWindows=0;
        AtomicInteger usedEmptyWindows=new AtomicInteger(0);
        List<String> windowMetric=new ArrayList<>();
        while (windowStart.isBefore(end)) {
            LocalDateTime windowEnd = windowStart.plus(windowSize);
            List<QueryRecord> windowData = getRecordsInWindow(records, windowStart, windowEnd);
            if(windowStart.getHour()>=0 && windowStart.getHour()<=9){
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
            //固定滑动步长
            windowStart.plus(fixedSlidingSize);
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
            LocalDateTime windowEnd = record.getEventTime();
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
                String queryIntent = APMFragmentIntent.getQueryIntent(
                    record.getSql(),
                    record.getEventTime(),
                    record.getEventTimeSec(),
                    schParse,
                    includeSelOpConst,
                    true,
                    false
                );
                if(queryIntent!=null){
                    if(intentAndOriginSql.containsKey(queryIntent)){
                        QueryRecord originRecord = intentAndOriginSql.get(queryIntent);
                        originRecord.sumDuration(record.getDuration());
                    }else{
                        intentAndOriginSql.put(queryIntent, record);
                    }
                }
            }
        }

        for (Map.Entry<String, QueryRecord> entry : intentAndOriginSql.entrySet()) {
            mergedQueryRecords.computeIfAbsent(entry.getValue().getTable(), k -> new ArrayList<>())
                              .add(entry.getValue());
        }
        return mergedQueryRecords;
    }

    // 获取窗口内记录
    private List<QueryRecord> getRecordsInWindow(List<QueryRecord> records,
                                                 LocalDateTime start,
                                                 LocalDateTime end) {
        return records.stream()
                      .filter(r -> r.getEventTime().isAfter(start) && (r.getEventTime().isEqual(end)
                                                                       || r.getEventTime().isBefore(end)))
                .collect(Collectors.toList());
    }

    // 处理单个窗口
    private String processWindow(List<QueryRecord> windowTemplateQuerys,
                                 LocalDateTime start,
                                 LocalDateTime end,
                                 SchemaParser schParse,
                                 boolean includeSelOpConst,
                                 String combineMethod,
                                 boolean isEmpty
    ) throws Exception
    {
        // Step 1: 按表统计总查询模板成本
        Map<String, Long> tableDurations = windowTemplateQuerys.stream()
//                .filter(record->DictAllTables.containsKey(record.table))
                .collect(Collectors.groupingBy(
                    r -> r.getTable(),
                    Collectors.summingLong(r -> r.getCost())
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
                                                                 .filter(r -> r.getTable().equals(table))
                                                                 .sorted(Comparator.comparingLong(r -> -r.getCost()))
                    .limit(topN)
                    .collect(Collectors.toList());

            selectedQueries.addAll(tableQueries);
        }

        // 窗口内所有tables和对应编码位置，并按table编码位置对对应的query编码进行存放
        HashMap<Integer,String> posTables=new HashMap<>();
        String windowTablesIntent=createBitVectorForTables(schParse,topTables, posTables);

        // Step 4: One-Hot编码
        String encodedQuerysInWindow = oneHotEncodeForQuerys(selectedQueries, schParse, includeSelOpConst,
                                                             windowTablesIntent, posTables, combineMethod, isEmpty
        );
        if(encodedQuerysInWindow==null){
            //默认这个窗口内没有对应表的查询
            return null;
        }
        encoded_length=encodedQuerysInWindow.length();
        // 输出结果
        System.out.printf("Window: %s - %s%n", start, end);
//        System.out.println("Top Tables: " + topTables);
        System.out.println("Encoded Windows queries length:" + encodedQuerysInWindow.length());
//        System.out.println(encodedQuerysInWindow);
        System.out.println("-----------------------");
        return encodedQuerysInWindow;
    }

    /**
     * 返回窗口内的表名列表编码+查询列表编码(忽略表名)
     *
     * @param queries
     * @param schParse
     * @param includeSelOpConst
     * @param windowTablesIntent
     * @param posTables
     * @param isEmpty
     * @return
     * @throws Exception
     */
    private String oneHotEncodeForQuerys(List<QueryRecord> queries, SchemaParser schParse, boolean includeSelOpConst,
                                         String windowTablesIntent,
                                         HashMap<Integer, String> posTables,
                                         String combineMethod,
                                         boolean isEmpty
    ) throws Exception
    {
        HashMap<String, String> tableAndQueryIntents = new HashMap<>();
        StringBuilder encoded = new StringBuilder();
        encoded.append(windowTablesIntent);
        int queryIntentLen = 0;
        for (QueryRecord queryRecord : queries) {
            String queryIntent = APMFragmentIntent.getQueryIntent(
                queryRecord.getSql(),
                queryRecord.getEventTime(),
                queryRecord.getEventTimeSec(),
                schParse,
                includeSelOpConst,
                true,
                false
            );
            if (queryIntent == null) continue;
            if (isEmpty) {//empty window
                queryIntent = queryIntent.replace('1', '0');
            }
            queryIntentLen = queryIntent.length();
            log.info("queryIntent len:{}", queryIntent.length());
            if (combineMethod.equals(COMBINE_METHOD_MERGE)) {
                //todo 合并sql编码
                log.error("combineMethod merge not implemented yet!!!");
            } else if (combineMethod.equals(COMBINE_METHOD_CONCAT)) {
                //拼接方式合并sql编码
                String mergedQueryIntents=queryIntent;
                if (tableAndQueryIntents.containsKey(queryRecord.getTable())) {
                    String oldQueryIntent = tableAndQueryIntents.get(queryRecord.getTable());
                    mergedQueryIntents = oldQueryIntent + mergedQueryIntents;
                }
                tableAndQueryIntents.put(queryRecord.getTable(), mergedQueryIntents);
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
    private static final HashMap<Integer, String> candidateTopTablesInverse = Maps.newHashMap();
    //按顺序保存每个session的sql序列集，用于划分训练集，验证集和测试集。
    private static final HashMap<Integer, List<String>> sessIdAndSqlIntentIndexes = Maps.newHashMap();
    public static final String BIT_FRAGMENT_SEQ_SPLIT_NAME_FORMAT = "Apm_5minute_1tabQueryLog_SPLIT_OUT_";
    public static final int SPLIT_BATCH_SIZE = 1000;
    public static final String COMBINE_METHOD_CONCAT="concat";
    public static final String COMBINE_METHOD_MERGE="merge";
    public static final int topTabN = 1;//1-candidateTopTables
    public static final int topQueryN = 2;
    public static final int expasionFactor = 1;
    public static final float split_train_ratio = 0.8f;
    public static final SlideMode windowMode = SlideMode.FIXED; //窗口滑动模式 FIXED or SLIDING
    public static final Duration window_size = Duration.ofMinutes(5); //窗口大小，单位分钟
    public static final Duration fixedSlidingSize = Duration.ofMinutes(1);
    public static final String outputDir = "table";
    // 使用示例
    public static void main(String[] args) throws Exception {
        String configFile = "input/table/" + topTabN + "/ApmJavaConfig.txt";
//        String tsvFilePath = "input/testQuerys.tsv"; // TSV 文件路径
        String tsvFilePath = "input/0318_ApmQuerys.tsv";


        boolean isTableGroupSequence = true;//是否按表分组序列
        if (topTabN == 1) {
            candidateTopTables.put("dwm_request", 0);
            candidateTopTablesInverse.put(0, "dwm_request");
        } else if (topTabN == 3) {
            candidateTopTables.put("dwm_request", 0);
            candidateTopTables.put("dwm_exception", 1);
            candidateTopTables.put("dwm_user", 2);
            candidateTopTablesInverse.put(0, "dwm_request");
            candidateTopTablesInverse.put(1, "dwm_exception");
            candidateTopTablesInverse.put(2, "dwm_user");
        }
        String combineMethod = COMBINE_METHOD_CONCAT;//concat or merge
        String outputFile =
            "window_querys_"
            + window_size.getSeconds() / 60
            + "m_"
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
                windowMode         // 滑动模式 FIXED or SLIDING
        );
        long startT = System.currentTimeMillis();
        List<String> sessionSqlList = analyzer.process(Path.of(tsvFilePath), schParse, includeSelOpConst,
                                                       isTableGroupSequence, combineMethod
        );
        System.out.println("sessionSqlList size:"
                           + sessionSqlList.size()
                           + ",cost:"
                           + (System.currentTimeMillis() - startT) / 1000
                           + "s");
        //按session分组划分训练集0.8，测试集0.2
        List<String> trainData = new ArrayList<>();
//        List<Integer> valSqlList = new ArrayList<>();
        List<String> testData = new ArrayList<>();
        for (Map.Entry<Integer, List<String>> entry : sessIdAndSqlIntentIndexes.entrySet()) {
            List<String> seqIntents = entry.getValue();
            List<String> trainSeqSqlList = seqIntents.subList(
                0,
                (int) (seqIntents.size() * split_train_ratio)
            );
//            List<Integer> valSeqSqlList = seqIntentIndexes.subList(
//                (int) (seqIntentIndexes.size() * 0.7),
//                (int) (seqIntentIndexes.size() * 0.85)
//            );
            List<String> testSeqSqlList = seqIntents.subList(
                (int) (seqIntents.size() * split_train_ratio),
                seqIntents.size()
            );
            trainData.addAll(trainSeqSqlList);
//            valSqlList.addAll(valSeqSqlList);
            testData.addAll(testSeqSqlList);
        }
        //保存trainData，testData 到文件中
        Util.writeSQLListToFile(testData, "test", "test");
        Util.writeSQLListToFile(trainData, "train", "train");
        //按固定split划分多个训练批次文件，每个批次内按固定窗口重新组织查询session
        reorganizeSqlList(trainData, window_size.getSeconds(), "train" + "_batchsize_" + SPLIT_BATCH_SIZE + "_");
        reorganizeSqlList(testData, window_size.getSeconds(), "test" + "_batchsize_" + SPLIT_BATCH_SIZE + "_");
    }

    public static void reorganizeSqlList(List<String> sqlList, long winSeconds, String prefixFileName)
    {
//        "Session 0, Query " + windowStart.toEpochSecond(ZoneOffset.UTC) + "; OrigQuery:"
        AtomicInteger count = new AtomicInteger();
        ArrayList<String> windowTablesIntent = new ArrayList<>(sqlList);
        HashMap<Integer, List<String>> sessSqls = Maps.newHashMap();
        List<String> batchSqlList = new ArrayList<>();
        for (int i = 0; i < windowTablesIntent.size(); i++) {
            List<String> curSeesionSqls = new ArrayList<>();
            sessSqls.put(i, curSeesionSqls);

            //add cursql
            //new sessId,queryId
            String sessId = "Session " + i;
            String queryId = ", Query " + curSeesionSqls.size();
            String truncateSqlElement = windowTablesIntent.get(i).split("; OrigQuery:")[1];
            addWindowIntent(curSeesionSqls, sessId, queryId, truncateSqlElement, count, prefixFileName, batchSqlList);

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
                addWindowIntent(
                    curSeesionSqls,
                    sessId,
                    queryId,
                    truncateSqlElement,
                    count,
                    prefixFileName,
                    batchSqlList
                );

//                nextWindowEndEventTime += winSeconds;
                nextWindowEndEventTime = Math.max(
                    nextWindowEndEventTime + winSeconds,
                    Long.parseLong(windowTablesIntent.get(endIndex).split(";")[0].split("Query")[1].trim())
                );
                endIndex = findFirstEndIndex(windowTablesIntent, nextWindowEndEventTime);
            }
        }
        //sessSqls中values存放到newSqlList中
        List<String> newRemainSqlList = new ArrayList<>();
        for (Map.Entry<Integer, List<String>> sessSql : sessSqls.entrySet()) {
            newRemainSqlList.addAll(sessSql.getValue());
        }
        addWindowIntent(newRemainSqlList, null, null, null, count, prefixFileName, batchSqlList);
        log.info("Total sessions:{}, split batchSize:{}", sessSqls.size(), SPLIT_BATCH_SIZE);
    }

    private static void addWindowIntent(
        List<String> curSeesionSqls,
        String sessId,
        String queryId,
        String truncateSqlElement,
        AtomicInteger count,
        String prefixFileName,
        List<String> batchSqlList
    )
    {
        if (queryId == null && !batchSqlList.isEmpty()) {
            //保存剩余split后数据
            Util.writeSQLListToFile(batchSqlList, outputDir,
                                    prefixFileName + ((count.get() / 1000) + 1)
            );
            log.info(
                "Saved split file end size[{}]:{}", curSeesionSqls.size(),
                prefixFileName + ((count.get() / 1000) + 1)
            );
            batchSqlList.clear();//clear，重新统计下个批次
        } else {
            curSeesionSqls.add(sessId + queryId + "; OrigQuery:" + truncateSqlElement);
            batchSqlList.add(sessId + queryId + "; OrigQuery:" + truncateSqlElement);
            int num = count.incrementAndGet();
            if (num % 1000 == 0) {
                //保存split后数据
                Util.writeSQLListToFile(batchSqlList, outputDir, prefixFileName + (num / 1000));
                log.info("Saved split file:{}, size:{}", prefixFileName + (num / 1000), curSeesionSqls);
                batchSqlList.clear();//clear，重新统计下个批次
            }
        }
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