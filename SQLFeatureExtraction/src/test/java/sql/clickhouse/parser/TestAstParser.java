package sql.clickhouse.parser;

import sql.clickhouse.ClickhouseSQLParser;
import sql.clickhouse.SchemaParser;
import sql.clickhouse.parser.ast.DistributedTableInfoDetector;
import sql.clickhouse.parser.ast.INode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;
import sql.reader.ExcelReader;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class TestAstParser {

    @Test
    public void testAstParser() {
        // parse SQL and generate its AST
        AstParser astParser = new AstParser();
        String sql1 = "ALTER TABLE my_db.my_tbl ADD COLUMN IF NOT EXISTS id Int64";
        Object parsedResult1 = astParser.parse(sql1);
        log.info(parsedResult1.toString());
        String sql2 = "ALTER TABLE my_db.my_tbl DROP PARTITION '2020-11-21'";
        Object parsedResult2 = astParser.parse(sql2);
        log.info(parsedResult2.toString());
    }

    @Test
    public void testReferredTablesDetector() {
        String sql = "SELECT t1.a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id LIMIT 1000";
        AstParser astParser = new AstParser();
        Object ast = astParser.parse(sql);
        ReferredTablesDetector referredTablesDetector = new ReferredTablesDetector();
        List<String> tables = referredTablesDetector.searchTables((INode) ast);
        tables.parallelStream().forEach(table -> System.out.println(table));
    }

    @Test
    public void testDistributedTableInfoDetector() {
        String sql = "CREATE TABLE my_db.my_tbl (date Date, name String) Engine = Distributed('my_cluster', 'my_db', 'my_tbl_local', rand())";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }

    @Test
    public void testDistributedTableInfoDetector2() {
        String sql = "CREATE TABLE mydb.mytb (uuid UUID DEFAULT generateUUIDv4(), cktime DateTime DEFAULT now() COMMENT 'c', openid String, username String, appid String, from_channel String, source_channel String, source String, regtime DateTime, brandid String, devicecode String, actiontime DateTime, ismingamelogin String, version String, platform String, project String, plat String, source_openid String COMMENT 'a', event Int16 COMMENT 'b') ENGINE = ReplicatedMergeTree('/clickhouse/mydb/mytb/{shard}', '{replica}') PARTITION BY toYYYYMM(cktime) ORDER BY (regtime, appid, openid) SETTINGS index_granularity = 8192";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }

    @Test
    public void testDistributedTableInfoDetector3() {
        String sql = "CREATE TABLE my_db.my_tbl on cluster my_cluster Engine = Distributed('my_cluster', 'my_db', 'my_tbl_local', rand()) as my_db.my_tbl_local";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }

    @Test
    public void testSQL() {
        String configFile = "input/ApmJavaConfig.txt";
        SchemaParser schParse = new SchemaParser();
        schParse.fetchSchema(configFile);
        ClickhouseSQLParser calciteSQLParser = new ClickhouseSQLParser(schParse);
        String tsvFilePath = "input/ApmQuerys.tsv"; // TSV 文件路径
        try (Reader reader = Files.newBufferedReader(Paths.get(tsvFilePath))) {
            CSVParser csvParser = new CSVParser(reader, CSVFormat.TDF.withFirstRecordAsHeader());
            Iterable<CSVRecord> records = csvParser.getRecords();

            // 处理标题行
            List<String> headers = csvParser.getHeaderNames();
            System.out.println("Headers: " + headers);
            int count=0;
            //1.抽取字段，聚合函数，2.查询编码向量
            for (CSVRecord record : records) {
                String query = record.get("query");
                if(!ExcelReader.filterSql(query)){
                    continue;
                }
                count++;
                //System.out.println("origin query:"+query);
                calciteSQLParser.createQueryVector(query,0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("successCount:"+ ClickhouseSQLParser.successCount.get());
        System.out.println("failCount:"+ ClickhouseSQLParser.failCount.get());
    }
    @Test
    public void testSQL2() {
        String query="SELECT count() AS total_RESP, toStartOfInterval(ts, INTERVAL 7 day, 'Asia/Shanghai') AS ts_RESP FROM dwm_request_cluster WHERE (appid = 'pro-api-g10-xingyun') AND (ts <= toDateTime64(1684487339.999, 3)) AND (ts >= toDateTime64(1677834480.000, 3)) GROUP BY ts_RESP ORDER BY ts_RESP ASC";
        ClickhouseSQLParser calciteSQLParser = new ClickhouseSQLParser(null);
        calciteSQLParser.createQueryVector(query,0);
        System.out.println("successCount:"+ ClickhouseSQLParser.successCount.get());
        System.out.println("failCount:"+ ClickhouseSQLParser.failCount.get());
    }

    @Test
    public void testSQL3() {
        String query="SELECT sum(biz) AS biz_RESP, sum(err) AS err_RESP, sum(exception) AS exception_RESP, sum(fail) AS fail_RESP, sum(frustrated) AS frustrated_RESP, sum(tolerated) AS tolerated_RESP, count() AS total_RESP, group FROM dwm_request WHERE (appid = 'pro-api-g10-xingyun') AND (is_model = true) AND (ts <= toDateTime64(1684406399.999, 3)) AND (ts >= toDateTime64(1683801540.000, 3)) GROUP BY group ORDER BY total_RESP DESC LIMIT 0, 5";
        ClickhouseSQLParser calciteSQLParser = new ClickhouseSQLParser(null);
        calciteSQLParser.createQueryVector(query,0);
        System.out.println("successCount:"+ ClickhouseSQLParser.successCount.get());
    }
    @Test
    public void testSQL4() {
        String query="select * from viewifpermitted(select message from system.warnings else null('message string'))";
        ClickhouseSQLParser calciteSQLParser = new ClickhouseSQLParser(null);
        calciteSQLParser.createQueryVector(query,0);
        System.out.println("successCount:"+ ClickhouseSQLParser.successCount.get());
    }


}
