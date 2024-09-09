import java.util.BitSet;
import java.util.HashMap;

public class MinWindowQueryIntent {
    public static void main(String[] args) {
//		String homeDir = System.getProperty("user.home");
        System.out.println(MINCFragmentIntent.getMachineName());
//		if(MINCFragmentIntent.getMachineName().contains("4119510") || MINCFragmentIntent.getMachineName().contains("4119509")
//				|| MINCFragmentIntent.getMachineName().contains("4119508") || MINCFragmentIntent.getMachineName().contains("4119507")) {
//			homeDir = "/hdd2/vamsiCodeData"; // comment it when you are not running on EN4119510L.cidse.dhcp.adu.edu
//		}
//		String configFile = homeDir+"/var/data/MINC/InputOutput/MincJavaConfig.txt";
        String configFile = "/var/data/BusTracker/InputOutput/MincJavaConfig.txt";
        SchemaParser schParse = new SchemaParser();
        schParse.fetchSchema(configFile);
        HashMap<String, String> configDict = schParse.getConfigDict();
        String queryFile = "/Users/postgres/PycharmProjects/HumanIntentEvaluation/sample_100K.log";
        String concSessFile = configDict.get("MINC_CONC_SESS_FILE"); // already configDict prepends the homeDir
        String intentVectorFile = configDict.get("MINC_FRAGMENT_INTENT_VECTOR_FILE");
        String rawSessFile = configDict.get("MINC_RAW_SESS_FILE");
        String tempLogDir = configDict.get("MINC_TEMP_LOG_DIR");
        int numThreads = Integer.parseInt(configDict.get("MINC_NUM_THREADS"));
        int startLineNum = Integer.parseInt(configDict.get("MINC_START_LINE_NUM"));
        String pruneKeepModifyRepeatedQueries = configDict.get("MINC_KEEP_PRUNE_MODIFY_REPEATED_QUERIES");
        boolean includeSelOpConst = Boolean.parseBoolean(configDict.get("MINC_SEL_OP_CONST"));
        String dataset = configDict.get("MINC_DATASET");
        try {
            String line = null;
            String prevSessionID = null;
            int queryID = 0;

            //uncomment the following when full run needs to happen on EC2 or on EN4119510L
            //	readFromRawSessionsFile(dataset, tempLogDir, rawSessFile, intentVectorFile, line, schParse, numThreads, startLineNum, pruneKeepModifyRepeatedQueries, includeSelOpConst);

            String query = null;
	/*		String query = "SELECT distinct a.agency_id FROM m_agency a, m_calendar c, m_trip t WHERE c.agency_id = a.agency_id AND t.agency_id = a.agency_id AND "
					+ "a.avl_agency_name =  '8\\b8164b0b579a1a3cde19a106c8e1fca8' AND t.trip_id =  '33\\94f574661cc4d7d3c40a333a0509fd4f' "
					+ "AND c.start_date <= 1480475749583 AND c.end_date+1 >= 1480475749583";
			query = "select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
					+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, route.route_short_name "
					+ "from m_stop AS s RIGHT JOIN m_stop_time AS st  ON st.agency_id = s.agency_id "
					+ "AND st.stop_id = s.stop_id LEFT JOIN m_trip AS t ON t.agency_id = st.agency_id "
					+ "AND t.trip_id = st.trip_id LEFT JOIN m_route AS route ON t.agency_id = route.agency_id "
					+ "AND t.route_id = route.route_id WHERE st.estimate_source in "
					+ "( '10\\2d9d369aa6dcb27617fe409b5cac85ca',  '14\\dbcdf91e0b5531167767adab3b850514') "
					+ "AND st.agency_id = 1 AND (((departure_time_hour * 60 + departure_time_minute) >= (2-5)  "
					+ "AND (departure_time_hour * 60 + departure_time_minute) <= (3+10)) "
					+ "OR ((departure_time_hour * 60 + departure_time_minute) >= (4-5)  "
					+ "AND (departure_time_hour * 60 + departure_time_minute) <= (5+10)))  "
					+ "order by st.stop_sequence";
			 query = "SELECT s.stop_id AS stop_id, s.stop_name, s.stop_lat, s.stop_lon, ceiling((h_distance(0.0,0.0,s.stop_lat,s.stop_lon)/1.29)/60) "
			 		+ "AS walk_time  FROM m_stop s  WHERE s.stop_lat BETWEEN (1-2) AND (3+4)  AND s.agency_id = 5  AND s.stop_lon BETWEEN (6-7) AND (8+9)  "
			 		+ "ORDER BY (((s.stop_lat-(10))+(s.stop_lon-(11))))";
			 query = "SELECT id, message_title, message, destination_screen, stamp FROM m_messages WHERE (device = 1 OR device IS NULL) AND "
			 		+ "(agency_id = 2 OR agency_id IS NULL) AND (device_id = 3 OR device_id IS NULL) AND (app_version = 4 OR app_version IS NULL) "
			 		+ "AND (NOW() >= start_date OR start_date IS NULL) AND (NOW() < end_date OR end_date IS NULL) "
			 		+ "AND (trigger_cond = 5 OR "
			 		+ "trigger_cond IS NULL) AND (SELECT COUNT(*) FROM m_popup_user_log WHERE device_id = 6 AND "
			 		+ "date_trunc( '3\\1533bfb25649bd25dd740b47c19b84e4', stamp) = 3) < 1ORDER BY num_conditions DESC LIMIT 1";
			 query = "select nm.trip_id,nm.id AS message_id, nm.message, nm.timestamp, nm.category,a.firstname AS first_name, a.lastname AS last_name "
				 		+ "from dv_notes_message nm, dv_account a, (SELECT dvNotes.trip_id, MAX(dvNotes.timestamp) AS maximum FROM dv_notes_message dvNotes WHERE "
				 		+ "dvNotes.agency_id IN (select c.agency_id from m_agency c, m_agency d where c.agency_id_id=d.agency_id_id and d.agency_id=1) "
				 		+ "AND dvNotes.trip_id IN ( '35\\89ad84e1a460f2041220847c65206b20', '33\\9a6cce223e3aa56cfc2128721095071b', "
				 		+ "'34\\57c15fbbf86f09cc1453e35c8c89a357', '33\\4d266129b208b1d32a7d75b9b89ec5ea', '35\\cbd995b9084ac97bc03ef0e7695d4b8d', "
				 		+ "'34\\d21fa0245cc37fec431d5591d6192e89')AND dvNotes.category= '4\\2da45b72d28efeb9a3954206d2ae2fa6' GROUP BY dvNotes.trip_id) "
				 		+ "as nmmax WHERE nm.deleted IS NULL AND a.id=nm.user_id AND nm.trip_id= nmmax.trip_id AND nm.timestamp = nmmax.maximum "
				 		+ "AND nm.agency_id IN (select c.agency_id from m_agency c, m_agency d where c.agency_id_id=d.agency_id_id and d.agency_id=2)";

			//query = "SELECT a.agency_timezone FROM m_agency a WHERE a.agency_id = 80";
	*/
            query = "SELECT s.stop_id AS stop_id, s.stop_name, s.stop_lat, s.stop_lon, ceiling((h_distance(0.0,0.0,s.stop_lat,s.stop_lon)/1.29)/60) "
                    + "AS walk_time  FROM m_stop s  WHERE s.stop_lat BETWEEN (1-2) AND (3+4)  AND s.agency_id = 5  AND s.stop_lon BETWEEN (6-7) AND (8+9)  "
                    + "ORDER BY (((s.stop_lat-(10))+(s.stop_lon-(11))))";
            String query1 = "SELECT a.agency_timezone FROM m_agency a WHERE a.agency_id = 80";
//            query = "SELECT M.*, C.`option`, MIN(C.id) as component FROM jos_menu AS M LEFT JOIN jos_components AS C ON M.componentid = C.id "
//                    + "and M.name = C.name and M.ordering = C.ordering WHERE M.published = 1 and M.params=C.params GROUP BY M.sublevel HAVING M.lft = 2 "
//                    + "ORDER BY M.sublevel, M.parent, M.ordering";
            //query = "SELECT id FROM jos_menu WHERE `link` LIKE '%option=com_community&view=profile%' AND `published`=1";
            //query = "SELECT COUNT(*) as count from jos_community_questions as a LEFT JOIN jos_community_groups AS b ON a.parentid = b.id LEFT JOIN jos_community_groups_members AS c ON a.parentid = c.groupid WHERE a.id = 2902 AND (b.id IS NULL OR (b.id IS NOT NULL AND b.approvals=0))";
            //query = "SELECT m.*, c.`option`, MIN(c.id) as component FROM jos_menu AS m LEFT JOIN jos_components AS c ON m.componentid = c.id and m.name = c.name and m.ordering = c.ordering WHERE m.published = 1 and m.params=c.params GROUP BY m.sublevel HAVING m.lft = 2 ORDER BY m.sublevel, m.parent, m.ordering";
            //query = "SELECT AVG(menutype) from jos_menu";
            //query = "SELECT DISTINCT a.*, f.name AS creatorname, b.count, \"\" AS thumbnail, \"\" AS storage, 1 AS display, 1 AS privacy, b.last_updated FROM jos_community_photos_albums AS a LEFT JOIN ((SELECT id, approvals FROM jos_community_groups) UNION (SELECT id, approvals FROM jos_community_courses)) d ON a.groupid = d.id LEFT JOIN jos_community_groups_members AS c ON a.groupid = c.groupid LEFT JOIN (SELECT albumid, creator, COUNT(*) AS count, MAX(created) AS last_updated FROM jos_community_photos WHERE permissions = 0 OR (permissions = 2 AND (creator = 0 OR owner = 0)) GROUP BY albumid, creator) b ON a.id = b.albumid AND a.creator = b.creator INNER JOIN jos_users AS f ON a.creator = f.id WHERE (a.permissions = 0 OR (a.permissions = 2 AND (a.creator = 0 OR a.owner = 0))) AND (a.groupid = 0 OR (a.groupid > 0 AND (d.approvals = 0 OR (d.approvals = 1 AND c.memberid = 0))))";
            //query = "SELECT * from jos_menu AS m, jos_components AS c WHERE m.published = 1 and m.parent=\"X\"";
            //query = "UPDATE `jos_session` SET `time`='1538611062',`userid`='0',`usertype`='',`username`='',`gid`='0',`guest`='1',`client_id`='0',`data`='__default|a:9:{s:15:\\\"session.counter\\\";i:89;s:19:\\\"session.timer.start\\\";i:1538610776;s:18:\\\"session.timer.last\\\";i:1538611055;s:17:\\\"session.timer.now\\\";i:1538611060;s:22:\\\"session.client.browser\\\";s:71:\\\"Mozilla/5.0 (compatible; SEOkicks; +https://www.seokicks.de/robot.html)\\\";s:8:\\\"registry\\\";O:9:\\\"JRegistry\\\":3:{s:17:\\\"_defaultNameSpace\\\";s:7:\\\"session\\\";s:9:\\\"_registry\\\";a:1:{s:7:\\\"session\\\";a:1:{s:4:\\\"data\\\";O:8:\\\"stdClass\\\":0:{}}}s:7:\\\"_errors\\\";a:0:{}}s:4:\\\"user\\\";O:5:\\\"JUser\\\":19:{s:2:\\\"id\\\";i:0;s:4:\\\"name\\\";N;s:8:\\\"username\\\";N;s:5:\\\"email\\\";N;s:8:\\\"password\\\";N;s:14:\\\"password_clear\\\";s:0:\\\"\\\";s:8:\\\"usertype\\\";N;s:5:\\\"block\\\";N;s:9:\\\"sendEmail\\\";i:0;s:3:\\\"gid\\\";i:0;s:12:\\\"registerDate\\\";N;s:13:\\\"lastvisitDate\\\";N;s:10:\\\"activation\\\";N;s:6:\\\"params\\\";N;s:3:\\\"aid\\\";i:0;s:5:\\\"guest\\\";i:1;s:7:\\\"_params\\\";O:10:\\\"JParameter\\\":7:{s:4:\\\"_raw\\\";s:0:\\\"\\\";s:4:\\\"_xml\\\";N;s:9:\\\"_elements\\\";a:0:{}s:12:\\\"_elementPath\\\";a:1:{i:0;s:58:\\\"/var/www/html/minc/libraries/joomla/html/parameter/element\\\";}s:17:\\\"_defaultNameSpace\\\";s:8:\\\"_default\\\";s:9:\\\"_registry\\\";a:1:{s:8:\\\"_default\\\";a:1:{s:4:\\\"data\\\";O:8:\\\"stdClass\\\":0:{}}}s:7:\\\"_errors\\\";a:0:{}}s:9:\\\"_errorMsg\\\";N;s:7:\\\"_errors\\\";a:0:{}}s:8:\\\"view-926\\\";b:1;s:13:\\\"session.token\\\";s:32:\\\"50cf27c9c56d1d64c5a1203e192fc4e6\\\";}' WHERE session_id='buledanlab7lhtd5tpc6jcp5t5'";
            //query = "INSERT INTO `jos_session` ( `session_id`,`time`,`username`,`gid`,`guest`,`client_id` ) VALUES ( '7susns2ghsr6du1vic7g8cgja2','1538611066','','0','1','0' )";
            //query = "DELETE FROM jos_session WHERE ( time < '1538607473' )";
            //query = "SELECT COUNT(*) as count from jos_community_questions as a LEFT JOIN jos_community_groups AS b ON a.parentid = b.id LEFT JOIN jos_community_groups_members AS c ON a.parentid = c.groupid WHERE a.id = 331 AND (b.id IS NULL OR (b.id IS NOT NULL AND b.approvals=0))";
            //query = "UPDATE jos_users SET lastvisitDate = \"2018-10-05 16\"";
            //query = "INSERT INTO `jos_session` ( `session_id`,`time`,`username`,`gid`,`guest`,`client_id` ) VALUES ( 'ok12387ga01bj1pi49f9ffbuh0','1540013023','','0','1','0' )";
            //query = "SELECT count(*) FROM jos_community_usefulness WHERE resourceid = 925";
            //query = "SELECT id, title, module, position, content, showtitle, control, params FROM jos_modules AS m LEFT JOIN jos_modules_menu AS mm ON mm.moduleid = m.id WHERE m.published = 1 AND m.access <= 0 AND m.client_id = 0 AND ( mm.menuid = 53 OR mm.menuid = 0 ) ORDER BY position, ordering";
            //query = "SELECT a.`userid` as _userid , a.`status` as _status , a.`level` as _level , a.`points` as _points, a.`posted_on` as _posted_on, a.`avatar` as _avatar , a.`thumb` as _thumb , a.`invite` as _invite, a.`params` as _cparams, a.`view` as _view, a.`friendcount` as _friendcount, a.`alias` as _alias, s.`userid` as _isonline, u.* FROM jos_community_users as a LEFT JOIN jos_users u ON u.`id`=a.`userid` LEFT OUTER JOIN jos_session s ON s.`userid`=a.`userid` AND s.client_id !='1'WHERE a.`userid`='0'";
            //query = "SELECT * from jos_community_fields_values where value = 'MINC'";
            //query = "SELECT count(*)  FROM jos_community_connection as a, jos_users as b WHERE a.`connect_from`='3637' AND a.`status`=1  AND a.`connect_to`=b.`id` AND NOT EXISTS ( SELECT d.`blocked_userid` FROM `jos_community_blocklist` AS d WHERE d.`userid` = '3637' AND d.`blocked_userid` = a.`connect_to`)  ORDER BY a.`connection_id` DESC";
            //query = "SELECT COUNT(*) FROM `jos_community_groups_members` AS a INNER JOIN `jos_users` AS b WHERE b.id=a.memberid AND a.memberid NOT IN (SELECT userid from jos_community_courses_ta where courseid = 3569) AND a.groupid='3569' AND a.permissions='1'";
            //query = "SELECT DISTINCT a.* FROM jos_community_apps AS a WHERE a.`userid`='72' AND a.`apps`!=\"news_feed\" AND a.`apps`!=\"profile\" AND a.`apps`!=\"friends\" AND a.`apps` IN ('walls') ORDER BY a.`ordering`";
            //query = "SELECT          a.`userid` as _userid ,         a.`status` as _status ,         a.`level`  as _level ,  a.`points`      as _points,     a.`posted_on` as _posted_on,    a.`avatar`      as _avatar ,    a.`thumb`       as _thumb ,     a.`invite`      as _invite,     a.`params`      as _cparams,    a.`view`        as _view,  a.`alias`    as _alias,  a.`friendcount` as _friendcount, s.`userid` as _isonline, u.*  FROM jos_community_users as a  LEFT JOIN jos_users u  ON u.`id`=a.`userid`  LEFT OUTER JOIN jos_session s  ON s.`userid`=a.`userid` WHERE a.`userid` IN (2839,2824,2828)";
            MINCFragmentIntent fragmentObj = new MINCFragmentIntent(query, schParse, includeSelOpConst, dataset);
            boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
            if(validQuery) {
                fragmentObj.printIntentVector();
                fragmentObj.writeIntentVectorToTempFile(query);
            }
            // string value translate to bitset
            String queryIntent=fragmentObj.getIntentBitVector();
            BitSet minbatchQueryBitSets = new BitSet(queryIntent.length());
            int maxLen=queryIntent.length();
            System.out.println(queryIntent.length()+" bitset length:"+minbatchQueryBitSets.length());
            minbatchQueryBitSets=updateMinBitSet(minbatchQueryBitSets, queryIntent);
            System.out.println("before:"+bitsetToString(minbatchQueryBitSets,maxLen));
            MINCFragmentIntent fragmentObj1 = new MINCFragmentIntent(query1, schParse, includeSelOpConst, dataset);
            boolean validQuery1 = fragmentObj1.parseQueryAndCreateFragmentVectors();
            if(validQuery1) {
                fragmentObj1.printIntentVector();
                fragmentObj1.writeIntentVectorToTempFile(query);
            }
            String queryIntent1=fragmentObj1.getIntentBitVector();
            minbatchQueryBitSets=updateMinBitSet(minbatchQueryBitSets, queryIntent1);
            //print bitset
            System.out.println("update:"+bitsetToString(minbatchQueryBitSets,maxLen));
            System.out.println(fragmentObj.getIntentBitVector().length()+"bitset length:"+minbatchQueryBitSets.length());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public static BitSet updateMinBitSet(BitSet minBatchBitSet, String queryIntent) {
        for(int i = 0; i < queryIntent.length(); i++) {
            if(queryIntent.charAt(i) == '1'){
                minBatchBitSet.set(i, true);
            }else{
                minBatchBitSet.set(i, false);
            }
        }
        return minBatchBitSet;
    }
    public static String bitsetToString(BitSet bitSet,int maxLen) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < bitSet.length(); i++) {
            sb.append(bitSet.get(i) ? "1" : "0");
        }
        if(sb.length() < maxLen) {
            for(int i = sb.length(); i < maxLen; i++) {
                sb.append("0");
            }
        }
        return sb.toString();
    }
}
