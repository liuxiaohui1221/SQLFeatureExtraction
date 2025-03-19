package demo;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Demo02 {


        public static void main(String[] args) {
            String dateTimeString1 = "2023/5/7 0:37";
            String dateTimeString2 = "2023/5/7 10:37";

            // 定义时间格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/M/d H:mm");

            // 解析时间字符串
            LocalDateTime dateTime1 = LocalDateTime.parse(dateTimeString1, formatter);
            LocalDateTime dateTime2 = LocalDateTime.parse(dateTimeString2, formatter);

            // 输出解析后的时间
            System.out.println("Parsed DateTime 1: " + dateTime1);
            System.out.println("Parsed DateTime 2: " + dateTime2);
        }
}
