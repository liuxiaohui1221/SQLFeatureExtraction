package tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static reader.ExcelReader.getOutputDir;

public class IOUtil {

    public static void writeFile(Map<String,Integer> stringIntegerMap, String outputD,String filePath) {
        File outputFile = new File(outputD);
        // 确保 output 目录存在
        if (!outputFile.exists() && !outputFile.mkdirs()) {
            throw new IllegalStateException("Cannot create output directory.");
        }
        try (FileWriter writer = new FileWriter(new File(outputD,filePath))) {
            stringIntegerMap.forEach((tableName,index) -> {
                try {
                    writer.write(tableName + ":" + index + "\n");
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeToFile(List<String> windowMetric, String file) {
        try (FileWriter writer = new FileWriter(getOutputDir() + file)) {
            windowMetric.forEach((metric) ->{
                try {
                writer.write(metric + "\n");
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
        });
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
