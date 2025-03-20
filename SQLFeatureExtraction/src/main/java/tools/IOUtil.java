package tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static reader.ExcelReader.getOutputDir;

public class IOUtil {

    public static void writeFile(Map<String,Integer> stringIntegerMap, String outputDir,String filePath) {
        File outputFile = new File(outputDir, filePath);
        try (FileWriter writer = new FileWriter(outputFile)) {
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
}
