package age;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author wuweifeng wrote on 2018/4/23.
 */
public class FileGenerator {
    public static void main(String[] args) throws IOException {
        FileWriter fileWriter = new FileWriter(new File("/users/wuwf/age"));
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 100; i++) {
            String sex = "M";
            if (i % 2 == 0) {
                sex = "F";
            }
            fileWriter.write(i + " " + sex + " " + random.nextInt(100));
            fileWriter.write(System.lineSeparator());
        }
        fileWriter.flush();
        fileWriter.close();
    }
}
