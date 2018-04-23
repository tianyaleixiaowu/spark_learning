package age;

import java.io.*;

/**
 * @author wuweifeng wrote on 2018/4/23.
 */
public class JavaCount {
    public static void main(String[] args) throws FileNotFoundException {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null; //用于包装InputStreamReader,提高处理性能。因为BufferedReader有缓冲的，而InputStreamReader没有。
        try {
            String str = "";
            String str1 = "";
            long time = System.currentTimeMillis();
            
            long sum = 0;
            fis = new FileInputStream("/users/wuwf/age");// FileInputStream
            // 从文件系统中的某个文件中获取字节
            isr = new InputStreamReader(fis);// InputStreamReader 是字节流通向字符流的桥梁,
            br = new BufferedReader(isr);// 从字符输入流中读取文件中的内容,封装了一个new InputStreamReader的对象
            while ((str = br.readLine()) != null) {
                sum += Long.valueOf(str.split(" ")[1]);
            }
            System.out.println(System.currentTimeMillis() - time);
            // 当读取的一行不为空时,把读到的str的值赋给str1
            System.out.println(sum);// 打印出str1
        } catch (FileNotFoundException e) {
            System.out.println("找不到指定文件");
        } catch (IOException e) {
            System.out.println("读取文件失败");
        } finally {
            try {
                br.close();
                isr.close();
                fis.close();
                // 关闭的时候最好按照先后顺序关闭最后开的先关闭所以先关s,再关n,最后关m
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
