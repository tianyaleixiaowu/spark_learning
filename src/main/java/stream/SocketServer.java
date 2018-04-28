package stream;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author wuweifeng wrote on 2018/4/24.
 */
public class SocketServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        SocketServer socketService = new SocketServer();
        socketService.oneServer();
    }

    @SuppressWarnings("resource")
    public void oneServer() throws IOException, InterruptedException {
        final ServerSocket server = new ServerSocket(55533);
        Executor executor = Executors.newFixedThreadPool(100);
        final List<String> list = new ArrayList<>();
        list.add("aa");
        list.add("bb");
        list.add("cc");
        list.add("dd");
        list.add("ee");

        for (int i = 0; i < 100; i++) {
            Thread.sleep(10000);
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        //与客户端建立连接
                        Socket conn = server.accept();
                        PrintWriter writer = new PrintWriter(conn.getOutputStream());

                        System.out.println("启动线程");
                        Random r = new java.util.Random();
                        //动态字符串
                        String outPut = list.get(r.nextInt(4))
                                + " " + list.get(r.nextInt(4))
                                + " " + list.get(r.nextInt(4));
                        //向客户端输出
                        writer.write(outPut);
                        writer.flush();
                        System.out.println("Server:" + outPut);
                        writer.close();
                        conn.close();
                    } catch (Throwable tb) {
                        tb.printStackTrace();
                    }
                }
            });
        }
    }
}