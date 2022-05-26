package atgg.basic01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Project: my-flink-project Package: atgg
 *
 * <p>User: Staticor Date: 2022/5/7 Time: 13:50
 *
 * <p>Created with IntelliJ IDEA author: staticor
 */
public class ClickSource implements SourceFunction<Event> {

  String[] users = {"zhangsan", "alice", "bob", "catherine", "dogg"};

  String[] urls = {"./home", "./search", "./product", "./activity", ",.cart", "./pay"};

  Random random = new Random();

  public static String getRandomString(int length) {
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random = new Random();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(62);
      sb.append(str.charAt(number));
    }
    return sb.toString();
  }

  private Boolean running = true;

  @Override
  public void run(SourceContext<Event> ctx) throws Exception {
    // 循环生成数据
    while (running) {
      String user = users[random.nextInt(users.length)];

      String url = urls[random.nextInt(urls.length)];

      Long timestamp = Calendar.getInstance().getTimeInMillis();

      //            SourceContext.collect(new Event(getRandomString(4), getRandomString(10), new
      // Random().nextLong()));

      ctx.collect(new Event(user, url, timestamp));

      // 降低频率
      Thread.sleep(500);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}