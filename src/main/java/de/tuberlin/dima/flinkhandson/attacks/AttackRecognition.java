package de.tuberlin.dima.flinkhandson.attacks;

import de.tuberlin.dima.flinkhandson.Config;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Time;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class AttackRecognition {

  public static void main(String[] args) throws Exception {
    new LogFileSimulator().start();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<LogFileEntry> logStream = env
        .readTextStream(Config.outputPathToLogFile())
        .map(new MapFunction<String, LogFileEntry>() {
      private LogFileEntry e = new LogFileEntry();
      @Override
      public LogFileEntry map(String value) throws Exception {
        String[] splitted = value.split(":");
        e.timestamp = Long.valueOf(splitted[0]);
        e.ip = splitted[1];
        return e;
      }
    });

    // IMPLEMENT THIS STEP

    env.execute();
  }

  // --------------------------------------------------------------------------------------------
  //                          Utilities
  // --------------------------------------------------------------------------------------------

  public static class LogFileEntry implements Serializable {
    public Long timestamp;
    public String ip;
  }

  /**
   * Generates log file entries with "timestamp:IP address". Simulates an attack.
   */
  public static class LogFileSimulator extends Thread {

    private static int TIME_BETWEEN_ENTRIES = 200;
    private static int ATTACK_EVERY = 10;
    private static int NUMBER_ATTACK_MESSAGES = 50;

    public LogFileSimulator() throws IOException {
      File f = new File(Config.outputPathToLogFile());
      f.getParentFile().mkdirs();
      f.createNewFile();
    }

    public void run() {
      long i = 0;
      while (true) {
        try {
          Thread.sleep(TIME_BETWEEN_ENTRIES);
        } catch (InterruptedException e) {
          // do nothing
        }
        if (i == ATTACK_EVERY) {
          i = 0;
          for (int j = 0; j < NUMBER_ATTACK_MESSAGES; j++) {
            writeLogEntry("86.102.2." + ((int) (Math.random()*128 + 1)));
          }
        }
        else {
          writeLogEntry("86.47.1.12");
        }
        i++;
      }
    }

    private static void writeLogEntry(String message) {
      PrintWriter out = null;
      try {
        out = new PrintWriter(new BufferedWriter(new FileWriter(Config.outputPathToLogFile(), true)));
        out.println(System.currentTimeMillis() + ":" + message);
      } catch (IOException e) {
        System.err.println(e);
      } finally {
        if(out != null){
          out.close();
        }
      }
    }
  }
}
