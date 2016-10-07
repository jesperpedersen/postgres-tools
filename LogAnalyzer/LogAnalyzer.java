/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Jesper Pedersen <jesper.pedersen@comcast.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Log analyzer
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class LogAnalyzer
{
   /** Default configuration */
   private static final String DEFAULT_CONFIGURATION = "loganalyzer.properties";

   /** The configuration */
   private static Properties configuration;

   /** Color 1 */
   private static final String COLOR_1 = "#cce6ff";

   /** Color 2 */
   private static final String COLOR_2 = "#ccffcc";

   /** Keep the raw data */
   private static boolean keepRaw;

   /** Raw data:      Process  Log */
   private static Map<Integer, List<String>> rawData = new TreeMap<>();

   /** Interaction */
   private static boolean interaction;

   /** Data:          Process  LogEntry */
   private static Map<Integer, List<LogEntry>> data = new TreeMap<>();

   /** Statements:    SQL     Count */
   private static Map<String, Integer> statements = new TreeMap<>();

   /** Max time:      SQL     Max time */
   private static Map<String, Double> maxtime = new TreeMap<>();

   /** Total time:    SQL     Time */
   private static Map<String, Double> totaltime = new TreeMap<>();

   /** The file name */
   private static String filename;
   
   /** The start date */
   private static String startDate;
   
   /** The end date */
   private static String endDate;
   
   /** Parse time */
   private static double parseTime;
   
   /** Bind time */
   private static double bindTime;
   
   /** Execute time */
   private static double executeTime;
   
   /** Empty time */
   private static double emptyTime;
   
   /**
    * Write data to a file
    * @param p The path of the file
    * @param l The data
    */
   private static void writeFile(Path p, List<String> l) throws Exception
   {
      BufferedWriter bw = Files.newBufferedWriter(p,
                                                  StandardOpenOption.CREATE,
                                                  StandardOpenOption.WRITE,
                                                  StandardOpenOption.TRUNCATE_EXISTING);
      for (String s : l)
      {
         bw.write(s, 0, s.length());
         bw.newLine();
      }

      bw.flush();
      bw.close();
   }

   /**
    * Write index.html
    */
   private static void writeIndex() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Log Analysis</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Log Analysis</h1>");
      l.add("");

      l.add("<table>");
      l.add("<tr>");
      l.add("<td><b>File</b></td>");
      l.add("<td>" + filename + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Start</b></td>");
      l.add("<td>" + startDate + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>End</b></td>");
      l.add("<td>" + endDate + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Run</b></td>");
      l.add("<td><a href=\"run.properties\">Link</a></td>");
      l.add("</tr>");
      l.add("</table>");

      l.add("<p>");

      int selectWeight = 0;
      int updateWeight = 0;
      int insertWeight = 0;
      int deleteWeight = 0;
      double totalWeight = 0;
      
      TreeMap<Integer, List<String>> counts = new TreeMap<>();
      for (String stmt : statements.keySet())
      {
         Integer count = statements.get(stmt);

         String upper = stmt.toUpperCase();
         if (upper.startsWith("SELECT"))
         {
            selectWeight += count;
         }
         else if (upper.startsWith("UPDATE"))
         {
            updateWeight += count;
         }
         else if (upper.startsWith("INSERT"))
         {
            insertWeight += count;
         }
         else if (upper.startsWith("DELETE"))
         {
            deleteWeight += count;
         }
         
         List<String> ls = counts.get(count);
         if (ls == null)
            ls = new ArrayList<>();
         ls.add(stmt);
         counts.put(count, ls);
      }

      totalWeight = selectWeight + updateWeight + insertWeight + deleteWeight;
      
      l.add("<h2>Overview</h2>");

      l.add("<table>");
      l.add("<tr>");
      l.add("<td><b>SELECT</b></td>");
      l.add("<td>" +  String.format("%.2f", ((selectWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td><b>PARSE</b></td>");
      l.add("<td>" +  String.format("%.3f", parseTime) + " ms</td>");
      l.add("<td><b>BEGIN</b></td>");
      l.add("<td>" + statements.get("BEGIN") + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>UPDATE</b></td>");
      l.add("<td>" + String.format("%.2f", ((updateWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td><b>BIND</b></td>");
      l.add("<td>" +  String.format("%.3f", bindTime) + " ms</td>");
      l.add("<td><b>COMMIT</b></td>");
      l.add("<td>" + statements.get("COMMIT") + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>INSERT</b></td>");
      l.add("<td>" + String.format("%.2f", ((insertWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td><b>EXECUTE</b></td>");
      l.add("<td>" +  String.format("%.3f", executeTime) + " ms</td>");
      l.add("<td><b>ROLLBACK</b></td>");
      l.add("<td>" + statements.get("ROLLBACK") + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>DELETE</b></td>");
      l.add("<td>" + String.format("%.2f", ((deleteWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td><b>TOTAL</b></td>");
      l.add("<td>" +  String.format("%.3f", parseTime + bindTime + executeTime) + " ms</td>");
      l.add("<td>" + (emptyTime > 0.0 ? "<b>EMPTY</b>" : "") + "</td>");
      l.add("<td>" + (emptyTime > 0.0 ? String.format("%.3f", emptyTime) + " ms" : "") + "</td>");
      l.add("</tr>");
      l.add("</table>");

      l.add("<p>");

      l.add("<table border=\"1\">");

      for (Integer count : counts.descendingKeySet())
      {
         List<String> ls = counts.get(count);
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < ls.size(); i++)
         {
            String v = ls.get(i);
            if (!filterStatement(v))
            {
               sb = sb.append(v);
               if (i < ls.size() - 1)
                  sb = sb.append("<p>");
            }
         }

         if (sb.length() > 0)
         {
            l.add("<tr>");
            l.add("<td>" + count + "</td>");
            l.add("<td>" + sb.toString() + "</td>");
            l.add("</tr>");
         }
      }
      l.add("</table>");
      l.add("<p>");

      l.add("<h2>Total time</h2>");
      List<String> interactionLinks = new ArrayList<>();
      for (Integer processId : data.keySet())
      {
         List<LogEntry> lle = data.get(processId);
         int executeCount = getExecuteCount(lle);
         if (executeCount > 0)
         {
            if (interaction)
               interactionLinks.add("<a href=\"" + processId + ".html\">" + processId + "</a>(" + executeCount + ")&nbsp;");
            writeInteractionReport(processId, lle);
         }
      }

      TreeMap<Double, List<String>> times = new TreeMap<>();
      for (String stmt : totaltime.keySet())
      {
         Double d = totaltime.get(stmt);
         List<String> stmts = times.get(d);
         if (stmts == null)
            stmts = new ArrayList<>();

         stmts.add(stmt);
         times.put(d, stmts);
      }

      l.add("<table border=\"1\">");
      int count = 0;
      for (Double d : times.descendingKeySet())
      {
         List<String> stmts = times.get(d);
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < stmts.size(); i++)
         {
            sb = sb.append(stmts.get(i));
            if (i < stmts.size() - 1)
               sb = sb.append("<p>");
         }

         l.add("<tr>");
         l.add("<td>" + String.format("%.3f", d) + "ms</td>");
         l.add("<td>" + sb.toString() + "</td>");
         l.add("</tr>");
         count++;

         if (count == 20)
            break;
      }
      l.add("</table>");
      
      l.add("<h2>Max time</h2>");
      times = new TreeMap<>();
      for (String stmt : maxtime.keySet())
      {
         Double d = maxtime.get(stmt);
         List<String> stmts = times.get(d);
         if (stmts == null)
            stmts = new ArrayList<>();

         stmts.add(stmt);
         times.put(d, stmts);
      }

      l.add("<table border=\"1\">");
      count = 0;
      for (Double d : times.descendingKeySet())
      {
         List<String> stmts = times.get(d);
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < stmts.size(); i++)
         {
            sb = sb.append(stmts.get(i));
            if (i < stmts.size() - 1)
               sb = sb.append("<p>");
         }

         l.add("<tr>");
         l.add("<td>" + String.format("%.3f", d) + "ms</td>");
         l.add("<td>" + sb.toString() + "</td>");
         l.add("</tr>");
         count++;

         if (count == 20)
            break;
      }
      l.add("</table>");

      if (interaction)
      {
         l.add("<h2>Interactions</h2>");
         l.addAll(interactionLinks);
      }
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Write the interaction report
    * @param processId The process id
    * @param lle The interactions
    */
   private static void writeInteractionReport(Integer processId, List<LogEntry> lle) throws Exception
   {
      List<String> queries = new ArrayList<>();
      double totalDuration = 0.0;
      double totalEmpty = 0.0;
      double duration = 0.0;
      int begin = 0;
      int commit = 0;
      int rollback = 0;
      boolean color = true;
      boolean inTransaction = false;
      double transactionTime = 0.0;
      
      for (LogEntry le : lle)
      {
         if (le.isParse())
         {
            duration += le.getDuration();
            totalDuration += duration;
         }
         else if (le.isBind())
         {
            duration += le.getDuration();
            totalDuration += duration;
            
            String s = le.getStatement();
            if (s == null || "".equals(s.trim()))
            {
               if (interaction)
               {
                  queries.add("<tr>");
                  queries.add("<td>" + String.format("%.3f", duration) + "</td>");
                  queries.add("<td>" + String.format("%.3f", duration) + "</td>");
                  queries.add("<td>" + (le.isPrepared() ? "P" : "S") + "</td>");
                  queries.add("<td></td>");
                  queries.add("</tr>");
               }
               totalEmpty += duration;
               duration = 0.0;
               transactionTime = 0.0;
            }
         }
         else if (le.isExecute())
         {
            duration += le.getDuration();
            totalDuration += duration;

            String s = le.getStatement();
            if (s != null)
            {
               if ("BEGIN".equals(s))
               {
                  begin++;
                  inTransaction = true;
                  transactionTime = duration;
               }
               else if (s.startsWith("COMMIT"))
               {
                  commit++;
                  transactionTime += duration;
               }
               else if (s.startsWith("ROLLBACK"))
               {
                  rollback++;
                  transactionTime += duration;
               }
               else
               {
                  // Total time
                  Double time = totaltime.get(s);
                  if (time == null)
                  {
                     time = new Double(duration);
                  }
                  else
                  {
                     time = new Double(time.doubleValue() + duration);
                  }
                  totaltime.put(s, time);

                  // Max time
                  time = maxtime.get(s);
                  if (time == null || duration > time.doubleValue())
                  {
                     time = new Double(duration);
                     maxtime.put(s, time);
                  }
                  
                  if (inTransaction)
                     transactionTime += duration;
               }

               if (interaction)
               {
                  queries.add("<tr style=\"background-color: " + (color ? COLOR_1 : COLOR_2) + ";\">");
                  queries.add("<td>" + String.format("%.3f", (inTransaction ? transactionTime : duration)) + "</td>");
                  queries.add("<td>" + String.format("%.3f", duration) + "</td>");
                  queries.add("<td>" + (le.isPrepared() ? "P" : "S") + "</td>");
                  queries.add("<td>" + s + "</td>");
                  queries.add("</tr>");
               }

               if (s.startsWith("COMMIT"))
               {
                  inTransaction = false;
               }
               else if (s.startsWith("ROLLBACK"))
               {
                  inTransaction = false;
               }

               if (!inTransaction)
               {
                  color = !color;
                  transactionTime = 0.0;
               }

               duration = 0.0;
            }
         }
      }

      if (interaction)
      {
         List<String> l = new ArrayList<>();
         l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
         l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
         l.add("");
         l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
         l.add("<head>");
         l.add("  <title>Log Analysis</title>");
         l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
         l.add("</head>");
         l.add("<body>");
         l.add("<h1>" + processId + "</h1>");
         l.add("");

         l.add("<h2>Overview</h2>");
         l.add("<table>");
         l.add("<tr>");
         l.add("<td><b>Time</b></td>");
         l.add("<td>" + String.format("%.3f", totalDuration) + " ms" +
               (totalEmpty > 0.0 ? " (" + String.format("%.3f", totalEmpty) + " ms)" : "")
               + "</td>");
         l.add("</tr>");
         l.add("<tr>");
         l.add("<td><b>Transaction</b></td>");
         l.add("<td>BEGIN: " + begin + " / COMMIT: " + commit + " / ROLLBACK: " + rollback + "</td>");
         l.add("</tr>");
         l.add("</table>");

         l.add("<h2>Executed</h2>");
         l.add("<table border=\"1\">");
         l.addAll(queries);
         l.add("</table>");

         if (keepRaw)
         {
            l.add("<h2>Raw</h2>");
            l.add("<a href=\"" + processId + ".log\">Link</a>");
         }
      
         l.add("<p>");
         l.add("<a href=\"index.html\">Back</a>");
         l.add("</body>");
         l.add("</html>");

         writeFile(Paths.get("report", processId + ".html"), l);
      }
   }
   
   /**
    * Write run.properties
    */
   private static void writeQueryAnalyzerFile() throws Exception
   {
      List<String> l = new ArrayList<>();
      List<String> queries = new ArrayList<>();

      int select = 1;
      int update = 1;
      int insert = 1;
      int delete = 1;

      int padding = (int)Math.log10(statements.size()) + 1;
      
      l.add("# https://github.com/jesperpedersen/postgres-tools/tree/master/QueryAnalyzer");
      l.add("host=localhost # ChangeMe");
      l.add("port=5432 # ChangeMe");
      l.add("database=test # ChangeMe");
      l.add("user=test # ChangeMe");
      l.add("password=test # ChangeMe");

      for (String stmt : statements.keySet())
      {
         String upper = stmt.toUpperCase();
         if (upper.startsWith("SELECT"))
         {
            queries.add("query.select." + String.format("%0" + padding + "d", select) + "=" + stmt);
            select++;
         }
         else if (upper.startsWith("UPDATE"))
         {
            queries.add("query.update." + String.format("%0" + padding + "d", update) + "=" + stmt);
            update++;
         }
         else if (upper.startsWith("INSERT"))
         {
            queries.add("query.insert." + String.format("%0" + padding + "d", insert) + "=" + stmt);
            insert++;
         }
         else if (upper.startsWith("DELETE"))
         {
            queries.add("query.delete." + String.format("%0" + padding + "d", delete) + "=" + stmt);
            delete++;
         }
      }

      Collections.sort(queries);
      l.addAll(queries);
      
      writeFile(Paths.get("report", "run.properties"), l);
   }
   
   /**
    * Get the execute count
    * @param lle The interactions
    * @return The count
    */
   private static int getExecuteCount(List<LogEntry> lle)
   {
      int count = 0;

      for (LogEntry le : lle)
      {
         if (le.isExecute())
            count++;
      }
      
      return count;
   }
   
   /**
    * Process the log
    */
   private static void processLog() throws Exception
   {
      List<String> l = Files.readAllLines(Paths.get(filename));

      for (int i = 0; i < l.size(); i++)
      {
         String s = l.get(i);
         LogEntry le = new LogEntry(s);

         // Raw data insert
         if (keepRaw)
         {
            List<String> ls = rawData.get(le.getProcessId());
            if (ls == null)
               ls = new ArrayList<>();
            ls.add(s);
            rawData.put(le.getProcessId(), ls);
         }

         // Data insert
         List<LogEntry> lle = data.get(le.getProcessId());
         if (lle == null)
            lle = new ArrayList<>();
         lle.add(le);
         data.put(le.getProcessId(), lle);
         
         if (le.isParse())
         {
            parseTime += le.getDuration();
         }
         else if (le.isBind())
         {
            bindTime += le.getDuration();

            String stmt = le.getStatement();
            if (stmt == null || "".equals(stmt.trim()))
               emptyTime += le.getDuration() + lle.get(lle.size() - 2).getDuration();
         }
         else if (le.isExecute())
         {
            executeTime += le.getDuration();

            String stmt = le.getStatement();

            // Statements insert
            Integer count = statements.get(stmt);
            if (count == null)
            {
               count = Integer.valueOf(1);
            }
            else
            {
               count = Integer.valueOf(count.intValue() + 1);
            }
            statements.put(stmt, count);
         }
         
         if (i == 0)
         {
            startDate = le.getTimestamp();
         }
         else if (i == l.size() - 1)
         {
            endDate = le.getTimestamp();
         }         
      }

      if (keepRaw)
      {
         for (Integer proc : rawData.keySet())
         {
            List<String> write = rawData.get(proc);
            writeFile(Paths.get("report", proc + ".log"), write);
         }
      }
   }

   /**
    * Should the statement be filtered from the report
    * @param stmt The statement
    * @return The result
    */
   private static boolean filterStatement(String stmt)
   {
      if ("BEGIN".equals(stmt) || stmt.startsWith("ROLLBACK") || stmt.startsWith("COMMIT"))
         return true;

      return false;
   }

   /**
    * Read the configuration (replay.properties)
    * @param config The configuration
    */
   private static void readConfiguration(String config) throws Exception
   {
      File f = new File(config);
      configuration = new Properties();

      if (f.exists())
      {
         FileInputStream fis = null;
         try
         {
            fis = new FileInputStream(f);
            configuration.load(fis);
         }
         finally
         {
            if (fis != null)
            {
               try
               {
                  fis.close();
               }
               catch (Exception e)
               {
                  // Nothing todo
               }
            }
         }
      }
   }

   /**
    * Setup
    */
   private static void setup() throws Exception
   {
      File report = new File("report");
      report.mkdir();
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length != 1)
         {
            System.out.println("Usage: LogAnalyzer <log_file>");
            return;
         }

         readConfiguration(DEFAULT_CONFIGURATION);
         keepRaw = Boolean.valueOf(configuration.getProperty("keep_raw", "false"));
         interaction = Boolean.valueOf(configuration.getProperty("interaction", "true"));

         setup();

         filename = args[0];
         processLog();
         writeIndex();
         writeQueryAnalyzerFile();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   /**
    * Log entry
    */
   static class LogEntry
   {
      private int processId;
      private String timestamp;
      private int transactionId;
      private String fullStatement;
      private String statement;
      private boolean prepared;
      private double duration;
      private boolean parse;
      private boolean bind;
      private boolean execute;
      
      LogEntry(String s)
      {
         int bracket1Start = s.indexOf("[");
         int bracket1End = s.indexOf("]");
         int bracket2Start = s.indexOf("[", bracket1End + 1);
         int bracket2End = s.indexOf("]", bracket1End + 1);

         this.processId = Integer.valueOf(s.substring(0, bracket1Start).trim());
         this.timestamp = s.substring(bracket1Start + 1, bracket1End);
         this.transactionId = Integer.valueOf(s.substring(bracket2Start + 1, bracket2End));
         this.fullStatement = s.substring(bracket2End + 2);

         this.statement = null;
         this.prepared = false;

         this.duration = getDuration(this.fullStatement);
         this.parse = isParse(this.fullStatement);
         this.bind = isBind(this.fullStatement);
         this.execute = isExecute(this.fullStatement);
      }

      int getProcessId()
      {
         return processId;
      }

      String getTimestamp()
      {
         return timestamp;
      }

      int getTransactionId()
      {
         return transactionId;
      }

      String getStatement()
      {
         return statement;
      }

      double getDuration()
      {
         return duration;
      }
      
      boolean isPrepared()
      {
         return prepared;
      }
      
      boolean isParse()
      {
         return parse;
      }
      
      boolean isBind()
      {
         return bind;
      }
      
      boolean isExecute()
      {
         return execute;
      }
      
      /**
       * Is parse
       * @param line The log line
       * @return True if parse, otherwise false
       */
      private boolean isParse(String line)
      {
         int offset = line.indexOf("parse <");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            return true;
         }

         offset = line.indexOf("parse S");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = true;
            return true;
         }
      
         return false;
      }
      
      /**
       * Is bind
       * @param line The log line
       * @return True if bind, otherwise false
       */
      private boolean isBind(String line)
      {
         int offset = line.indexOf("bind <");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            return true;
         }

         offset = line.indexOf("bind S");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = true;
            return true;
         }
      
         return false;
      }
      
      /**
       * Is execute
       * @param line The log line
       * @return True if execute, otherwise false
       */
      private boolean isExecute(String line)
      {
         int offset = line.indexOf("execute <");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            return true;
         }
         
         offset = line.indexOf("execute S");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = true;
            return true;
         }
         
         return false;
      }
      
      /**
       * Get the duration of a statement
       * @param line The log line
       * @return The duration
       */
      private double getDuration(String line)
      {
         int start = line.indexOf("duration:");
         if (start != -1)
         {
            int end = line.indexOf("ms", start);
            return Double.valueOf(line.substring(start + 10, end - 1));
         }

         return 0.0;
      }
      
      @Override
      public String toString()
      {
         return processId + " [" + timestamp + "] [" + transactionId + "] " + fullStatement;
      }
   }
}
