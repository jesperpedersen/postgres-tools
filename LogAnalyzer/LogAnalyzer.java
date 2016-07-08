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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Log analyzer
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class LogAnalyzer
{
   /** Raw data:      Process  LogEntry */
   private static Map<Integer, List<LogEntry>> rawData = new TreeMap<>();

   /** Executed:      Process  Statements */
   private static Map<Integer, List<String>> executed = new TreeMap<>();

   /** Statements:    SQL     Count */
   private static Map<String, Integer> statements = new TreeMap<>();

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
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>UPDATE</b></td>");
      l.add("<td>" + String.format("%.2f", ((updateWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td><b>BIND</b></td>");
      l.add("<td>" +  String.format("%.3f", bindTime) + " ms</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>INSERT</b></td>");
      l.add("<td>" + String.format("%.2f", ((insertWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td><b>EXECUTE</b></td>");
      l.add("<td>" +  String.format("%.3f", executeTime) + " ms</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>DELETE</b></td>");
      l.add("<td>" + String.format("%.2f", ((deleteWeight / totalWeight) * 100)) + "%</td>");
      l.add("<td>&nbsp;</td>");
      l.add("<td>&nbsp;</td>");
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

      l.add("<h2>Transactions</h2>");
      l.add("<table>");
      l.add("<tr>");
      l.add("<td>BEGIN</td>");
      l.add("<td>" + statements.get("BEGIN") + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td>COMMIT</td>");
      l.add("<td>" + statements.get("COMMIT") + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td>ROLLBACK</td>");
      l.add("<td>" + statements.get("ROLLBACK") + "</td>");
      l.add("</tr>");
      l.add("</table>");
      l.add("<p>");
      
      l.add("<h2>Interactions</h2>");
      for (Integer processId : executed.keySet())
      {
         List<String> data = executed.get(processId);
         l.add("<a href=\"" + processId + ".html\">" + processId + "</a>(" + data.size() + ")&nbsp;");
         writeInteractionReport(processId, data);
      }
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Write the interaction report
    * @param processId The process id
    * @param data The data
    */
   private static void writeInteractionReport(Integer processId, List<String> data) throws Exception
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

      l.add("<a href=\"" + processId + ".log\">Raw</a>");

      l.add("<h2>Executed</h2>");
      
      l.add("<pre>");
      l.addAll(data);
      l.add("</pre>");

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", processId + ".html"), l);
   }
   
   /**
    * Write run.properties
    */
   private static void writeQueryAnalyzerFile() throws Exception
   {
      List<String> l = new ArrayList<>();

      int select = 1;
      int update = 1;
      int insert = 1;
      int delete = 1;

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
            l.add("query.select." + select + "=" + stmt);
            select++;
         }
         else if (upper.startsWith("UPDATE"))
         {
            l.add("query.update." + update + "=" + stmt);
            update++;
         }
         else if (upper.startsWith("INSERT"))
         {
            l.add("query.insert." + insert + "=" + stmt);
            insert++;
         }
         else if (upper.startsWith("DELETE"))
         {
            l.add("query.delete." + delete + "=" + stmt);
            delete++;
         }
      }

      writeFile(Paths.get("report", "run.properties"), l);
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
         int bracket1Start = s.indexOf("[");
         int bracket1End = s.indexOf("]");
         int bracket2Start = s.indexOf("[", bracket1End + 1);
         int bracket2End = s.indexOf("]", bracket1End + 1);

         Integer processId = Integer.valueOf(s.substring(0, bracket1Start).trim());
         String timestamp = s.substring(bracket1Start + 1, bracket1End);
         Integer transactionId = Integer.valueOf(s.substring(bracket2Start + 1, bracket2End));
         String statement = s.substring(bracket2End + 2);

         LogEntry le = new LogEntry(processId, timestamp, transactionId, statement);

         if (i == 0)
         {
            startDate = le.getTimestamp();
         }
         else if (i == l.size() - 1)
         {
            endDate = le.getTimestamp();
         }
         
         // Raw data insert
         List<LogEntry> lle = rawData.get(processId);
         if (lle == null)
            lle = new ArrayList<>();
         lle.add(le);
         rawData.put(processId, lle);

         if (isParse(statement))
         {
            parseTime += getDuration(statement);
         }
         else if (isBind(statement))
         {
            bindTime += getDuration(statement);
         }
         else if (isExecute(statement))
         {
            executeTime += getDuration(statement);

            int execute = statement.indexOf("execute");
            String stmt = statement.substring(statement.indexOf(":", execute) + 2);
            stmt = stmt.replace('$', '?');

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

            // Executed insert
            List<String> ls = executed.get(processId);
            if (ls == null)
               ls = new ArrayList<>();
            ls.add(stmt);
            executed.put(processId, ls);
         }
      }

      for (Integer proc : rawData.keySet())
      {
         List<String> write = new ArrayList<>();
         List<LogEntry> lle = rawData.get(proc);

         for (LogEntry le : lle)
            write.add(le.toString());

         writeFile(Paths.get("report", proc + ".log"), write);
      }      
   }

   /**
    * Is parse
    * @param line The log line
    * @return True if parse, otherwise false
    */
   private static boolean isParse(String line)
   {
      int offset = line.indexOf("parse <");
      if (offset != -1)
         return true;

      offset = line.indexOf("parse S");
      if (offset != -1)
         return true;
      
      return false;
   }
      
   /**
    * Is bind
    * @param line The log line
    * @return True if bind, otherwise false
    */
   private static boolean isBind(String line)
   {
      int offset = line.indexOf("bind <");
      if (offset != -1)
         return true;

      offset = line.indexOf("bind S");
      if (offset != -1)
         return true;
      
      return false;
   }
      
   /**
    * Is execute
    * @param line The log line
    * @return True if execute, otherwise false
    */
   private static boolean isExecute(String line)
   {
      int offset = line.indexOf("execute <");
      if (offset != -1)
         return true;

      offset = line.indexOf("execute S");
      if (offset != -1)
         return true;
      
      return false;
   }
      
   /**
    * Get the duration of a statement
    * @param line The log line
    * @return The duration
    */
   private static double getDuration(String line)
   {
      int start = line.indexOf("duration:");
      int end = line.indexOf("ms", start);

      return Double.valueOf(line.substring(start + 10, end - 1));
   }
      
   /**
    * Should the statement be filtered from the report
    * @param stmt The statement
    * @return The result
    */
   private static boolean filterStatement(String stmt)
   {
      if ("BEGIN".equals(stmt) || "ROLLBACK".equals(stmt) || "COMMIT".equals(stmt))
         return true;

      return false;
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
      private String statement;
      
      LogEntry(int processId, String timestamp, int transactionId, String statement)
      {
         this.processId = processId;
         this.timestamp = timestamp;
         this.transactionId = transactionId;
         this.statement = statement;
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

      @Override
      public String toString()
      {
         return processId + " [" + timestamp + "] [" + transactionId + "] " + statement;
      }
   }
}
