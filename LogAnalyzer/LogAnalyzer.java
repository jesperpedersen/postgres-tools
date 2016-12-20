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
import java.io.FileReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Log analyzer
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class LogAnalyzer
{
   /** Default configuration */
   private static final String DEFAULT_CONFIGURATION = "loganalyzer.properties";

   /** Date format */
   private static DateFormat df;

   /** The configuration */
   private static Properties configuration;

   /** Color 1 */
   private static final String COLOR_1 = "#cce6ff";

   /** Color 2 */
   private static final String COLOR_2 = "#ccffcc";

   /** Keep the raw data */
   private static boolean keepRaw;

   /** Interaction */
   private static boolean interaction;

   /** Histogram count */
   private static int histogramCount;

   /** Histogram min */
   private static double histogramMin = Double.MAX_VALUE;

   /** Histogram max */
   private static double histogramMax = Double.MIN_VALUE;

   /** Histogram values */
   private static List<Double> histogramValues = new ArrayList<>();

   /** Raw data:      Process  Log */
   private static Map<Integer, List<String>> rawData = new TreeMap<>();

   /** Data:          Process  LogEntry */
   private static Map<Integer, List<LogEntry>> data = new TreeMap<>();

   /** Statements:    SQL     Count */
   private static Map<String, Integer> statements = new TreeMap<>();

   /** Max time:      SQL     Max time */
   private static Map<String, Double> maxtime = new TreeMap<>();

   /** Total time:    SQL     Time */
   private static Map<String, Double> totaltime = new TreeMap<>();

   /** Query names:   SQL     Name */
   private static Map<String, String> queryNames = new TreeMap<>();

   /** Query samples: SQL     Samples */
   private static Map<String, List<QuerySample>> querySamples = new TreeMap<>();

   /** Total idle in transaction */
   private static long totalIdleInTransaction = 0;

   /** Max clients */
   private static int maxClients = 0;

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
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined.js\"></script>");
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

      if (histogramCount > 0)
      {
         int qNumber = 1;
         for (String sql : querySamples.keySet())
         {
            String qName = "q" + qNumber;
            queryNames.put(sql, qName);

            writeQueryReport(sql, qName);

            qNumber++;
         }
      }

      l.add("<h2>Overview</h2>");

      l.add("<table>");
      l.add("<tr>");
      l.add("<td><b>SELECT</b></td>");
      l.add("<td>" +  String.format("%.2f", totalWeight != 0.0 ? ((selectWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>PARSE</b></td>");
      l.add("<td>" +  String.format("%.3f", parseTime) + " ms</td>");
      l.add("<td><b>BEGIN</b></td>");
      l.add("<td>" + (statements.get("BEGIN") != null ? statements.get("BEGIN") : 0) + "</td>");
      l.add("<td><b>MAX CLIENTS</b></td>");
      l.add("<td>" + (maxClients != 0 ? maxClients : 1) + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>UPDATE</b></td>");
      l.add("<td>" + String.format("%.2f", totalWeight != 0.0 ? ((updateWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>BIND</b></td>");
      l.add("<td>" +  String.format("%.3f", bindTime) + " ms</td>");
      l.add("<td><b>COMMIT</b></td>");
      l.add("<td>" + (statements.get("COMMIT") != null ? statements.get("COMMIT") : 0) + "</td>");
      l.add("<td></td>");
      l.add("<td></td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>INSERT</b></td>");
      l.add("<td>" + String.format("%.2f", totalWeight != 0.0 ? ((insertWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>EXECUTE</b></td>");
      l.add("<td>" +  String.format("%.3f", executeTime) + " ms</td>");
      l.add("<td><b>ROLLBACK</b></td>");
      l.add("<td>" + (statements.get("ROLLBACK") != null ? statements.get("ROLLBACK") : 0) + "</td>");
      l.add("<td></td>");
      l.add("<td></td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>DELETE</b></td>");
      l.add("<td>" + String.format("%.2f", totalWeight != 0.0 ? ((deleteWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>TOTAL</b></td>");
      l.add("<td>" +  String.format("%.3f", parseTime + bindTime + executeTime) + " ms</td>");
      l.add("<td><b>IDLE</b></td>");
      l.add("<td>" +  totalIdleInTransaction + " ms</td>");
      l.add("<td></td>");
      l.add("<td></td>");
      l.add("</tr>");
      if (emptyTime > 0.0)
      {
         l.add("<tr>");
         l.add("<td></td>");
         l.add("<td></td>");
         l.add("<td><b>EMPTY</b></td>");
         l.add("<td>" + String.format("%.3f", emptyTime) + " ms</td>");
         l.add("<td></td>");
         l.add("<td></td>");
         l.add("<td></td>");
         l.add("<td></td>");
         l.add("</tr>");
      }
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
               if (histogramCount > 0)
               {
                  sb = sb.append("<a href=\"" + (queryNames.get(v)) + ".html\" class=\"nohighlight\">" + v + "</a>");
               }
               else
               {
                  sb = sb.append(v);
               }
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
            if (histogramCount > 0)
            {
               sb = sb.append("<a href=\"" + (queryNames.get(stmts.get(i))) + ".html\" class=\"nohighlight\">" + stmts.get(i) + "</a>");
            }
            else
            {
                  sb = sb.append(stmts.get(i));
            }
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
            if (histogramCount > 0)
            {
               sb = sb.append("<a href=\"" + (queryNames.get(stmts.get(i))) + ".html\" class=\"nohighlight\">" + stmts.get(i) + "</a>");
            }
            else
            {
                  sb = sb.append(stmts.get(i));
            }
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

      l.add("<h2>Transaction histogram</h2>");
      int[] h = new int[histogramCount];
      double delta = (histogramMax - histogramMin) / (double)histogramCount;

      for (Double d : histogramValues)
      {
         h[Math.min(histogramCount - 1, (int)((d / histogramMax) * histogramCount))]++;
      }

      l.add("<div id=\"txhistogram\" style=\"width:1024px; height:768px;\">");
      l.add("</div>");

      List<String> txHistogram = new ArrayList<>();
      txHistogram.add("Duration,Count");
      for (int i = 0; i < h.length; i++)
      {
         txHistogram.add((histogramMin + i * delta) + "," + h[i]);
      }
      writeFile(Paths.get("report", "transaction.csv"), txHistogram);

      if (interaction)
      {
         l.add("<h2>Interactions</h2>");
         l.addAll(interactionLinks);
      }
      
      l.add("<script type=\"text/javascript\">");
      l.add("   txHistogram = new Dygraph(document.getElementById(\"txhistogram\"),");
      l.add("                             \"transaction.csv\",");
      l.add("                             {");
      l.add("                               legend: 'always',");
      l.add("                               ylabel: 'Count',");
      l.add("                             }");
      l.add("   );");
      l.add("</script>");
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
      List<String> transactionTimeline = new ArrayList<>();
      double totalDuration = 0.0;
      double totalEmpty = 0.0;
      double duration = 0.0;
      int begin = 0;
      int commit = 0;
      int rollback = 0;
      boolean color = true;
      boolean inTransaction = false;
      double transactionTime = 0.0;
      long pIdleInTransaction = 0;
      long idleInTransaction = 0;
      LogEntry beginLE = null;

      transactionTimeline.add("Time,Duration");
      
      for (LogEntry le : lle)
      {
         if (le.isParse())
         {
            duration += le.getDuration();
            totalDuration += le.getDuration();

            if ("BEGIN".equals(le.getStatement()))
            {
               begin++;
               inTransaction = true;
               transactionTime = le.getDuration();
               beginLE = le;
            }
            else
            {
               if (inTransaction)
                  transactionTime += le.getDuration();
            }
         }
         else if (le.isBind())
         {
            duration += le.getDuration();
            totalDuration += le.getDuration();
            
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
               totalEmpty += le.getDuration();
               duration = 0.0;
               transactionTime = 0.0;
            }
            else
            {
               if (!inTransaction && "BEGIN".equals(s))
               {
                  begin++;
                  inTransaction = true;
                  transactionTime = le.getDuration();
                  beginLE = le;
               }
               else
               {
                  if (inTransaction)
                     transactionTime += le.getDuration();
               }
            }
         }
         else if (le.isExecute())
         {
            duration += le.getDuration();
            totalDuration += le.getDuration();

            String s = le.getStatement();
            if (s != null)
            {
               if (s.startsWith("COMMIT"))
               {
                  commit++;
                  idleInTransaction = Math.max(0, (le.timeAsLong() - (beginLE.timeAsLong() + (long)Math.ceil(transactionTime + le.getDuration()))));
               }
               else if (s.startsWith("ROLLBACK"))
               {
                  rollback++;
                  idleInTransaction = Math.max(0, (le.timeAsLong() - (beginLE.timeAsLong() + (long)Math.ceil(transactionTime + le.getDuration()))));
               }

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

               if (histogramCount > 0)
               {
                  List<QuerySample> l = querySamples.get(s);

                  if (l == null)
                     l = new ArrayList<>();

                  l.add(new QuerySample(le.timeAsLong(), duration));
                  querySamples.put(s, l);
               }

               if (inTransaction)
                  transactionTime += le.getDuration();

               if (interaction)
               {
                  queries.add("<tr style=\"background-color: " + (color ? COLOR_1 : COLOR_2) + ";\">");
                  if (s.startsWith("COMMIT") || s.startsWith("ROLLBACK"))
                  {
                     StringBuilder sb = new StringBuilder();
                     sb = sb.append("<td>");
                     sb = sb.append("<div class=\"tooltip\">");
                     sb = sb.append(String.format("%.3f", (inTransaction ? transactionTime : duration)));
                     sb = sb.append("<span class=\"tooltiptext\">");
                     sb = sb.append("<table>");
                     sb = sb.append("<tr>");
                     sb = sb.append("<td><b>Start</b></td>");
                     sb = sb.append("<td>" + beginLE.getTimestamp() + "</td>");
                     sb = sb.append("</tr>");
                     sb = sb.append("<tr>");
                     sb = sb.append("<td><b>End</b></td>");
                     sb = sb.append("<td>" + le.getTimestamp() + "</td>");
                     sb = sb.append("</tr>");
                     sb = sb.append("<tr>");
                     sb = sb.append("<td><b>Clock</b></td>");
                     sb = sb.append("<td>" + (le.timeAsLong() - beginLE.timeAsLong()) + " ms</td>");
                     sb = sb.append("</tr>");
                     sb = sb.append("<tr>");
                     sb = sb.append("<td><b>Idle</b></td>");
                     sb = sb.append("<td>" + idleInTransaction + " ms (" + (idleInTransaction + pIdleInTransaction) + " ms)</td>");
                     sb = sb.append("</tr>");
                     sb = sb.append("</table>");
                     sb = sb.append("</span>");
                     sb = sb.append("</div>");
                     sb = sb.append("</td>");

                     queries.add(sb.toString());

                     transactionTimeline.add(le.timeAsLong() + "," + String.format("%.3f", (inTransaction ? transactionTime : duration)));
                  }
                  else
                  {
                     queries.add("<td>" + String.format("%.3f", (inTransaction ? transactionTime : duration)) + "</td>");

                     if (!inTransaction)
                        transactionTimeline.add(le.timeAsLong() + "," + String.format("%.3f", (inTransaction ? transactionTime : duration)));
                  }

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
               pIdleInTransaction += idleInTransaction;
               idleInTransaction = 0;
            }
         }
      }

      totalIdleInTransaction += pIdleInTransaction;

      if (interaction)
      {
         List<String> l = new ArrayList<>();
         l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
         l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
         l.add("");
         l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
         l.add("<head>");
         l.add("  <title>Log Analysis</title>");
         l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
         l.add("  <script type=\"text/javascript\" src=\"dygraph-combined.js\"></script>");
         l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
         l.add("</head>");
         l.add("<body>");
         l.add("<h1>" + processId + "</h1>");
         l.add("");

         l.add("<h2>Overview</h2>");
         l.add("<table>");
         l.add("<tr>");
         l.add("<td><b>Total time</b></td>");
         l.add("<td>" + (lle.get(lle.size() - 1).timeAsLong() - lle.get(0).timeAsLong()) + " ms</td>");
         l.add("</tr>");
         l.add("<tr>");
         l.add("<td><b>Statement time</b></td>");
         l.add("<td>" + String.format("%.3f", totalDuration) + " ms" +
               (totalEmpty > 0.0 ? " (" + String.format("%.3f", totalEmpty) + " ms)" : "")
               + "</td>");
         l.add("</tr>");
         l.add("<tr>");
         l.add("<td><b>Idle in transaction</b></td>");
         l.add("<td>" + pIdleInTransaction + " ms</td>");
         l.add("</tr>");
         l.add("<tr>");
         l.add("<td><b>BEGIN</b></td>");
         l.add("<td>" + begin + "</td>");
         l.add("</tr>");
         l.add("<tr>");
         l.add("<td><b>COMMIT</b></td>");
         l.add("<td>" + commit + "</td>");
         l.add("</tr>");
         l.add("<tr>");
         l.add("<td><b>ROLLBACK</b></td>");
         l.add("<td>" + rollback + "</td>");
         l.add("</tr>");
         l.add("</table>");

         l.add("<h2>Time line</h2>");
         l.add("<div id=\"txtimeline\" style=\"width:1024px; height:768px;\">");
         l.add("</div>");

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
         l.add("<script type=\"text/javascript\">");
         l.add("   txTimeline = new Dygraph(document.getElementById(\"txtimeline\"),");
         l.add("                            \"" + processId + "-transaction.csv\",");
         l.add("                            {");
         l.add("                              legend: 'always',");
         l.add("                              ylabel: 'Duration',");
         l.add("                            }");
         l.add("   );");
         l.add("</script>");

         l.add("</body>");
         l.add("</html>");

         writeFile(Paths.get("report", processId + ".html"), l);
         writeFile(Paths.get("report", processId + "-transaction.csv"), transactionTimeline);
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
    * Write loganalyzer.css
    */
   private static void writeCSS() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add(".nohighlight {");
      l.add("  color: black;");
      l.add("  text-decoration: none;");
      l.add("}");
      l.add("");
      l.add(".tooltip {");
      l.add("  position: relative;");
      l.add("  display: inline-block;");
      l.add("  border-bottom: 1px dotted black;");
      l.add("}");
      l.add("");
      l.add(".tooltip .tooltiptext {");
      l.add("  visibility: hidden;");
      l.add("  width: 300px;");
      l.add("  background-color: #f2f2f2;");
      l.add("  color: #000000;");
      l.add("  text-align: left;");
      l.add("  padding: 5px;");
      l.add("  border-radius: 3px;");
      l.add("  position: absolute;");
      l.add("  z-index: 1;");
      l.add("}");
      l.add("");
      l.add(".tooltip:hover .tooltiptext {");
      l.add("  visibility: visible;");
      l.add("}");

      writeFile(Paths.get("report", "loganalyzer.css"), l);
   }

   /**
    * Write the query report
    * @param sql The SQL
    * @param qName The query name
    */
   private static void writeQueryReport(String sql, String qName) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Log Analysis: " + sql + "</title>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined.js\"></script>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Query</h1>");
      l.add("");
      l.add(sql);
      l.add("<p>");

      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;

      List<QuerySample> qsl = querySamples.get(sql);
      for (QuerySample qs : qsl)
      {
         double d = qs.getDuration();
         if (d < min)
            min = d;

         if (d > max)
            max = d;
      }

      if (min < histogramMin)
         histogramMin = min;

      if (max > histogramMax)
         histogramMax = max;

      int[] h = new int[histogramCount];
      double delta = (max - min) / (double)histogramCount;

      for (QuerySample qs : qsl)
      {
         double d = qs.getDuration();
         h[Math.min(histogramCount - 1, (int)((d / max) * histogramCount))]++;

         histogramValues.add(d);
      }

      l.add("<div id=\"histogram\" style=\"width:1024px; height:768px;\">");
      l.add("</div>");

      l.add("<p>");

      l.add("<div id=\"timeline\" style=\"width:1024px; height:768px;\">");
      l.add("</div>");

      l.add("<script type=\"text/javascript\">");
      l.add("   histogram = new Dygraph(document.getElementById(\"histogram\"),");
      l.add("                           \"" + qName + "-histogram" + ".csv\",");
      l.add("                           {");
      l.add("                             legend: 'always',");
      l.add("                             title: 'Histogram',");
      l.add("                             ylabel: 'Count',");
      l.add("                           }");
      l.add("   );");
      l.add("   timeLine = new Dygraph(document.getElementById(\"timeline\"),");
      l.add("                          \"" + qName + "-time" + ".csv\",");
      l.add("                          {");
      l.add("                            legend: 'always',");
      l.add("                            title: 'Time line',");
      l.add("                            ylabel: 'ms',");
      l.add("                          }");
      l.add("   );");
      l.add("</script>");

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", qName + ".html"), l);

      List<String> csvHistogram = new ArrayList<>();
      csvHistogram.add("Duration,Count");
      for (int i = 0; i < h.length; i++)
      {
         csvHistogram.add((min + i * delta) + "," + h[i]);
      }
      writeFile(Paths.get("report", qName + "-histogram.csv"), csvHistogram);

      List<String> csvTime = new ArrayList<>();
      csvTime.add("Timestamp,Duration");
      for (QuerySample qs : qsl)
      {
         csvTime.add(qs.getTimestamp() + "," + qs.getDuration());
      }
      writeFile(Paths.get("report", qName + "-time.csv"), csvTime);
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
      Set<Integer> clients = new TreeSet<>();
      FileReader fr = null;
      LineNumberReader lnr = null;
      String s = null;
      LogEntry le = null;
      try
      {
         fr = new FileReader(Paths.get(filename).toFile());
         lnr = new LineNumberReader(fr);

         while ((s = lnr.readLine()) != null)
         {
            le = new LogEntry(s);

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
               {
                  emptyTime += le.getDuration() + lle.get(lle.size() - 2).getDuration();
               }
               else
               {
                  if (le.getStatement().equals("BEGIN"))
                  {
                     clients.add(le.getProcessId());
                  }
               }
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

               if (le.getStatement().startsWith("COMMIT") || le.getStatement().startsWith("ROLLBACK"))
               {
                  if (maxClients < clients.size())
                     maxClients = clients.size();

                  clients.remove(le.getProcessId());
               }
            }

            if (startDate == null)
               startDate = le.getTimestamp();
         }

         endDate = le.getTimestamp();
      }
      finally
      {
         if (lnr != null)
            lnr.close();

         if (fr != null)
            fr.close();
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

      Files.copy(Paths.get("dygraph-combined.js"), Paths.get("report", "dygraph-combined.js"), StandardCopyOption.REPLACE_EXISTING);
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
         histogramCount = Integer.valueOf(configuration.getProperty("histogram", "1000"));
         df = new SimpleDateFormat(configuration.getProperty("date_format", "yyyy-MM-dd HH:mm:ss.SSS"));

         setup();

         filename = args[0];
         processLog();
         writeIndex();
         writeQueryAnalyzerFile();
         writeCSS();
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
      private Long time;
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
         this.time = null;
         this.transactionId = Integer.valueOf(s.substring(bracket2Start + 1, bracket2End));
         this.fullStatement = s.substring(bracket2End + 2);

         this.statement = null;
         this.prepared = false;

         this.duration = getDuration(this.fullStatement);

         this.parse = isParse(this.fullStatement);
         this.bind = false;
         this.execute = false;

         if (!parse)
         {
            this.bind = isBind(this.fullStatement);
            if (!bind)
            {
               this.execute = isExecute(this.fullStatement);
            }
         }
      }

      int getProcessId()
      {
         return processId;
      }

      String getTimestamp()
      {
         return timestamp;
      }

      long timeAsLong()
      {
         if (time == null)
         {
            int space = timestamp.indexOf(" ");
            String t = timestamp.substring(0, timestamp.indexOf(" ", space + 1));
            try
            {
               Date date = df.parse(t);
               time = Long.valueOf(date.getTime());
            }
            catch (Exception ex)
            {
               time = Long.valueOf(0L);
            }
         }

         return time.longValue();
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

   /**
    * Query sample
    */
   static class QuerySample
   {
      private long timestamp;
      private double duration;

      QuerySample(long timestamp, double duration)
      {
         this.timestamp = timestamp;
         this.duration = duration;
      }

      /**
       * Get timestamp
       * @return The value
       */
      long getTimestamp()
      {
         return timestamp;
      }

      /**
       * Get duration
       * @return The value
       */
      double getDuration()
      {
         return duration;
      }
   }
}
