/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Jesper Pedersen <jesper.pedersen@comcast.net>
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
import java.util.Comparator;
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
   /** Default */
   private static final String DEFAULT = "run";

   /** Default configuration */
   private static final String DEFAULT_CONFIGURATION = "loganalyzer.properties";

   /** Log line type: EOF */
   private static final int EOF = -1;

   /** Log line type: UNKNOWN */
   private static final int UNKNOWN = 0;

   /** Log line type: PANIC */
   private static final int PANIC = 1;

   /** Log line type: FATAL */
   private static final int FATAL = 2;

   /** Log line type: ERROR */
   private static final int ERROR = 3;

   /** Log line type: WARNING */
   private static final int WARNING = 4;

   /** Log line type: INFO */
   private static final int INFO = 5;

   /** Log line type: DEBUG1 */
   private static final int DEBUG1 = 6;

   /** Log line type: DEBUG2 */
   private static final int DEBUG2 = 7;

   /** Log line type: DEBUG3 */
   private static final int DEBUG3 = 8;

   /** Log line type: DEBUG4 */
   private static final int DEBUG4 = 9;

   /** Log line type: DEBUG5 */
   private static final int DEBUG5 = 10;

   /** Log line type: STATEMENT */
   private static final int STATEMENT = 11;

   /** Log line type: DETAIL */
   private static final int DETAIL = 12;

   /** Log line type: LOG */
   private static final int LOG = 13;

   /** Log line type: NOTICE */
   private static final int NOTICE = 14;

   /** Log line type: HINT */
   private static final int HINT = 15;

   /** Log line type: CONTEXT */
   private static final int CONTEXT = 16;

   /** Date format */
   private static DateFormat df;

   /** The configuration */
   private static Properties configuration;

   /** Color 1 */
   private static final String COLOR_1 = "#cce6ff";

   /** Color 2 */
   private static final String COLOR_2 = "#ccffcc";

   /** Color ERROR */
   private static final String COLOR_ERROR = "#ff0000";

   /** Multi database */
   private static boolean multidb;

   /** Keep the raw data */
   private static boolean keepRaw;

   /** Interaction */
   private static boolean interaction;

   /** Histogram count */
   private static int histogramCount;

   /** The file name */
   private static String filename;

   /** The start date */
   private static String startDate;

   /** The end date */
   private static String endDate;

   /** Histogram min */
   private static Map<String, Double> histogramMin = new TreeMap<>();

   /** Histogram max */
   private static Map<String, Double> histogramMax = new TreeMap<>();

   /** Histogram values */
   private static Map<String, List<Double>> histogramValues = new TreeMap<>();

   /** Raw data:      Db          Process  Writer */
   private static Map<String, Map<Integer, BufferedWriter>> rawData = new TreeMap<>();

   /** Data:          Db          Process  LogEntry */
   private static Map<String, Map<Integer, List<LogEntry>>> data = new TreeMap<>();

   /** Statements:    Db          SQL     Count */
   private static Map<String, Map<String, Integer>> statements = new TreeMap<>();

   /** Max time:      Db          SQL     Max time */
   private static Map<String, Map<String, Double>> maxtime = new TreeMap<>();

   /** Total time:    Db          SQL     Time */
   private static Map<String, Map<String, Double>> totaltime = new TreeMap<>();

   /** Query names:   Db          SQL     Name */
   private static Map<String, Map<String, String>> queryNames = new TreeMap<>();

   /** Query samples: Db          SQL     Samples */
   private static Map<String, Map<String, List<QuerySample>>> querySamples = new TreeMap<>();

   /** Total idle in transaction */
   private static Map<String, Long> totalIdleInTransaction = new TreeMap<>();

   /** Idle in transaction: Proc      Ms       Count */
   private static Map<String, TreeMap<Integer, Integer>> mIdleInTransaction = new TreeMap<>();

   /** Wait time:     Process         Ms       Count */
   private static Map<String, TreeMap<Integer, Integer>> waitTime = new TreeMap<>();

   /** Max clients */
   private static Map<String, Integer> maxClients = new TreeMap<>();

   /** Parse time */
   private static Map<String, Double> parseTime = new TreeMap<>();
   
   /** Bind time */
   private static Map<String, Double> bindTime = new TreeMap<>();
   
   /** Execute time */
   private static Map<String, Double> executeTime = new TreeMap<>();
   
   /** Empty time */
   private static Map<String, Double> emptyTime = new TreeMap<>();
   
   /** Number of errors */
   private static int errors = 0;

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
    * Open an append file
    * @param p The file path
    * @return The writer
    */
   private static BufferedWriter appendOpen(Path p) throws Exception
   {
      return Files.newBufferedWriter(p,
                                     StandardOpenOption.CREATE,
                                     StandardOpenOption.WRITE,
                                     StandardOpenOption.APPEND);
   }

   /**
    * Append data to a file
    * @param p The file path
    * @param s The data
    */
   private static void appendWrite(BufferedWriter bw, String s) throws Exception
   {
      bw.write(s, 0, s.length());
      bw.newLine();
   }

   /**
    * Close append file
    * @param bw The writer
    */
   private static void appendClose(BufferedWriter bw) throws Exception
   {
      bw.flush();
      bw.close();
   }

   /**
    * Write index.html
    */
   private static void writeIndex() throws Exception
   {
      if (!multidb)
      {
         writeReport(DEFAULT, "index");
      }
      else
      {
         List<String> l = new ArrayList<>();

         l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
         l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
         l.add("");
         l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
         l.add("<head>");
         l.add("  <title>Log Analysis: " + filename + "</title>");
         l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
         l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"dygraph.min.css\"/>");
         l.add("  <script type=\"text/javascript\" src=\"dygraph.min.js\"></script>");
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
         l.add("</table>");

         l.add("<ul>");
         for (String id : statements.keySet())
         {
            l.add("<li>");
            l.add("<a href=\"" + id + ".html\">" + id + "</a>");
            l.add("</li>");

            writeReport(id, id);
         }
         l.add("</ul>");

         l.add("</body>");
         l.add("</html>");

         writeFile(Paths.get("report", "index.html"), l);
      }
   }

   /**
    * Write report
    * @param id The identifier
    * @param file The file name
    */
   private static void writeReport(String id, String file) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Log Analysis: " + (!multidb ? filename : filename + " - " + id) + "</title>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"dygraph.min.css\"/>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph.min.js\"></script>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Log Analysis</h1>");
      l.add("");

      l.add("<table>");
      if (!multidb)
      {
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
      }
      l.add("<tr>");
      l.add("<td><b>Run</b></td>");
      l.add("<td><a href=\"" + id + ".properties\">Link</a></td>");
      l.add("</tr>");
      l.add("</table>");

      l.add("<p>");

      int selectWeight = 0;
      int updateWeight = 0;
      int insertWeight = 0;
      int deleteWeight = 0;
      double totalWeight = 0;
      
      TreeMap<Integer, List<String>> counts = new TreeMap<>();
      if (statements.get(id) != null)
      {
         for (String stmt : statements.get(id).keySet())
         {
            Integer count = statements.get(id).get(stmt);

            String upper = stmt.toUpperCase();
            if (upper.startsWith("SELECT") || upper.startsWith("WITH"))
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
      }

      totalWeight = selectWeight + updateWeight + insertWeight + deleteWeight;
      
      List<String> interactionLinks = new ArrayList<>();
      Map<Integer, List<LogEntry>> dd = data.get(id);
      for (Integer processId : dd.keySet())
      {
         List<LogEntry> lle = dd.get(processId);
         int executeCount = getExecuteCount(lle);
         if (executeCount > 0)
         {
            String pname = (!multidb ? Integer.toString(processId) : id + "-" + processId);
            if (interaction)
            {
               boolean err = hasError(lle);
               boolean dis = hasDisconnect(lle);
               if (!err && dis)
               {
                  interactionLinks.add("<a href=\"" + pname + ".html\">" + processId + "</a>(" + executeCount + ")&nbsp;");
               }
               else if (err)
               {
                  interactionLinks.add("<a style=\"background-color:#ff0000\" href=\"" + pname + ".html\">" + processId +
                                       "</a>(" + executeCount + ")&nbsp;");
               }
               else
               {
                  interactionLinks.add("<a style=\"background-color:#ffff00\" href=\"" + pname + ".html\">" + processId +
                                       "</a>(" + executeCount + ")&nbsp;");
               }
            }
            writeInteractionReport(id, pname, lle);
         }
      }

      int select = 1;
      int update = 1;
      int insert = 1;
      int delete = 1;
      int padding = 1;

      if (statements.get(id) != null)
         padding = (int)Math.log10(statements.get(id).size()) + 1;

      Map<String, List<QuerySample>> qs = querySamples.get(id);
      if (qs != null)
      {
         for (String sql : qs.keySet())
         {
            boolean include = true;
            String qName = multidb ? id + "-" : "";
            String upper = sql.toUpperCase();
            if (upper.startsWith("SELECT") || upper.startsWith("WITH"))
            {
               qName += "query.select." + String.format("%0" + padding + "d", select);
               select++;
            }
            else if (upper.startsWith("UPDATE"))
            {
               qName += "query.update." + String.format("%0" + padding + "d", update);
               update++;
            }
            else if (upper.startsWith("INSERT"))
            {
               qName += "query.insert." + String.format("%0" + padding + "d", insert);
               insert++;
            }
            else if (upper.startsWith("DELETE"))
            {
               qName += "query.delete." + String.format("%0" + padding + "d", delete);
               delete++;
            }
            else
            {
               include = false;
            }

            if (include)
            {
               Map<String, String> qn = queryNames.get(id);
               if (qn == null)
                  qn = new TreeMap<>();
               qn.put(sql, qName);
               queryNames.put(id, qn);

               writeQueryReport(id, sql, qName);
            }
         }
      }

      l.add("<h2>Overview</h2>");

      l.add("<table>");
      l.add("<tr>");
      l.add("<td><b>SELECT</b></td>");
      l.add("<td>" +  String.format("%.2f", totalWeight != 0.0 ? ((selectWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>PARSE</b></td>");
      l.add("<td>" +  String.format("%.3f", (parseTime.get(id) != null ? parseTime.get(id) : 0.0)) + " ms</td>");
      l.add("<td><b>BEGIN</b></td>");
      if (statements.get(id) != null)
      {
         l.add("<td>" + (statements.get(id).get("BEGIN") != null ? statements.get(id).get("BEGIN") : 0) + "</td>");
      }
      else
      {
         l.add("<td>0</td>");
      }
      l.add("<td><b>MAX CLIENTS</b></td>");
      l.add("<td>" + (maxClients.get(id) != null && maxClients.get(id) != 0 ? maxClients.get(id) : 1) + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>UPDATE</b></td>");
      l.add("<td>" + String.format("%.2f", totalWeight != 0.0 ? ((updateWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>BIND</b></td>");
      l.add("<td>" +  String.format("%.3f", (bindTime.get(id) != null ? bindTime.get(id) : 0.0)) + " ms</td>");
      l.add("<td><b>COMMIT</b></td>");
      if (statements.get(id) != null)
      {
         l.add("<td>" + ((statements.get(id).get("COMMIT") != null ? statements.get(id).get("COMMIT") : 0) +
                         (statements.get(id).get("COMMIT PREPARED") != null ? statements.get(id).get("COMMIT PREPARED") : 0)) + "</td>");
      }
      else
      {
         l.add("<td>0</td>");
      }
      l.add("<td><b>ERRORS</b></td>");
      l.add("<td>" + errors + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>INSERT</b></td>");
      l.add("<td>" + String.format("%.2f", totalWeight != 0.0 ? ((insertWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>EXECUTE</b></td>");
      l.add("<td>" +  String.format("%.3f", (executeTime.get(id) != null ? executeTime.get(id) : 0.0)) + " ms</td>");
      l.add("<td><b>ROLLBACK</b></td>");
      if (statements.get(id) != null)
      {
         l.add("<td>" + ((statements.get(id).get("ROLLBACK") != null ? statements.get(id).get("ROLLBACK") : 0) +
                         (statements.get(id).get("ROLLBACK PREPARED") != null ? statements.get(id).get("ROLLBACK PREPARED") : 0)) + "</td>");
      }
      else
      {
         l.add("<td>0</td>");
      }
      l.add("<td></td>");
      l.add("<td></td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>DELETE</b></td>");
      l.add("<td>" + String.format("%.2f", totalWeight != 0.0 ? ((deleteWeight / totalWeight) * 100) : 0.0) + "%</td>");
      l.add("<td><b>TOTAL</b></td>");
      l.add("<td>" +  String.format("%.3f",
                                    (parseTime.get(id) != null ? parseTime.get(id) : 0.0) +
                                    (bindTime.get(id) != null ? bindTime.get(id) : 0.0) +
                                    (executeTime.get(id) != null ? executeTime.get(id) : 0.0)) + " ms</td>");
      l.add("<td><b>IDLE</b></td>");
      l.add("<td>" +  (totalIdleInTransaction.get(id) != null ? totalIdleInTransaction.get(id) : 0.0) + " ms</td>");
      l.add("<td></td>");
      l.add("<td></td>");
      l.add("</tr>");
      if (emptyTime.get(id) != null && emptyTime.get(id) > 0.0)
      {
         l.add("<tr>");
         l.add("<td></td>");
         l.add("<td></td>");
         l.add("<td><b>EMPTY</b></td>");
         l.add("<td>" + String.format("%.3f", emptyTime.get(id)) + " ms</td>");
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
               if (queryNames.get(id) != null && queryNames.get(id).get(v) != null)
               {
                  sb = sb.append("<a href=\"" + (queryNames.get(id).get(v)) + ".html\" class=\"nohighlight\">" + v + "</a>");
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
      l.addAll(getTimeInfo(id, 20));
      writeTimeReport(id);
      l.add("<p>");
      l.add("<a href=\"" + (multidb ? id + "-" : "") + "totaltime.html\">Report</a>");
      
      l.add("<h2>Max time</h2>");
      l.addAll(getMaxInfo(id, 20));
      writeMaxReport(id);
      l.add("<p>");
      l.add("<a href=\"" + (multidb ? id + "-" : "") + "maxtime.html\">Report</a>");

      if (histogramCount > 0)
      {
         l.add("<h2>Transaction histogram</h2>");
         int[] h = new int[histogramCount];

         Double hMax = histogramMax.get(id);
         if (hMax == null)
            hMax = Double.valueOf(0);
         Double hMin = histogramMin.get(id);
         if (hMin == null)
            hMin = Double.valueOf(0);

         double delta = (hMax - hMin) / (double)histogramCount;
         if (histogramValues.get(id) != null)
         {
            for (Double d : histogramValues.get(id))
            {
               h[Math.min(histogramCount - 1, (int)((d / hMax) * histogramCount))]++;
            }
         }

         l.add("<div id=\"txhistogram\" style=\"width:1024px; height:768px;\">");
         l.add("</div>");

         List<String> txHistogram = new ArrayList<>();
         txHistogram.add("Duration,Count");
         for (int i = 0; i < h.length; i++)
         {
            txHistogram.add((hMin + i * delta) + "," + h[i]);
         }
         writeFile(Paths.get("report", (!multidb ? "transaction.csv" : id + "-transaction.csv")), txHistogram);
      }

      if (interaction)
      {
         l.add("<h2>Interactions</h2>");
         l.addAll(interactionLinks);
      }
      
      l.add("<p>");
      l.add("Generated by <a href=\"https://github.com/jesperpedersen/postgres-tools/tree/master/LogAnalyzer\">LogAnalyzer</a>");

      if (histogramCount > 0)
      {
         l.add("<script type=\"text/javascript\">");
         l.add("   txHistogram = new Dygraph(document.getElementById(\"txhistogram\"),");
         l.add("                             \"" + (!multidb ? "transaction.csv" : id + "-transaction.csv") + "\",");
         l.add("                             {");
         l.add("                               legend: 'always',");
         l.add("                               ylabel: 'Count',");
         l.add("                             }");
         l.add("   );");
         l.add("</script>");
      }

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", file + ".html"), l);
   }

   /**
    * Write the interaction report
    * @param id The database identifier
    * @param pname The process name
    * @param lle The interactions
    */
   private static void writeInteractionReport(String id, String pname, List<LogEntry> lle) throws Exception
   {
      List<String> queries = new ArrayList<>();
      List<String> transactionTimeline = new ArrayList<>();
      double totalDuration = 0.0;
      double totalEmpty = 0.0;
      double duration = 0.0;
      int begin = 0;
      int commit = 0;
      int rollback = 0;
      int error = 0;
      boolean color = true;
      boolean inError = false;
      boolean inTransaction = false;
      double transactionTime = 0.0;
      long pIdleInTransaction = 0;
      long idleInTransaction = 0;
      long pWaitTime = 0;
      LogEntry beginLE = null;
      LogEntry previousLE = null;
      String errorText = "";
      String contextText = "";
      String disconnectText = "";

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

               if (previousLE != null)
               {
                  Integer waitKey = Integer.valueOf((int)(le.timeAsLong() - previousLE.timeAsLong()));
                  if (waitKey.intValue() > 0)
                  {
                     pWaitTime += waitKey;

                     TreeMap<Integer, Integer> countMap = waitTime.get(pname);
                     if (countMap == null)
                        countMap = new TreeMap<>();
                     Integer count = countMap.get(waitKey);
                     if (count == null)
                        count = Integer.valueOf(0);
                     count = Integer.valueOf(count.intValue() + 1);
                     countMap.put(waitKey, count);
                     waitTime.put(pname, countMap);
                  }
               }
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

                  if (previousLE != null)
                  {
                     Integer waitKey = Integer.valueOf((int)(le.timeAsLong() - previousLE.timeAsLong()));
                     if (waitKey.intValue() > 0)
                     {
                        pWaitTime += waitKey;

                        TreeMap<Integer, Integer> countMap = waitTime.get(pname);
                        if (countMap == null)
                           countMap = new TreeMap<>();
                        Integer count = countMap.get(waitKey);
                        if (count == null)
                           count = Integer.valueOf(0);
                        count = Integer.valueOf(count.intValue() + 1);
                        countMap.put(waitKey, count);
                        waitTime.put(pname, countMap);
                     }
                  }
               }
               else
               {
                  if (inTransaction)
                     transactionTime += le.getDuration();
               }
            }
         }
         else if (le.isExecute() || le.isStmt())
         {
            duration += le.getDuration();
            totalDuration += le.getDuration();

            String s = le.getStatement();
            if (s != null)
            {
               if (s.startsWith("BEGIN") && le.isStmt())
               {
                  begin++;
                  inTransaction = true;
                  transactionTime = 0.0;
                  beginLE = le;

                  if (previousLE != null)
                  {
                     Integer waitKey = Integer.valueOf((int)(le.timeAsLong() - previousLE.timeAsLong()));
                     if (waitKey.intValue() > 0)
                     {
                        pWaitTime += waitKey;

                        TreeMap<Integer, Integer> countMap = waitTime.get(pname);
                        if (countMap == null)
                           countMap = new TreeMap<>();
                        Integer count = countMap.get(waitKey);
                        if (count == null)
                           count = Integer.valueOf(0);
                        count = Integer.valueOf(count.intValue() + 1);
                        countMap.put(waitKey, count);
                        waitTime.put(pname, countMap);
                     }
                  }
               }
               else if (s.startsWith("COMMIT"))
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
               Map<String, Double> tt = totaltime.get(id);
               if (tt == null)
                  tt = new TreeMap<>();
               Double time = tt.get(s);
               if (time == null)
               {
                  time = new Double(duration);
               }
               else
               {
                  time = new Double(time.doubleValue() + duration);
               }
               tt.put(s, time);
               totaltime.put(id, tt);

               // Max time
               Map<String, Double> mt = maxtime.get(id);
               if (mt == null)
                  mt = new TreeMap<>();
               time = mt.get(s);
               if (time == null || duration > time.doubleValue())
               {
                  time = new Double(duration);
                  mt.put(s, time);
               }
               maxtime.put(id, mt);

               Map<String, List<QuerySample>> qs = querySamples.get(id);
               if (qs == null)
                  qs = new TreeMap<>();
               if (histogramCount > 0)
               {
                  List<QuerySample> l = qs.get(s);

                  if (l == null)
                     l = new ArrayList<>();

                  l.add(new QuerySample(le.timeAsLong(), duration));
                  qs.put(s, l);
               }
               else
               {
                  if (!qs.containsKey(s))
                     qs.put(s, new ArrayList<>(0));
               }
               querySamples.put(id, qs);

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
                  if (histogramCount > 0)
                  {
                     List<Double> hv = histogramValues.get(id);
                     if (hv == null)
                        hv = new ArrayList<>();
                     hv.add(transactionTime);
                     histogramValues.put(id, hv);

                     Double hm = histogramMin.get(id);
                     if (hm == null)
                        hm = Double.MAX_VALUE;
                     if (transactionTime < hm)
                     {
                        hm = transactionTime;
                        histogramMin.put(id, hm);
                     }

                     hm = histogramMax.get(id);
                     if (hm == null)
                        hm = Double.MIN_VALUE;
                     if (transactionTime > hm)
                     {
                        hm = transactionTime;
                        histogramMax.put(id, hm);
                     }
                  }

                  color = !color;
                  transactionTime = 0.0;
               }

               Integer idleKey = Integer.valueOf((int)idleInTransaction);
               if (idleKey.intValue() > 0)
               {
                  TreeMap<Integer, Integer> countMap = mIdleInTransaction.get(pname);
                  if (countMap == null)
                     countMap = new TreeMap<>();
                  Integer count = countMap.get(idleKey);
                  if (count == null)
                     count = Integer.valueOf(0);
                  count = Integer.valueOf(count.intValue() + 1);
                  countMap.put(idleKey, count);
                  mIdleInTransaction.put(pname, countMap);
               }

               duration = 0.0;
               pIdleInTransaction += idleInTransaction;
               idleInTransaction = 0;
            }
         }
         else if (le.isError())
         {
            inError = true;
            error++;
            errorText = le.getFullStatement().substring(8);
         }
         else if (inError)
         {
            if (le.getFullStatement().startsWith("CONTEXT:"))
            {
               contextText = le.getFullStatement().substring(10);
            }
            else if (le.getFullStatement().startsWith("STATEMENT:"))
            {
               if (interaction)
               {
                  queries.add("<tr style=\"background-color: " + COLOR_ERROR + ";\">");
                  queries.add("<td></td>");
                  queries.add("<td></td>");
                  queries.add("<td>E</td>");
                  queries.add("<td>" + errorText + "<p/>" + contextText + "<p/>" +
                              le.getFullStatement().substring(12).replace('$', '?') + "</td>");
                  queries.add("</tr>");
               }

               inError = false;
            }
         }
         previousLE = le;
      }

      Long tidit = totalIdleInTransaction.get(id);
      if (tidit == null)
         tidit = Long.valueOf(0);
      tidit += pIdleInTransaction;
      totalIdleInTransaction.put(id, tidit);

      if (interaction)
      {
         LogEntry lastEntry = lle.get(lle.size() - 1);
         if (lastEntry.getFullStatement().indexOf("disconnection") != -1)
         {
            int offset = lastEntry.getFullStatement().indexOf("disconnection");
            disconnectText = lastEntry.getFullStatement().substring(offset);
         }

         List<String> l = new ArrayList<>();
         l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
         l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
         l.add("");
         l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
         l.add("<head>");
         l.add("  <title>Log Analysis: " + pname + "</title>");
         l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
         l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"dygraph.min.css\"/>");
         l.add("  <script type=\"text/javascript\" src=\"dygraph.min.js\"></script>");
         l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
         l.add("</head>");
         l.add("<body>");
         l.add("<h1>" + pname + "</h1>");
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
         l.add("<td><b>Wait time</b></td>");
         l.add("<td>" + pWaitTime + " ms</td>");
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
         l.add("<tr>");
         l.add("<td><b>ERROR</b></td>");
         l.add("<td>" + error + "</td>");
         l.add("</tr>");

         if (keepRaw)
         {
            l.add("<tr>");
            l.add("<td><b>Raw</b></td>");
            l.add("<td><a href=\"" + pname + ".log\">Link</a></td>");
            l.add("</tr>");
         }
         l.add("</table>");

         l.add("<h2>Time line</h2>");
         l.add("<div id=\"txtimeline\" style=\"width:1024px; height:768px;\">");
         l.add("</div>");

         l.add("<h2>Idle in transaction</h2>");
         l.add("<div id=\"idleintx\" style=\"width:1024px; height:768px;\">");
         l.add("</div>");

         l.add("<h2>Wait time</h2>");
         l.add("<div id=\"waittime\" style=\"width:1024px; height:768px;\">");
         l.add("</div>");

         l.add("<h2>Executed</h2>");
         l.add("<table border=\"1\">");
         l.addAll(queries);
         l.add("</table>");

         l.add("<p>");
         if (!"".equals(disconnectText))
         {
             l.add(disconnectText);
         }
         else
         {
             l.add("<b>No disconnect event found</b>");
         }

         l.add("<p>");
         l.add("<a href=\"index.html\">Back</a>");
         l.add("<script type=\"text/javascript\">");
         l.add("   txTimeline = new Dygraph(document.getElementById(\"txtimeline\"),");
         l.add("                            \"" + pname + "-transaction.csv\",");
         l.add("                            {");
         l.add("                              legend: 'always',");
         l.add("                              ylabel: 'Duration',");
         l.add("                            }");
         l.add("   );");
         l.add("   idleInTx = new Dygraph(document.getElementById(\"idleintx\"),");
         l.add("                          \"" + pname + "-idle.csv\",");
         l.add("                          {");
         l.add("                            legend: 'always',");
         l.add("                            ylabel: 'Count',");
         l.add("                          }");
         l.add("   );");
         l.add("   waitTime = new Dygraph(document.getElementById(\"waittime\"),");
         l.add("                          \"" + pname + "-wait.csv\",");
         l.add("                          {");
         l.add("                            legend: 'always',");
         l.add("                            ylabel: 'Count',");
         l.add("                          }");
         l.add("   );");
         l.add("</script>");

         l.add("</body>");
         l.add("</html>");

         writeFile(Paths.get("report", pname + ".html"), l);
         writeFile(Paths.get("report", pname + "-transaction.csv"), transactionTimeline);

         List<String> idleReport = new ArrayList<>();
         idleReport.add("Time,Count");
         TreeMap<Integer, Integer> idleCounts = mIdleInTransaction.get(pname);
         if (idleCounts != null)
         {
            Integer max = idleCounts.lastKey();
            if (max != null)
            {
               for (int i = 0; i <= max + 1; i++)
               {
                  Integer val = idleCounts.get(i);
                  if (val == null)
                     val = Integer.valueOf(0);
                  idleReport.add(i + "," + val);
               }
            }
         }
         writeFile(Paths.get("report", pname + "-idle.csv"), idleReport);

         List<String> waitReport = new ArrayList<>();
         waitReport.add("Time,Count");
         TreeMap<Integer, Integer> waitCounts = waitTime.get(pname);
         if (waitCounts != null)
         {
            Integer max = waitCounts.lastKey();
            if (max != null)
            {
               for (int i = 0; i <= max + 1; i++)
               {
                  Integer val = waitCounts.get(i);
                  if (val == null)
                     val = Integer.valueOf(0);
                  waitReport.add(i + "," + val);
               }
            }
         }
         writeFile(Paths.get("report", pname + "-wait.csv"), waitReport);
      }
   }
   
   /**
    * Write <run>.properties
    * @param id The identifier
    */
   private static void writeQueryAnalyzerFile(String id) throws Exception
   {
      List<String> l = new ArrayList<>();
      int select = 0;
      int update = 0;
      int insert = 0;
      int delete = 0;
      int count = 0;
      int total = 0;
      
      l.add("# https://github.com/jesperpedersen/postgres-tools/tree/master/QueryAnalyzer");
      l.add("host=localhost # ChangeMe");
      l.add("port=5432 # ChangeMe");
      l.add("database=test # ChangeMe");
      l.add("user=test # ChangeMe");
      l.add("password=test # ChangeMe");

      if (queryNames.get(id) != null)
      {
         for (Map.Entry<String, String> entry : queryNames.get(id).entrySet())
         {
            if (entry.getValue() != null)
            {
               Integer c = statements.get(id).get(entry.getKey());
               int t = totaltime.get(id).get(entry.getKey()).intValue();

               l.add("#!" + c + "," + t);
               l.add(entry.getValue() + "=" + entry.getKey());

               count += c;
               total += t;

               if (entry.getValue().indexOf("select") != -1)
               {
                  select += c;
               }
               else if (entry.getValue().indexOf("update") != -1)
               {
                  update += c;
               }
               if (entry.getValue().indexOf("insert") != -1)
               {
                  insert += c;
               }
               if (entry.getValue().indexOf("delete") != -1)
               {
                  delete += c;
               }
            }
         }
      }
      l.add("#@" + count + "," + total + "," + select + "," + update + "," + insert + "," + delete);

      writeFile(Paths.get("report", id + ".properties"), l);
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
    * @param id The database identifier
    * @param sql The SQL
    * @param qName The query name
    */
   private static void writeQueryReport(String id, String sql, String qName) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Log Analysis: " + sql + "</title>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"dygraph.min.css\"/>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph.min.js\"></script>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Query</h1>");
      l.add("<table>");
      l.add("<tr>");
      l.add("<td><b>Statement</b></td>");
      l.add("<td>" + sql + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Count</b></td>");
      l.add("<td>" + statements.get(id).get(sql) + "</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Total time</b></td>");
      l.add("<td>" + String.format("%.3f", totaltime.get(id).get(sql)) + " ms</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Max time</b></td>");
      l.add("<td>" + String.format("%.3f", maxtime.get(id).get(sql)) + " ms</td>");
      l.add("</tr>");
      l.add("</table>");
      l.add("<p>");

      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;

      List<QuerySample> qsl = null;
      int[] h = null;
      double delta = 0.0;

      if (histogramCount > 0)
      {
         qsl = querySamples.get(id).get(sql);
         for (QuerySample qs : qsl)
         {
            double d = qs.getDuration();
            if (d < min)
               min = d;

            if (d > max)
               max = d;
         }

         h = new int[histogramCount];
         delta = (max - min) / (double)histogramCount;

         for (QuerySample qs : qsl)
         {
            double d = qs.getDuration();
            h[Math.min(histogramCount - 1, (int)((d / max) * histogramCount))]++;
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
      }

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", qName + ".html"), l);

      if (histogramCount > 0)
      {
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
   }

   /**
    * Write the time report
    * @param id The database identifier
    */
   private static void writeTimeReport(String id) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Log Analysis: Total time</title>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"dygraph.min.css\"/>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph.min.js\"></script>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Total time</h1>");

      l.addAll(getTimeInfo(id, Integer.MAX_VALUE));

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", (multidb ? id + "-" : "") + "totaltime.html"), l);
   }

   /**
    * Write the max report
    * @param id The database identifier
    */
   private static void writeMaxReport(String id) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Log Analysis: Max time</title>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"loganalyzer.css\"/>");
      l.add("  <link rel=\"stylesheet\" type=\"text/css\" href=\"dygraph.min.css\"/>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph.min.js\"></script>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Max time</h1>");

      l.addAll(getMaxInfo(id, Integer.MAX_VALUE));

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", (multidb ? id + "-" : "") + "maxtime.html"), l);
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
         if (le.isExecute() || le.isStmt() || le.isError())
            count++;
      }
      
      return count;
   }
   
   /**
    * Is there an error in the interaction
    * @param lle The interactions
    * @return True, if error; otherwise false
    */
   private static boolean hasError(List<LogEntry> lle)
   {
      for (LogEntry le : lle)
      {
         if (le.isError())
            return true;
      }

      return false;
   }

   /**
    * Is there a disconnect event in the interaction
    * @param lle The interactions
    * @return True, if disconnected; otherwise false
    */
   private static boolean hasDisconnect(List<LogEntry> lle)
   {
      if (lle.size() > 0)
      {
         LogEntry lastEntry = lle.get(lle.size() - 1);
         return lastEntry.getFullStatement().indexOf("disconnection") != -1;
      }

      return false;
   }

   /**
    * Get the type of the log line
    * @param s The string
    * @return The type
    */
   private static int getLogLineType(String s)
   {
      if (s == null || "".equals(s))
         return EOF;

      int bracket1Start = s.indexOf("[");
      int bracket1End = s.indexOf("]");

      if (bracket1Start != -1)
      {
         int bracket2Start = s.indexOf("[", bracket1End + 1);
         int bracket2End = s.indexOf("]", bracket1End + 1);

         if (multidb)
         {
            bracket2Start = s.indexOf("[", bracket2End + 1);
            bracket2End = s.indexOf("]", bracket2End + 1);
         }

         String type = s.substring(bracket2End + 2, s.indexOf(":", bracket2End + 2));

         if ("LOG".equals(type))
         {
            return LOG;
         }
         else if ("STATEMENT".equals(type))
         {
            return STATEMENT;
         }
         else if ("DETAIL".equals(type))
         {
            return DETAIL;
         }
         else if ("NOTICE".equals(type))
         {
            return NOTICE;
         }
         else if ("PANIC".equals(type))
         {
            return PANIC;
         }
         else if ("FATAL".equals(type))
         {
            return FATAL;
         }
         else if ("ERROR".equals(type))
         {
            return ERROR;
         }
         else if ("WARNING".equals(type))
         {
            return WARNING;
         }
         else if ("INFO".equals(type))
         {
            return INFO;
         }
         else if ("DEBUG".equals(type))
         {
            return DEBUG1;
         }
         else if ("DEBUG1".equals(type))
         {
            return DEBUG1;
         }
         else if ("DEBUG2".equals(type))
         {
            return DEBUG2;
         }
         else if ("DEBUG3".equals(type))
         {
            return DEBUG3;
         }
         else if ("DEBUG4".equals(type))
         {
            return DEBUG4;
         }
         else if ("DEBUG5".equals(type))
         {
            return DEBUG5;
         }
         else if ("HINT".equals(type))
         {
            return HINT;
         }
         else if ("CONTEXT".equals(type))
         {
            return CONTEXT;
         }
         else
         {
            System.out.println("Unknown log line type for: " + s);
            System.exit(1);
         }
      }

      return UNKNOWN;
   }

   /**
    * Get the lines for the time report
    */
   private static List<String> getTimeInfo(String id, int cutoff)
   {
      List<String> l = new ArrayList<>();
      TreeMap<Double, List<String>> times = new TreeMap<>();
      int count = 0;

      Map<String, Double> tt = totaltime.get(id);
      if (tt != null)
      {
         for (String stmt : tt.keySet())
         {
            Double d = tt.get(stmt);
            List<String> stmts = times.get(d);
            if (stmts == null)
               stmts = new ArrayList<>();

            stmts.add(stmt);
            times.put(d, stmts);
         }
      }

      l.add("<table border=\"1\">");
      Map<String, String> qn = queryNames.get(id);
      for (Double d : times.descendingKeySet())
      {
         List<String> stmts = times.get(d);
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < stmts.size(); i++)
         {
            if (!filterStatement(stmts.get(i), true) && qn.get(stmts.get(i)) != null)
            {
               sb = sb.append("<a href=\"" + (qn.get(stmts.get(i))) + ".html\" class=\"nohighlight\">" + stmts.get(i) + "</a>");
               if (i < stmts.size() - 1)
                  sb = sb.append("<p>");
            }
            else
            {
               sb = sb.append(stmts.get(i));
               if (i < stmts.size() - 1)
                  sb = sb.append("<p>");
            }
         }

         l.add("<tr>");
         l.add("<td>" + String.format("%.3f", d) + "ms</td>");
         l.add("<td>" + sb.toString() + "</td>");
         l.add("</tr>");
         count++;

         if (count == cutoff)
            break;
      }
      l.add("</table>");
      return l;
   }

   /**
    * Get the lines for the time report
    */
   private static List<String> getMaxInfo(String id, int cutoff)
   {
      List<String> l = new ArrayList<>();
      TreeMap<Double, List<String>> times = new TreeMap<>();
      Map<String, Double> mt = maxtime.get(id);
      int count = 0;

      if (mt != null)
      {
         for (String stmt : mt.keySet())
         {
            Double d = mt.get(stmt);
            List<String> stmts = times.get(d);
            if (stmts == null)
               stmts = new ArrayList<>();

            stmts.add(stmt);
            times.put(d, stmts);
         }
      }

      l.add("<table border=\"1\">");
      Map<String, String> qn = queryNames.get(id);
      for (Double d : times.descendingKeySet())
      {
         List<String> stmts = times.get(d);
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < stmts.size(); i++)
         {
            if (!filterStatement(stmts.get(i), true) && qn.get(stmts.get(i)) != null)
            {
               sb = sb.append("<a href=\"" + (qn.get(stmts.get(i))) + ".html\" class=\"nohighlight\">" + stmts.get(i) + "</a>");
               if (i < stmts.size() - 1)
                  sb = sb.append("<p>");
            }
            else
            {
               sb = sb.append(stmts.get(i));
               if (i < stmts.size() - 1)
                  sb = sb.append("<p>");
            }
         }

         l.add("<tr>");
         l.add("<td>" + String.format("%.3f", d) + "ms</td>");
         l.add("<td>" + sb.toString() + "</td>");
         l.add("</tr>");
         count++;

         if (count == cutoff)
            break;
      }
      l.add("</table>");

      return l;
   }

   /**
    * Process the log
    */
   private static void processLog() throws Exception
   {
      Map<String, Set<Integer>> clients = new TreeMap<>();
      FileReader fr = null;
      LineNumberReader lnr = null;
      String s = null;
      String str = null;
      LogEntry le = null;
      try
      {
         fr = new FileReader(Paths.get(filename).toFile());
         lnr = new LineNumberReader(fr);
         s = lnr.readLine();

         while (s != null)
         {
            str = s;
            s = lnr.readLine();

            while (getLogLineType(s) == UNKNOWN)
            {
               str += " ";
               str += s.trim();
               s = lnr.readLine();
            }

            le = new LogEntry(str);

            if (!removeStatement(le.getStatement()))
            {
               // Raw data insert
               if (keepRaw)
               {
                  Map<Integer, BufferedWriter> rd = rawData.get(le.getDatabase());
                  if (rd == null)
                     rd = new TreeMap<>();

                  BufferedWriter bw = rd.get(le.getProcessId());
                  if (bw == null)
                     bw = appendOpen(Paths.get("report", (!multidb ? "" : le.getDatabase() + "-") + le.getProcessId() + ".log"));

                  if (str != null)
                     appendWrite(bw, str);

                  rd.put(le.getProcessId(), bw);
                  rawData.put(le.getDatabase(), rd);
               }

               // Data insert
               Map<Integer, List<LogEntry>> dm = data.get(le.getDatabase());
               if (dm == null)
                  dm = new TreeMap<>();
               List<LogEntry> lle = dm.get(le.getProcessId());
               if (lle == null)
                  lle = new ArrayList<>();
               lle.add(le);
               dm.put(le.getProcessId(), lle);
               data.put(le.getDatabase(), dm);
         
               if (le.isParse())
               {
                  Double pt = parseTime.get(le.getDatabase());
                  if (pt == null)
                     pt = Double.valueOf(0);
                  pt += le.getDuration();
                  parseTime.put(le.getDatabase(), pt);
               }
               else if (le.isBind())
               {
                  Double bt = bindTime.get(le.getDatabase());
                  if (bt == null)
                     bt = Double.valueOf(0);
                  bt += le.getDuration();
                  bindTime.put(le.getDatabase(), bt);

                  String stmt = le.getStatement();
                  if (stmt == null || "".equals(stmt.trim()))
                  {
                     Double et = emptyTime.get(le.getDatabase());
                     if (et == null)
                        et = Double.valueOf(0);
                     if (lle.size() > 2)
                        et += le.getDuration() + lle.get(lle.size() - 2).getDuration();
                     emptyTime.put(le.getDatabase(), et);
                  }
                  else
                  {
                     if (le.getStatement().equals("BEGIN"))
                     {
                        Set<Integer> c = clients.get(le.getDatabase());
                        if (c == null)
                           c = new TreeSet<>();
                        c.add(le.getProcessId());
                        clients.put(le.getDatabase(), c);
                     }
                  }
               }
               else if (le.isExecute() || le.isStmt())
               {
                  Double et = executeTime.get(le.getDatabase());
                  if (et == null)
                     et = Double.valueOf(0);
                  et += le.getDuration();
                  executeTime.put(le.getDatabase(), et);

                  String stmt = le.getStatement();

                  // Statements insert
                  Map<String, Integer> sc = statements.get(le.getDatabase());
                  if (sc == null)
                     sc = new TreeMap<>();
                  Integer count = sc.get(stmt);
                  if (count == null)
                  {
                     count = Integer.valueOf(1);
                  }
                  else
                  {
                     count = Integer.valueOf(count.intValue() + 1);
                  }
                  sc.put(stmt, count);
                  statements.put(le.getDatabase(), sc);

                  if (le.getStatement().startsWith("COMMIT") || le.getStatement().startsWith("ROLLBACK"))
                  {
                     Set<Integer> c = clients.get(le.getDatabase());
                     if (c == null)
                        c = new TreeSet<>();

                     Integer mc = maxClients.get(le.getDatabase());
                     if (mc == null)
                        mc = Integer.valueOf(0);

                     if (mc < c.size())
                        maxClients.put(le.getDatabase(), c.size());

                     c.remove(le.getProcessId());
                     clients.put(le.getDatabase(), c);
                  }
               }
               else if (le.isError())
               {
                  errors++;
               }
            }

            if (startDate == null)
               startDate = le.getTimestamp();

            endDate = le.getTimestamp();
         }
      }
      catch (Exception e)
      {
         System.err.println("S  : " + s);
         System.err.println("STR: " + str);
         System.err.println("LE : " + le);
         throw e;
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
         for (Map.Entry<String, Map<Integer, BufferedWriter>> e : rawData.entrySet())
         {
            for (Integer proc : e.getValue().keySet())
            {
               BufferedWriter bw = e.getValue().get(proc);
               appendClose(bw);
            }
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
      return filterStatement(stmt, false);
   }

   /**
    * Should the statement be filtered from the report
    * @param stmt The statement
    * @param all All statements
    * @return The result
    */
   private static boolean filterStatement(String stmt, boolean all)
   {
      if (stmt == null || "".equals(stmt))
         return true;

      if ("BEGIN".equals(stmt) ||
          stmt.startsWith("ROLLBACK") ||
          stmt.startsWith("COMMIT") ||
          stmt.startsWith("PREPARE"))
         return true;

      if (all && stmt.startsWith("SET"))
         return true;

      return false;
   }

   /**
    * Should the statement be removed from the report
    * @param stmt The statement
    * @return The result
    */
   private static boolean removeStatement(String stmt)
   {
      if (stmt == null)
         return false;

      if (stmt.startsWith("ANALYZE"))
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
      if (report.exists())
      {
         Files.walk(Paths.get("report"))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
      report.mkdir();

      Files.copy(Paths.get("dygraph.min.js"), Paths.get("report", "dygraph.min.js"), StandardCopyOption.REPLACE_EXISTING);
      Files.copy(Paths.get("dygraph.min.css"), Paths.get("report", "dygraph.min.css"), StandardCopyOption.REPLACE_EXISTING);
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
         multidb = Boolean.valueOf(configuration.getProperty("multidb", "false"));

         setup();

         filename = args[0];
         processLog();
         writeIndex();
         if (!multidb)
         {
            writeQueryAnalyzerFile(DEFAULT);
         }
         else
         {
            for (String id : statements.keySet())
            {
               writeQueryAnalyzerFile(id);
            }
         }
         writeCSS();

         if (errors > 0)
         {
            System.exit(1);
         }

         System.exit(0);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
   }

   /**
    * Log entry
    */
   static class LogEntry
   {
      private int processId;
      private String timestamp;
      private String database;
      private Long time;
      private int transactionId;
      private String fullStatement;
      private String statement;
      private boolean prepared;
      private double duration;
      private boolean error;
      private boolean parse;
      private boolean bind;
      private boolean execute;
      private boolean stmt;
      
      LogEntry(String s)
      {
         int bracket1Start = s.indexOf("[");
         int bracket1End = s.indexOf("]");
         int bracket2Start = s.indexOf("[", bracket1End + 1);
         int bracket2End = s.indexOf("]", bracket1End + 1);
         int bracket3Start = 0;
         int bracket3End = 0;

         if (multidb)
         {
            bracket3Start = s.indexOf("[", bracket2End + 1);
            bracket3End = s.indexOf("]", bracket2End + 1);
         }

         this.processId = Integer.valueOf(s.substring(0, bracket1Start).trim());
         this.timestamp = s.substring(bracket1Start + 1, bracket1End);
         this.time = null;

         if (!multidb)
         {
            this.database = DEFAULT;
            this.transactionId = Integer.valueOf(s.substring(bracket2Start + 1, bracket2End));
            this.fullStatement = s.substring(bracket2End + 2);
         }
         else
         {
            this.database = s.substring(bracket2Start + 1, bracket2End);
            if (this.database == null)
               this.database = "";
            this.transactionId = Integer.valueOf(s.substring(bracket3Start + 1, bracket3End));
            this.fullStatement = s.substring(bracket3End + 2);
         }

         this.statement = null;
         this.prepared = false;

         this.duration = getDuration(this.fullStatement);

         this.error = isError(this.fullStatement);

         this.parse = isParse(this.fullStatement);
         this.bind = false;
         this.execute = false;
         this.stmt = false;

         if (!parse)
         {
            this.bind = isBind(this.fullStatement);
            if (!bind)
            {
               this.execute = isExecute(this.fullStatement);
               if (!execute)
               {
                  this.stmt = isStmt(this.fullStatement);
               }
            }
         }

         if (statement != null)
         {
            if (statement.startsWith("PREPARE TRANSACTION"))
            {
               statement = "PREPARE TRANSACTION";
            }
            else if (statement.startsWith("COMMIT PREPARED"))
            {
               statement = "COMMIT PREPARED";
            }
            else if (statement.startsWith("ROLLBACK PREPARED"))
            {
               statement = "ROLLBACK PREPARED";
            }
         }
      }

      String getFullStatement()
      {
         return fullStatement;
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

      String getDatabase()
      {
         return database;
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
      
      boolean isError()
      {
         return error;
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
      
      boolean isStmt()
      {
         return stmt;
      }

      /**
       * Is error
       * @param line The log line
       * @return True if error, otherwise false
       */
      private boolean isError(String line)
      {
         if (line.startsWith("ERROR:"))
         {
            return true;
         }

         return false;
      }

      /**
       * Is parse
       * @param line The log line
       * @return True if parse, otherwise false
       */
      private boolean isParse(String line)
      {
         int offset = line.indexOf("parse ");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = line.indexOf("<unnamed>") == -1;
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
         int offset = line.indexOf("bind ");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = line.indexOf("<unnamed>") == -1;
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
         int offset = line.indexOf("execute ");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = line.indexOf("<unnamed>") == -1;
            return true;
         }
         
         return false;
      }

      /**
       * Is stmt
       * @param line The log line
       * @return True if stmt, otherwise false
       */
      private boolean isStmt(String line)
      {
         int offset = line.indexOf("statement:");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replace('$', '?');
            prepared = false;
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
