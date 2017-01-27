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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * Query analyzer
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class QueryAnalyzer
{
   /** Default configuration */
   private static final String DEFAULT_CONFIGURATION = "queryanalyzer.properties";
   
   /** EXPLAIN (ANALYZE, VERBOSE, BUFFERS ON) */
   private static final String EXPLAIN_ANALYZE_VERBOSE_BUFFERS = "EXPLAIN (ANALYZE, VERBOSE, BUFFERS ON)";

   /** The configuration */
   private static Properties configuration;

   /** Plan count */
   private static int planCount;

   /** Output debug information */
   private static boolean debug;

   /** Current table name */
   private static String currentTableName = null;

   /** Saw an IN expression */
   private static boolean sawIn = false;

   /** IN expression column */
   private static String inExpressionColumn = null;

   /** Data:          Table       Column  Value */        
   private static Map<String, Map<String, Object>> data = new TreeMap<>();
   
   /** Aliases:       Alias   Name */
   private static Map<String, String> aliases = new TreeMap<>();

   /** Planner time:  Query   Time */
   private static Map<String, Double> plannerTimes = new TreeMap<>();

   /** Executor time: Query   Time */
   private static Map<String, Double> executorTimes = new TreeMap<>();

   /** Tables:        Name        Column  Type */
   private static Map<String, Map<String, Integer>> tables = new TreeMap<>();
   
   /** Indexes:       Table       Index   Columns */
   private static Map<String, Map<String, Set<String>>> indexes = new TreeMap<>();
   
   /** Primary key:   Table   Columns */
   private static Map<String, Set<String>> primaryKeys = new TreeMap<>();

   /** ON/IN usage:   Table       Query   Columns */
   private static Map<String, Map<String, Set<String>>> on = new TreeMap<>();

   /** WHERE usage:   Table       Query   Columns */
   private static Map<String, Map<String, List<String>>> where = new TreeMap<>();

   /** SET usage:     Table   Columns */
   private static Map<String, Set<String>> set = new TreeMap<>();
   
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
    * @parma queryIds The query identifiers
    */
   private static void writeIndex(SortedSet<String> queryIds) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Query Analysis</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Query Analysis</h1>");
      l.add("");

      if (configuration.getProperty("url") != null)
      {
         l.add("Ran against \'" + configuration.getProperty("url") + "\' on " + new java.util.Date());
      }
      else
      {
         l.add("Ran against \'" +
               configuration.getProperty("host", "localhost") + ":" +
               Integer.valueOf(configuration.getProperty("port", "5432")) + "/" +
               configuration.getProperty("database") + "\' on " + new java.util.Date());
      }
      l.add("<p>");

      l.add("<h2>Overview</h2>");
      l.add("<ul>");
      l.add("<li><a href=\"tables.html\">Tables</a></li>");
      l.add("<li><a href=\"result.csv\">Times</a></li>");
      l.add("<li><a href=\"hot.html\">HOT</a></li>");
      l.add("</ul>");
      l.add("<p>");
      
      l.add("<h2>Queries</h2>");
      l.add("<ul>");
      for (String q : queryIds)
      {
         l.add("<li><a href=\"" + q + ".html\">" + q +"</a>" +
               (plannerTimes.get(q) != null ? " (" + plannerTimes.get(q) + "ms / " + executorTimes.get(q) + "ms)" : "") +
               "</li>");
      }
      l.add("</ul>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Write tables.html
    */
   private static void writeTables() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Table analysis</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Table analysis</h1>");
      l.add("");

      for (String tableName : tables.keySet())
      {
         l.add("<h2>" + tableName + "</h2>");

         Map<String, Integer> tableData = tables.get(tableName);
         Set<String> pkInfo = primaryKeys.get(tableName);

         l.add("<table>");
         for (String columnName : tableData.keySet())
         {
            l.add("<tr>");
            if (pkInfo.contains(columnName))
            {
               l.add("<td><b>" + columnName + "</b></td>");
               l.add("<td><b>" + getTypeName(tableData.get(columnName)) + "</b></td>");
            }
            else
            {
               l.add("<td>" + columnName + "</td>");
               l.add("<td>" + getTypeName(tableData.get(columnName)) + "</td>");
            }
            l.add("</tr>");
         }
         l.add("</table>");

         l.add("<p>");
         l.add("<u><b>Primary key</b></u>");
         if (pkInfo.size() > 0)
         {
            l.add("<table>");
            for (String columnName : pkInfo)
            {
               l.add("<tr>");
               l.add("<td><b>" + columnName + "</b></td>");
               l.add("<td><b>" + getTypeName(tableData.get(columnName)) + "</b></td>");
               l.add("</tr>");
            }
            l.add("</table>");
         }
         else
         {
            l.add("<p>");
            l.add("None");
         }
         
         Map<String, Set<String>> indexData = indexes.get(tableName);
         l.add("<p>");
         l.add("<u><b>Indexes</b></u>");
         if (indexData.size() > 0)
         {
            l.add("<table>");
            for (String indexName : indexData.keySet())
            {
               l.add("<tr>");
               if (indexData.get(indexName).equals(pkInfo))
               {
                  l.add("<td><b>" + indexName + "</b></td>");
                  l.add("<td><b>" + indexData.get(indexName) + "</b></td>");
               }
               else
               {
                  l.add("<td>" + indexName + "</td>");
                  l.add("<td>" + indexData.get(indexName) + "</td>");
               }
               l.add("</tr>");
            }
            l.add("</table>");
         }
         else
         {
            l.add("<p>");
            l.add("None");
         }

         Map<String, Set<String>> tableOn = on.get(tableName);
         Map<String, List<String>> tableWhere = where.get(tableName);
         Set<String> tableSet = set.get(tableName);

         for (String alias : aliases.keySet())
         {
            String t = aliases.get(alias);
            if (tableName.equals(t))
            {
               Map<String, Set<String>> extra = on.get(alias);
               if (extra != null)
               {
                  if (tableOn == null)
                     tableOn = new TreeMap<>();

                  tableOn.putAll(extra);
               }
            }
         }
         
         if (tableWhere != null || tableOn != null)
         {
            l.add("<p>");
            l.add("<u><b>Suggestions</b></u>");

            try
            {
               l.addAll(suggestionPrimaryKey(tableData, pkInfo, tableOn, tableWhere, tableSet));
            }
            catch (Exception e)
            {
               l.add("(Primary Key) Exception: " + e.getMessage());
            }
            try
            {
               l.addAll(suggestionIndexes(tableOn, tableWhere, tableSet));
            }
            catch (Exception e)
            {
               l.add("(Indexes) Exception: " + e.getMessage());
            }
         }

         if (debug && (tableWhere != null || tableOn != null))
         {
            l.add("<h3>DEBUG</h3>");

            if (tableOn != null)
            {
               l.add("<p>");
               l.add("<u><b>ON</b></u>");
               l.add("<pre>");
               l.add(tableOn.toString());
               l.add("</pre>");
            }

            if (tableWhere != null)
            {
               l.add("<p>");
               l.add("<u><b>WHERE</b></u>");
               l.add("<pre>");
               l.add(tableWhere.toString());
               l.add("</pre>");
            }

            if (tableSet != null)
            {
               l.add("<p>");
               l.add("<u><b>SET</b></u>");
               l.add("<pre>");
               l.add(set.get(tableName).toString());
               l.add("</pre>");
            }
         }
      }

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "tables.html"), l);
   }

   /**
    * Write HTML report
    * @parma queryId The query identifier
    * @param origQuery The original query
    * @param query The executed query
    * @param usedTables The used tables
    * @param plan The plan
    * @param types Types used in the prepared query
    * @param values Values used in the prepared query
    */
   private static void writeReport(String queryId,
                                   String origQuery, String query,
                                   Set<String> usedTables,
                                   String plan,
                                   List<Integer> types,
                                   List<String> values) throws Exception
   {
      int number = -1;

      // Replay integration
      if (queryId.startsWith("query.select") ||
          queryId.startsWith("query.update") ||
          queryId.startsWith("query.delete"))
      {
         int factor = 100;
         String ns = queryId.substring(queryId.lastIndexOf(".") + 1);
         while (ns.charAt(0) == '0')
         {
            ns = ns.substring(1);
            if (ns.charAt(0) == '0')
               factor *= 10;
         }

         number = Integer.valueOf(ns);
         boolean commit = true;
         if (queryId.startsWith("query.select"))
         {
            number += 2 * factor;
         }
         else if (queryId.startsWith("query.update"))
         {
            number += 3 * factor;
            commit = false;
         }
         else
         {
            number += 1 * factor;
            commit = false;
         }

         List<String> cli = new ArrayList<>();
         cli.add("#");
         cli.add("# " + queryId);
         cli.add("#");

         cli.add("P");
         cli.add("BEGIN");
         cli.add("");
         cli.add("");

         cli.add("P");
         cli.add(origQuery);
         if (types.size() > 0)
         {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < types.size(); i++)
            {
               sb = sb.append(types.get(i));
               if (i < types.size() - 1)
                  sb = sb.append("|");
            }
            cli.add(sb.toString());

            sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++)
            {
               String v = values.get(i);

               if (v.startsWith("'") && v.endsWith("'"))
                  v = v.substring(1, v.length() - 1);

               sb = sb.append(v);
               if (i < values.size() - 1)
                  sb = sb.append("|");
            }
            cli.add(sb.toString());
         }
         else
         {
            cli.add("");
            cli.add("");
         }

         cli.add("P");
         if (commit)
         {
            cli.add("COMMIT");
         }
         else
         {
            cli.add("ROLLBACK");
         }
         cli.add("");
         cli.add("");

         writeFile(Paths.get("report", number + ".cli"), cli);
      }

      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>" + queryId + "</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>" + queryId + "</h1>");
      l.add("");

      l.add("<h2>Query</h2>");
      l.add("<pre>");
      l.add(origQuery);
      l.add("</pre>");

      if (query != null && !query.equals(origQuery))
      {
         l.add("<p>");
         l.add("<b>Executed query:</b><br>");
         l.add("<pre>");
         l.add(query);
         l.add("</pre>");
      }

      if (number != -1)
      {
         l.add("<p>");
         l.add("<b>Replay:</b>");
         l.add("<p>");
         l.add("<a href=\"" + number + ".cli\">File</a>");
      }

      l.add("<h2>Plan</h2>");

      if (plan != null && !"".equals(plan))
      {
         l.add("<pre>");
         l.add(plan);
         l.add("</pre>");
      }

      l.add("<h2>Tables</h2>");
      for (String tableName : usedTables)
      {
         Map<String, Integer> tableData = tables.get(tableName);
         Set<String> pkInfo = primaryKeys.get(tableName);
         if (tableData != null)
         {
            l.add("<h3>" + tableName + "</h3>");
            l.add("<table>");
            for (String columnName : tableData.keySet())
            {
               l.add("<tr>");
               if (pkInfo.contains(columnName))
               {
                  l.add("<td><b>" + columnName + "</b></td>");
                  l.add("<td><b>" + getTypeName(tableData.get(columnName)) + "</b></td>");
               }
               else
               {
                  l.add("<td>" + columnName + "</td>");
                  l.add("<td>" + getTypeName(tableData.get(columnName)) + "</td>");
               }
               l.add("</tr>");
            }
            l.add("</table>");
         }
      }
      
      l.add("<h2>Indexes</h2>");
      for (String tableName : usedTables)
      {
         Map<String, Set<String>> indexData = indexes.get(tableName);
         Set<String> pkInfo = primaryKeys.get(tableName);
         if (indexData.size() > 0)
         {
            l.add("<h3>" + tableName + "</h3>");
            l.add("<table>");
            for (String indexName : indexData.keySet())
            {
               l.add("<tr>");
               if (indexData.get(indexName).equals(pkInfo))
               {
                  l.add("<td><b>" + indexName + "</b></td>");
                  l.add("<td><b>" + indexData.get(indexName) + "</b></td>");
               }
               else
               {
                  l.add("<td>" + indexName + "</td>");
                  l.add("<td>" + indexData.get(indexName) + "</td>");
               }
               l.add("</tr>");
            }
            l.add("</table>");
         }
      }
      
      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", queryId + ".html"), l);
   }

   /**
    * Write result.csv
    */
   private static void writeCSV() throws Exception
   {
      List<String> l = new ArrayList<>();

      for (String q : plannerTimes.keySet())
      {
         l.add(q + "," + plannerTimes.get(q) + "," + executorTimes.get(q));
      }

      writeFile(Paths.get("report", "result.csv"), l);
   }

   /**
    * Write hot.html
    */
   private static void writeHOT() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>HOT information</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>HOT information</h1>");
      l.add("");

      //  Table       Column  Indexes
      Map<String, Map<String, Set<String>>> m = new TreeMap<>();

      for (Map.Entry<String, Set<String>> sEntry : set.entrySet())
      {
         Map<String, Set<String>> tIndexes = indexes.get(sEntry.getKey());
         if (tIndexes != null && !tIndexes.isEmpty())
         {
            for (Map.Entry<String, Set<String>> idxEntry : tIndexes.entrySet())
            {
               for (String col : sEntry.getValue())
               {
                  if (idxEntry.getValue().contains(col))
                  {
                     Map<String, Set<String>> d = m.get(sEntry.getKey());
                     if (d == null)
                        d = new TreeMap<>();

                     Set<String> idxs = d.get(col);
                     if (idxs == null)
                        idxs = new TreeSet<>();

                     idxs.add(idxEntry.getKey());
                     d.put(col, idxs);
                     m.put(sEntry.getKey(), d);
                  }
               }
            }
         }
      }

      for (Map.Entry<String, Map<String, Set<String>>> entry : m.entrySet())
      {
         l.add("<h2>" + entry.getKey() + "</h2>");
         l.add("<table>");
         l.add("<tr>");
         l.add("<td><b>Column</b></td>");
         l.add("<td><b>Index(es)</b></td>");
         l.add("</tr>");

         for (Map.Entry<String, Set<String>> te : entry.getValue().entrySet())
         {
            l.add("<tr>");
            l.add("<td>" + te.getKey() + "</td>");

            StringBuilder sb = new StringBuilder();
            Iterator<String> it = te.getValue().iterator();
            while (it.hasNext())
            {
               sb = sb.append(it.next());
               if (it.hasNext())
                  sb = sb.append(", ");
            }
            l.add("<td>" + sb.toString() + "</td>");
            l.add("</tr>");
         }
         l.add("</table>");
      }

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "hot.html"), l);
   }

   /**
    * Process the queries
    * @param c The connection
    * @return The query identifiers
    */
   private static SortedSet<String> processQueries(Connection c) throws Exception
   {
      SortedSet<String> keys = new TreeSet<>();
      for (String key : configuration.stringPropertyNames())
      {
         if (key.startsWith("query"))
            keys.add(key);
      }
      
      for (String key : keys)
      {
         String origQuery = configuration.getProperty(key);
         String query = origQuery;
         String plan = "";
         List<Integer> types = new ArrayList<>();
         List<String> values = new ArrayList<>();

         try
         {
            if (query.indexOf("?") != -1)
               query = rewriteQuery(c, key, query, types, values);

            if (query != null)
            {
               for (int i = 0; i < planCount; i++)
                  executeStatement(c, EXPLAIN_ANALYZE_VERBOSE_BUFFERS + " " + query, false);
                  
               List<String> l = executeStatement(c, EXPLAIN_ANALYZE_VERBOSE_BUFFERS + " " + query);
               for (String s : l)
               {
                  plan += s;
                  plan += "\n";

                  if (s.startsWith("Planning time:"))
                  {
                     int index = s.indexOf(" ", 15);
                     plannerTimes.put(key, Double.valueOf(s.substring(15, index)));
                  }
                  else if (s.startsWith("Execution time:"))
                  {
                     int index = s.indexOf(" ", 16);
                     executorTimes.put(key, Double.valueOf(s.substring(16, index)));
                  }
               }
            }

            Set<String> usedTables = getUsedTables(c, origQuery);
            
            writeReport(key, origQuery, query, usedTables, plan, types, values);
         }
         catch (Exception e)
         {
            System.out.println("Original query: " + origQuery);
            System.out.println("Data: " + data);
            throw e;
         }
      }

      return keys;
   }

   /**
    * Suggestion: Primary key
    * @param tableData The data types of the table
    * @param pkInfo The primary key of the table
    * @param tableOn The columns accessed in ON per query
    * @param tableWhere The columns accessed in WHERE per query
    * @param tableSet The columns accessed in SET
    * @return The report data
    */
   private static List<String> suggestionPrimaryKey(Map<String, Integer> tableData,
                                                    Set<String> pkInfo,
                                                    Map<String, Set<String>> tableOn,
                                                    Map<String, List<String>> tableWhere,
                                                    Set<String> tableSet)
   {
      List<String> result = new ArrayList<>();

      SortedSet<String> pkColumns = new TreeSet<>();

      if (tableOn != null)
         for (Set<String> s : tableOn.values())
            pkColumns.addAll(s);

      if (tableWhere != null)
         for (List<String> l : tableWhere.values())
            pkColumns.addAll(l);
      
      if (tableSet != null)
         pkColumns.removeAll(tableSet);

      if (pkColumns.size() > 0 && tableData != null)
      {
         result.add("<p>");
         result.add("<b><u>Primary key</u></b>");
         result.add("<table>");
         
         for (String columnName : pkColumns)
         {
            result.add("<tr>");
            result.add("<td><b>" + columnName + "</b></td>");
            result.add("<td><b>" + getTypeName(tableData.get(columnName)) + "</b></td>");
            result.add("</tr>");
         }

         result.add("</table>");
         result.add("<p>");

         if (pkColumns.equals(pkInfo))
         {
            result.add("Current primary key equal: <b>Yes</b>");
         }
         else
         {
            result.add("Current primary key equal: <b>No</b>");
         }
      }

      return result;
   }
   
   /**
    * Suggestion: Indexes
    * @param tableOn The columns accessed in ON per query
    * @param tableWhere The columns accessed in WHERE per query
    * @param tableSet The columns accessed in SET
    * @return The report data
    */
   private static List<String> suggestionIndexes(Map<String, Set<String>> tableOn,
                                                 Map<String, List<String>> tableWhere,
                                                 Set<String> tableSet)
   {
      List<String> result = new ArrayList<>();
      Set<String> suggested = new HashSet<>();

      result.add("<p>");
      result.add("<b><u>Indexes</u></b>");
      result.add("<table>");

      int idx = 1;

      if (tableOn != null)
      {
         for (Set<String> s : tableOn.values())
         {
            for (String columnName : s)
            {
               if (!suggested.contains(columnName))
               {
                  result.add("<tr>");
                  result.add("<td>IDX" + idx + "</td>");
                  result.add("<td>" + columnName + "</td>");
                  result.add("</tr>");
                  suggested.add(columnName);
                  idx++;
               }
            }
         }
      }

      if (tableWhere != null)
      {
         for (List<String> l : tableWhere.values())
         {
            String columnName = l.get(0);

            if (!suggested.contains(columnName))
            {
               result.add("<tr>");
               result.add("<td>IDX" + idx + "</td>");
               result.add("<td>" + columnName + "</td>");
               result.add("</tr>");
               suggested.add(columnName);
               idx++;
            }
         }
      }

      result.add("</table>");

      return result;
   }
   
   /**
    * Rewrite the query
    * @param c The connection
    * @param queryId The query id
    * @param query The query
    * @param types Types used in the prepared query
    * @param values Values used in the prepared query
    * @return The new query
    */
   private static String rewriteQuery(Connection c, String queryId, String query,
                                      List<Integer> types, List<String> values) throws Exception
   {
      net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil.parse(query);
      
      if (s instanceof Select)
      {
         Select select = (Select)s;
         Map<String, Set<String>> extraIndexes = new TreeMap<>();
         sawIn = false;
         
         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private Column currentColumn = null;
            
            @Override
            public void visit(Column column)
            {
               currentColumn = column;
               this.getBuffer().append(column);
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               if (sawIn)
                  return;

               try
               {
                  String data = getData(c, currentColumn);
                  Integer type = getType(c, currentColumn, query);

                  values.add(data);
                  types.add(type);

                  this.getBuffer().append(data);
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         };

         SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer)
         {
            @Override
            public void visit(Table table)
            {
               currentTableName = table.getName();
               
               if (table.getAlias() != null && !table.getAlias().getName().equals(""))
                  aliases.put(table.getAlias().getName(), table.getName());

               this.getBuffer().append(table);
            }
         };
         expressionDeParser.setSelectVisitor(deparser);
         expressionDeParser.setBuffer(buffer);

         if (select.getSelectBody() instanceof PlainSelect)
         {
            PlainSelect plainSelect = (PlainSelect)select.getSelectBody();

            ExpressionDeParser extraIndexExpressionDeParser = new ExpressionDeParser()
            {
               @Override
               public void visit(Column column)
               {
                  String tableName = null;
                  if (column.getTable() != null)
                     tableName = column.getTable().getName();

                  if (tableName == null)
                  {
                     List<String> tables = new TablesNamesFinder().getTableList(s);
                     if (tables != null && tables.size() == 1)
                        tableName = tables.get(0);
                  }
                  
                  if (tableName != null)
                  {
                     Set<String> cols = extraIndexes.get(tableName);
                     if (cols == null)
                        cols = new HashSet<>();
                  
                     cols.add(column.getColumnName());
                     extraIndexes.put(tableName, cols);
                  }
               }

               @Override
               public void visit(InExpression inExpression)
               {
                  sawIn = true;
                  inExpressionColumn = null;

                  ExpressionDeParser inExpressionDeParser = new ExpressionDeParser()
                  {
                     @Override
                     public void visit(Column column)
                     {
                        inExpressionColumn = column.getColumnName().toLowerCase();
                     }
                  };

                  inExpression.getLeftExpression().accept(inExpressionDeParser);

                  String tableName = null;
                  List<String> tables = new TablesNamesFinder().getTableList(s);
                  if (tables != null && tables.size() == 1)
                     tableName = tables.get(0);
                  
                  if (tableName != null && inExpressionColumn != null)
                  {
                     Map<String, Set<String>> m = on.get(tableName);
                     if (m == null)
                        m = new TreeMap<>();

                     Set<String> s = m.get(queryId);
                     if (s == null)
                        s = new TreeSet<>();
                  
                     s.add(inExpressionColumn.toLowerCase());
                     m.put(queryId, s);
                     on.put(tableName, m);
                  }
               }
            };

            if (plainSelect.getJoins() != null)
            {
               for (Join join : plainSelect.getJoins())
               {
                  join.getOnExpression().accept(new JoinVisitor(queryId, s));
               }
            }
            
            if (plainSelect.getWhere() != null)
            {
               plainSelect.getWhere().accept(extraIndexExpressionDeParser);
            }
            
            if (plainSelect.getLimit() != null)
            {
               Limit limit = new Limit();
               limit.setRowCount(1L);
               plainSelect.setLimit(limit);

               values.add(Integer.toString(1));
               types.add(Integer.valueOf(Types.INTEGER));
            }

            select.setSelectBody(plainSelect);
         }

         select.getSelectBody().accept(deparser);

         for (String tableName : extraIndexes.keySet())
         {
            Set<String> vals = extraIndexes.get(tableName);
            
            if (aliases.containsKey(tableName))
               tableName = aliases.get(tableName);

            Map<String, List<String>> m = where.get(tableName);
            if (m == null)
               m = new TreeMap<>();

            List<String> l = m.get(queryId);
            if (l == null)
               l = new ArrayList<>();

            for (String col : vals)
               l.add(col.toLowerCase());

            m.put(queryId, l);
            where.put(tableName, m);
         }

         if (!sawIn)
            return buffer.toString();
      }
      else if (s instanceof Update)
      {
         Update update = (Update)s;

         for (Table table : update.getTables())
         {
            initTableData(c, table.getName());
            currentTableName = table.getName();
         }

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private Column currentColumn = null;
            
            @Override
            public void visit(Column column)
            {
               currentColumn = column;

               boolean isSET = false;
               for (Column c : update.getColumns())
               {
                  if (c.getColumnName().equals(column.getColumnName()))
                     isSET = true;
               }

               if (isSET)
               {
                  Set<String> s = set.get(currentTableName);
                  if (s == null)
                     s = new TreeSet<>();

                  s.add(column.getColumnName().toLowerCase());
                  set.put(currentTableName, s);
               }
               else
               {
                  Map<String, List<String>> m = where.get(currentTableName);
                  if (m == null)
                     m = new TreeMap<>();

                  List<String> l = m.get(queryId);
                  if (l == null)
                     l = new ArrayList<>();

                  l.add(column.getColumnName().toLowerCase());
                  m.put(queryId, l);
                  where.put(currentTableName, m);
               }

               this.getBuffer().append(column);
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               try
               {
                  String data = getData(c, currentColumn);
                  Integer type = getType(c, currentColumn, query);

                  values.add(data);
                  types.add(type);

                  this.getBuffer().append(data);
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         };

         SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer)
         {
            @Override
            public void visit(Table table)
            {
               currentTableName = table.getName();
               
               if (table.getAlias() != null && !table.getAlias().getName().equals(""))
                  aliases.put(table.getAlias().getName(), table.getName());

               this.getBuffer().append(table);
            }
         };
         expressionDeParser.setSelectVisitor(selectDeParser);
         expressionDeParser.setBuffer(buffer);

         net.sf.jsqlparser.util.deparser.UpdateDeParser updateDeParser =
            new net.sf.jsqlparser.util.deparser.UpdateDeParser(expressionDeParser, selectDeParser, buffer);

         net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
            new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer)
         {
            @Override
            public void visit(Update update)
            {
               updateDeParser.deParse(update);
            }
         };
         
         update.accept(statementDeParser);
         
         return buffer.toString();
      }
      else if (s instanceof Delete)
      {
         Delete delete = (Delete)s;
         sawIn = false;
         
         initTableData(c, delete.getTable().getName());
         currentTableName = delete.getTable().getName();

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private int index = 0;
            private Column currentColumn = null;
            
            @Override
            public void visit(Column column)
            {
               currentColumn = column;

               Map<String, List<String>> m = where.get(currentTableName);
               if (m == null)
                  m = new TreeMap<>();

               List<String> l = m.get(queryId);
               if (l == null)
                  l = new ArrayList<>();

               l.add(column.getColumnName().toLowerCase());
               m.put(queryId, l);
               where.put(currentTableName, m);

               this.getBuffer().append(column);
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               try
               {
                  String data = getData(c, currentColumn);
                  Integer type = getType(c, currentColumn, query);

                  values.add(data);
                  types.add(type);

                  this.getBuffer().append(data);
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }

            @Override
            public void visit(InExpression inExpression)
            {
               sawIn = true;
               inExpressionColumn = null;

               ExpressionDeParser inExpressionDeParser = new ExpressionDeParser()
               {
                  @Override
                  public void visit(Column column)
                  {
                     inExpressionColumn = column.getColumnName().toLowerCase();
                  }
               };

               inExpression.getLeftExpression().accept(inExpressionDeParser);

               if (currentTableName != null && inExpressionColumn != null)
               {
                  Map<String, List<String>> m = where.get(currentTableName);
                  if (m == null)
                     m = new TreeMap<>();

                  List<String> l = m.get(queryId);
                  if (l == null)
                     l = new ArrayList<>();

                  l.add(inExpressionColumn.toLowerCase());
                  m.put(queryId, l);
                  where.put(currentTableName, m);
               }
            }
         };
         expressionDeParser.setBuffer(buffer);

         net.sf.jsqlparser.util.deparser.DeleteDeParser deleteDeParser =
            new net.sf.jsqlparser.util.deparser.DeleteDeParser(expressionDeParser, buffer);

         net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
            new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer)
         {
            @Override
            public void visit(Delete delete)
            {
               deleteDeParser.deParse(delete);
            }
         };
         
         delete.accept(statementDeParser);

         if (!sawIn)
            return buffer.toString();
      }

      System.out.println("Unsupported query: " + s);
      return null;
   }

   /**
    * Get data
    * @param c The connection
    * @param column The column
    * @return The value
    */
   private static String getData(Connection c, Column column) throws Exception
   {
      String tableName = column.getTable().getName();

      if (tableName != null && aliases.containsKey(tableName))
         tableName = aliases.get(tableName);

      if (tableName == null)
         tableName = currentTableName;
      
      Map<String, Object> values = data.get(tableName);

      if (values == null)
         values = initTableData(c, tableName);
      
      Object o = values.get(column.getColumnName().toUpperCase());

      if (needsQuotes(o))
         return "'" + o.toString() + "'";
      
      return o.toString();
   }

   /**
    * Get type
    * @param c The connection
    * @param column The column
    * @param query The query
    * @return The value
    */
   private static Integer getType(Connection c, Column column, String query) throws Exception
   {
      String tableName = column.getTable().getName();

      if (tableName != null && aliases.containsKey(tableName))
         tableName = aliases.get(tableName);

      if (tableName == null)
         tableName = currentTableName;

      Map<String, Integer> tableData = tables.get(tableName);

      if (tableData == null)
      {
         getUsedTables(c, query);
         tableData = tables.get(tableName);
      }

      return tableData.get(column.getColumnName().toLowerCase());
   }

   /**
    * Init data for a table
    * @param c The connection
    * @param tableName The name of the table
    * @return The values
    */
   private static Map<String, Object> initTableData(Connection c, String tableName) throws Exception
   {
      Map<String, Object> values = new TreeMap<>();

      Statement stmt = null;
      ResultSet rs = null;
      String query = "SELECT * FROM " + tableName + " LIMIT 1";
      try
      {
         stmt = c.createStatement();
         stmt.execute(query);
         rs = stmt.getResultSet();

         if (rs.next())
         {
            ResultSetMetaData rsmd = rs.getMetaData();

            for (int i = 1; i <= rsmd.getColumnCount(); i++)
            {
               String columnName = rsmd.getColumnName(i);
               int columnType = rsmd.getColumnType(i);

               columnName = columnName.toUpperCase();
               values.put(columnName, getResultSetValue(rs, i, columnType));
            }
         }

         data.put(tableName, values);
         return values;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Exception e)
            {
               // Nothing to do
            }
         }
         if (stmt != null)
         {
            try
            {
               stmt.close();
            }
            catch (Exception e)
            {
               // Nothing to do
            }
         }
      }
   }
   
   /**
    * Get value from ResultSet
    * @param rs The ResultSet
    * @param col The column
    * @param type The type
    * @return The value
    */
   private static Object getResultSetValue(ResultSet rs, int col, int type) throws Exception
   {
      switch (type)
      {
         case Types.BINARY:
            return rs.getBytes(col);
         case Types.BIT:
            return Boolean.valueOf(rs.getBoolean(col));
         case Types.BIGINT:
            return Long.valueOf(rs.getLong(col));
         case Types.BOOLEAN:
            return Boolean.valueOf(rs.getBoolean(col));
         case Types.CHAR:
            return rs.getString(col);
         case Types.DATE:
            java.sql.Date d = rs.getDate(col);
            if (d == null)
               d = new java.sql.Date(System.currentTimeMillis());
            return d;
         case Types.DECIMAL:
            return rs.getBigDecimal(col);
         case Types.DOUBLE:
            return Double.valueOf(rs.getDouble(col));
         case Types.FLOAT:
            return Double.valueOf(rs.getDouble(col));
         case Types.INTEGER:
            return Integer.valueOf(rs.getInt(col));
         case Types.LONGVARBINARY:
            return rs.getBytes(col);
         case Types.LONGVARCHAR:
            return rs.getString(col);
         case Types.NUMERIC:
            return rs.getBigDecimal(col);
         case Types.REAL:
            return Float.valueOf(rs.getFloat(col));
         case Types.SMALLINT:
            return Short.valueOf(rs.getShort(col));
         case Types.TIME:
         case Types.TIME_WITH_TIMEZONE:
            java.sql.Time t = rs.getTime(col);
            if (t == null)
               t = new java.sql.Time(System.currentTimeMillis());
            return t;
         case Types.TIMESTAMP:
         case Types.TIMESTAMP_WITH_TIMEZONE:
            java.sql.Timestamp ts = rs.getTimestamp(col);
            if (ts == null)
               ts = new java.sql.Timestamp(System.currentTimeMillis());
            return ts;
         case Types.TINYINT:
            return Short.valueOf(rs.getShort(col));
         case Types.VARBINARY:
            return rs.getBytes(col);
         case Types.VARCHAR:
            return rs.getString(col);
         default:
            System.out.println("Unsupported value: " + type);
            break;
      }

      return null;
   }
   
   /**
    * Needs quotes
    * @param o The object
    */
   private static boolean needsQuotes(Object o)
   {
      if (o instanceof String ||
          o instanceof java.sql.Date ||
          o instanceof java.sql.Time ||
          o instanceof java.sql.Timestamp)
         return true;

      return false;
   }
   
   /**
    * Get type name
    * @param type
    * @return The name
    */
   private static String getTypeName(int type)
   {
      switch (type)
      {
         case Types.BINARY:
            return "BINARY";
         case Types.BIT:
            return "BIT";
         case Types.BIGINT:
            return "BIGINT";
         case Types.BOOLEAN:
            return "BOOLEAN";
         case Types.CHAR:
            return "CHAR";
         case Types.DATE:
            return "DATE";
         case Types.DECIMAL:
            return "DECIMAL";
         case Types.DOUBLE:
            return "DOUBLE";
         case Types.FLOAT:
            return "FLOAT";
         case Types.INTEGER:
            return "INTEGER";
         case Types.LONGVARBINARY:
            return "LONGVARBINARY";
         case Types.LONGVARCHAR:
            return "LONGVARCHAR";
         case Types.NUMERIC:
            return "NUMERIC";
         case Types.REAL:
            return "REAL";
         case Types.SMALLINT:
            return "SMALLINT";
         case Types.TIME:
            return "TIME";
         case Types.TIME_WITH_TIMEZONE:
            return "TIME_WITH_TIMEZONE";
         case Types.TIMESTAMP:
            return "TIMESTAMP";
         case Types.TIMESTAMP_WITH_TIMEZONE:
            return "TIMESTAMP_WITH_TIMEZONE";
         case Types.TINYINT:
            return "TINYINT";
         case Types.VARBINARY:
            return "VARBINARY";
         case Types.VARCHAR:
            return "VARCHAR";
         default:
            System.out.println("Unsupported value: " + type);
            break;
      }

      return null;
   }

   /**
    * Execute statement
    * @param c The connection
    * @param s The string
    * @return The result
    */
   private static List<String> executeStatement(Connection c, String s) throws Exception
   {
      return executeStatement(c, s, true);
   }

   /**
    * Execute statement
    * @param c The connection
    * @param s The string
    * @param parseResult Parse the result
    * @return The result
    */
   private static List<String> executeStatement(Connection c, String s, boolean parseResult) throws Exception
   {
      List<String> result = new ArrayList<>();
      Statement stmt = null;
      ResultSet rs = null;
      boolean autoCommit = true;
      try
      {
         autoCommit = c.getAutoCommit();
         c.setAutoCommit(false);

         stmt = c.createStatement();
         stmt.execute(s);

         if (parseResult)
         {
            rs = stmt.getResultSet();
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int numberOfColumns = rsmd.getColumnCount();
            int[] types = new int[numberOfColumns];

            for (int i = 1; i <= numberOfColumns; i++)
            {
               types[i - 1] = rsmd.getColumnType(i);
            }

            while (rs.next())
            {
               StringBuilder sb = new StringBuilder();

               for (int i = 1; i <= numberOfColumns; i++)
               {
                  int type = types[i - 1];
                  sb = sb.append(getResultSetValue(rs, i, type));

                  if (i < numberOfColumns)
                     sb = sb.append(",");
               }

               result.add(sb.toString());
            }
         }

         c.rollback();
         
         return result;
      }
      catch (Exception e)
      {
         System.out.println("Query: " + s);
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Exception e)
            {
               // Nothing to do
            }
         }
         if (stmt != null)
         {
            try
            {
               stmt.close();
            }
            catch (Exception e)
            {
               // Nothing to do
            }
         }
         try
         {
            c.setAutoCommit(autoCommit);
         }
         catch (Exception e)
         {
            // Nothing to do
         }
      }
   }

   /**
    * Get the used tables for a query
    * @param c The connection
    * @param query The query
    * @return The names
    */
   private static Set<String> getUsedTables(Connection c, String query) throws Exception
   {
      Set<String> result = new HashSet<>();

      net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil.parse(query);
      result.addAll(new TablesNamesFinder().getTableList(s));
      
      for (String tableName : result)
      {
         if (!tables.containsKey(tableName))
         {
            Map<String, Integer> tableData = new TreeMap<>();
            ResultSet rs = null;
            try
            {
               DatabaseMetaData dmd = c.getMetaData();

               rs = dmd.getColumns(null, null, tableName.toLowerCase(), "");
               while (rs.next())
               {
                  String columnName = rs.getString("COLUMN_NAME");
                  int dataType = rs.getInt("DATA_TYPE");
                  tableData.put(columnName, dataType);
               }
            
               tables.put(tableName, tableData);
            }
            finally
            {
               if (rs != null)
               {
                  try
                  {
                     rs.close();
                  }
                  catch (Exception e)
                  {
                     // Ignore
                  }
               }
            }

            rs = null;
            try
            {
               DatabaseMetaData dmd = c.getMetaData();
               Map<String, Set<String>> indexInfo = new TreeMap<>();

               rs = dmd.getIndexInfo(null, null, tableName.toLowerCase(), false, false);
               while (rs.next())
               {
                  String indexName = rs.getString("INDEX_NAME");
                  String columnName = rs.getString("COLUMN_NAME");

                  Set<String> existing = indexInfo.get(indexName);
                  if (existing == null)
                     existing = new TreeSet<>();

                  existing.add(columnName);
                  indexInfo.put(indexName, existing);
               }

               indexes.put(tableName, indexInfo);
            }
            finally
            {
               if (rs != null)
               {
                  try
                  {
                     rs.close();
                  }
                  catch (Exception e)
                  {
                     // Ignore
                  }
               }
            }

            rs = null;
            try
            {
               DatabaseMetaData dmd = c.getMetaData();
               Set<String> pkInfo = new TreeSet<>();

               rs = dmd.getPrimaryKeys(null, null, tableName.toLowerCase());
               while (rs.next())
               {
                  String columnName = rs.getString("COLUMN_NAME");
                  pkInfo.add(columnName);
               }

               primaryKeys.put(tableName, pkInfo);
            }
            finally
            {
               if (rs != null)
               {
                  try
                  {
                     rs.close();
                  }
                  catch (Exception e)
                  {
                     // Ignore
                  }
               }
            }
         }
      }
      
      return result;
   }
   
   /**
    * Read the configuration (queryanalyzer.properties)
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
      Connection c = null;
      try
      {
         if (args.length != 0 && args.length != 2)
         {
            System.out.println("Usage: QueryAnalyzer [-c <configuration.properties>]");
            return;
         }

         setup();

         String config = DEFAULT_CONFIGURATION;

         if (args.length > 0)
         {
            int position = 0;
            if ("-c".equals(args[position]))
            {
               position++;
               config = args[position];
               position++;
            }
         }

         readConfiguration(config);

         String url = null;
         
         if (configuration.getProperty("url") == null)
         {
            String host = configuration.getProperty("host", "localhost");
            int port = Integer.valueOf(configuration.getProperty("port", "5432"));
            String database = configuration.getProperty("database");
            if (database == null)
            {
               System.out.println("database not defined.");
               return;
            }
         
            url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
         }
         else
         {
            url = configuration.getProperty("url");
         }

         String user = configuration.getProperty("user");
         if (user == null)
         {
            System.out.println("user not defined.");
            return;
         }

         String password = configuration.getProperty("password");
         if (password == null)
         {
            System.out.println("password not defined.");
            return;
         }

         planCount = Integer.valueOf(configuration.getProperty("plan_count", "5"));
         debug = Boolean.valueOf(configuration.getProperty("debug", "false"));
         
         c = DriverManager.getConnection(url, user, password);

         SortedSet<String> queries = processQueries(c);
         writeIndex(queries);
         writeTables();
         writeCSV();
         writeHOT();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (c != null)
         {
            try
            {
               c.close();
            }
            catch (Exception e)
            {
               // Nothing to do
            }
         }
      }
   }

   /**
    * JOIN visitor
    */
   static class JoinVisitor extends ExpressionDeParser
   {
      private String queryId;
      private net.sf.jsqlparser.statement.Statement statement;
      
      JoinVisitor(String queryId, net.sf.jsqlparser.statement.Statement statement)
      {
         this.queryId = queryId;
         this.statement = statement;
      }

      @Override
      public void visit(Column column)
      {
         String tableName = null;
         if (column.getTable() != null)
            tableName = column.getTable().getName();

         if (tableName == null)
         {
            List<String> tables = new TablesNamesFinder().getTableList(statement);
            if (tables != null && tables.size() == 1)
               tableName = tables.get(0);
         }

         if (tableName != null)
            if (aliases.containsKey(tableName))
               tableName = aliases.get(tableName);
         
         if (tableName != null)
         {
            Map<String, Set<String>> m = on.get(tableName);
            if (m == null)
               m = new TreeMap<>();
            
            Set<String> cols = m.get(queryId);
            if (cols == null)
               cols = new TreeSet<>();

            cols.add(column.getColumnName());
            m.put(queryId, cols);
            on.put(tableName, m);
         }
      }
   }
}
