/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Jesper Pedersen <jesper.pedersen@comcast.net>
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

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

   /** EXPLAIN (VERBOSE) */
   private static final String EXPLAIN_VERBOSE = "EXPLAIN (VERBOSE)";

   /** Color: Green */
   private static final String COLOR_GREEN = "lime";

   /** Color: Yellow */
   private static final String COLOR_YELLOW = "yellow";

   /** Color: Red */
   private static final String COLOR_RED = "red";

   /** Color: Black */
   private static final String COLOR_BLACK = "black";

   /** Issue type: High priority */
   private static final int ISSUE_TYPE_HIGH_PRIORITY = 0;

   /** Issue type: Normal priority */
   private static final int ISSUE_TYPE_NORMAL_PRIORITY = 1;

   /** Issue code: Duplicated column */
   private static final String ISSUE_CODE_DUPLICATED_COLUMN = "Duplicated column";

   /** Issue code: Identical column */
   private static final String ISSUE_CODE_IDENTICAL_COLUMN = "Identical column";

   /** Issue code: Non parameter column */
   private static final String ISSUE_CODE_NON_PARAMETER_COLUMN = "Non parameter column";

   /** Issue code: Never executed */
   private static final String ISSUE_CODE_NEVER_EXECUTED = "Never executed";

   /** Issue code: Disk sort */
   private static final String ISSUE_CODE_DISK_SORT = "Disk sort";

   /** Issue code: UPDATE all columns */
   private static final String ISSUE_CODE_UPDATE_ALL_COLUMNS = "UPDATE of all columns";

   /** Issue code: UPDATE of primary key */
   private static final String ISSUE_CODE_UPDATE_PRIMARY_KEY = "UPDATE of primary key";

   /** The configuration */
   private static Properties configuration;

   /** Is PostgreSQL 10 or higher */
   private static boolean is10 = false;

   /** Is PostgreSQL 11 or higher */
   private static boolean is11 = false;

   /** Plan count */
   private static int planCount;

   /** Output debug information */
   private static boolean debug;

   /** Current table name */
   private static String currentTableName = null;

   /** IN expression column */
   private static String inExpressionColumn = null;

   /** Data:          Table       Column  Value */
   private static Map<String, Map<String, Object>> data = new TreeMap<>();

   /** Columns:       Table       Number   Name */
   private static Map<String, Map<Integer, String>> columns = new TreeMap<>();
   
   /** Aliases:       Alias   Name */
   private static Map<String, String> aliases = new TreeMap<>();

   /** Plans:         Query   Plan */
   private static Map<String, String> plans = new TreeMap<>();

   /** Planner time:  Query   Time */
   private static Map<String, Double> plannerTimes = new TreeMap<>();

   /** Executor time: Query   Time */
   private static Map<String, Double> executorTimes = new TreeMap<>();

   /** Tables:        Name        Column  Type */
   private static Map<String, Map<String, Integer>> tables = new TreeMap<>();
   
   /** Column sizes:  Name        Column  Size */
   private static Map<String, Map<String, Integer>> columnSizes = new TreeMap<>();

   /** Indexes:       Table       Index   Columns */
   private static Map<String, Map<String, List<String>>> indexes = new TreeMap<>();
   
   /** Index color    Index   Color */
   private static Map<String, String> indexColor = new TreeMap<>();

   /** Used indexes   Index        Query */
   private static Map<String, List<String>> usedIndexes = new TreeMap<>();

   /** Primary key:   Table   Columns */
   private static Map<String, List<String>> primaryKeys = new TreeMap<>();

   /** ON/IN usage:   Table       Query   Columns */
   private static Map<String, Map<String, Set<String>>> on = new TreeMap<>();

   /** WHERE usage:   Table       Query   Columns */
   private static Map<String, Map<String, List<String>>> where = new TreeMap<>();

   /** SET usage:     Table   Columns */
   private static Map<String, Set<String>> set = new TreeMap<>();
   
   /** Select usage:  Table       QueryId */
   private static Map<String, Set<String>> selects = new TreeMap<>();

   /** Insert usage:  Table       QueryId */
   private static Map<String, Set<String>> inserts = new TreeMap<>();

   /** Update usage:  Table       QueryId */
   private static Map<String, Set<String>> updates = new TreeMap<>();

   /** Delete usage:  Table       QueryId */
   private static Map<String, Set<String>> deletes = new TreeMap<>();

   /** Export usage:  Table       Name        Key     Value */
   private static Map<String, Map<String, Map<String, String>>> exports = new TreeMap<>();

   /** Import usage:  Table       Name        Key     Value */
   private static Map<String, Map<String, Map<String, String>>> imports = new TreeMap<>();

   /** Row count:     Table   Rows */
   private static Map<String, String> rowCounts = new TreeMap<>();

   /** Index row count: Id    Rows */
   private static Map<String, String> indexCounts = new TreeMap<>();

   /** Partitions     Parent  Children */
   private static Map<String, List<String>> partitions = new TreeMap<>();

   /** Partition type:Table   Type */
   private static Map<String, String> partitionType = new TreeMap<>();

   /** Partition map: Child   Parent */
   private static Map<String, String> partitionMap = new TreeMap<>();

   /** Partition indexes: Parent Children */
   private static Map<String, List<String>> partitionIndexes = new TreeMap<>();

   /** Partition index child: Child Parent */
   private static Map<String, String> partitionIndexChildren = new TreeMap<>();

   /** Issues         Query   Issues */
   private static Map<String, List<Issue>> issues = new TreeMap<>();

   /** Suggestions    Table   SQL */
   private static Map<String, List<String>> sql = new TreeMap<>();

   /** Constraints    Name */
   private static Set<String> constraints = new TreeSet<>();

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
      l.add("<li><a href=\"indexes.html\">Indexes</a></li>");
      l.add("<li><a href=\"hot.html\">HOT</a></li>");
      l.add("<li><a href=\"result.csv\">Times</a></li>");
      l.add("<li><a href=\"suggestions.html\">Suggestions</a></li>");
      l.add("<li><a href=\"environment.html\">Environment</a></li>");
      l.add("</ul>");
      l.add("<p>");
      
      l.add("<h2>Queries</h2>");
      l.add("<table>");
      for (String q : queryIds)
      {
         l.add("<tr>");
         if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("issues", "true"))) && issues.containsKey(q))
         {
            String color = COLOR_YELLOW;
            for (Issue is : issues.get(q))
            {
               if (ISSUE_TYPE_HIGH_PRIORITY == is.getType())
                  color = COLOR_RED;
            }

            l.add("<td style=\"background-color: " + color + "\"><a href=\"" + q + ".html\">" + q +"</a></td>");
         }
         else
         {
            l.add("<td><a href=\"" + q + ".html\">" + q +"</a></td>");
         }
         l.add("<td>" + (plannerTimes.get(q) != null ? plannerTimes.get(q) + "ms" : "") + "</td>");
         l.add("<td>" + (plannerTimes.get(q) != null ? executorTimes.get(q) + "ms" : "") + "</td>");
         l.add("<td>" + (plans.get(q) != null ? plans.get(q) : "") + "</td>");
         l.add("</tr>");
      }
      l.add("</table>");
      
      l.add("<p>");
      l.add("Generated by <a href=\"https://github.com/jesperpedersen/postgres-tools/tree/master/QueryAnalyzer\">QueryAnalyzer</a>");

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Write tables.html
    * @param c The connection
    */
   private static void writeTables(Connection c) throws Exception
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
         if (partitionMap.containsKey(tableName) &&
             Boolean.FALSE.equals(Boolean.valueOf(configuration.getProperty("show_partitions", "false"))))
            continue;

         String size = getTableSize(c, tableName);
         l.add("<h2>" + tableName.toUpperCase() + " (" + size + ")</h2>");

         Map<String, Integer> tableData = tables.get(tableName);
         Map<String, Integer> columnSize = columnSizes.get(tableName);
         List<String> pkInfo = primaryKeys.get(tableName);
         Map<String, Map<String, String>> exp = exports.get(tableName.toLowerCase());
         Map<String, Map<String, String>> imp = imports.get(tableName.toLowerCase());
         boolean partition = false;

         l.add("<table>");
         for (String columnName : tableData.keySet())
         {
            l.add("<tr>");
            String typeName = getTypeName(tableData.get(columnName));
            if (typeName == null)
            {
               typeName = getTypeName(c, tableName, columnName);
            }
            if ("CHAR".equals(typeName) || "VARCHAR".equals(typeName))
            {
               typeName = typeName + "(" + columnSize.get(columnName) + ")";
            }
            if (pkInfo.contains(columnName))
            {
               l.add("<td><b>" + columnName + "</b></td>");
               l.add("<td><b>" + typeName + "</b></td>");
            }
            else
            {
               l.add("<td>" + columnName + "</td>");
               l.add("<td>" + typeName + "</td>");
            }
            l.add("</tr>");
         }
         l.add("</table>");

         if (partitions.containsKey(tableName))
         {
            l.add("<p>");
            l.add("<u><b>Partitions</b></u>");
            l.add("<p>");

            StringBuilder sb = new StringBuilder();
            sb.append(Integer.toString(partitions.get(tableName).size()));
            sb.append(" x ");
            sb.append(partitionType.get(tableName));
            l.add(sb.toString());

            partition = true;
         }

         if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("row_information", "false"))))
         {
            l.add("<p>");
            l.add("<u><b>Rows</b></u>");
            l.add("<p>");
            l.add(getRowCount(c, tableName));
         }

         l.add("<p>");
         l.add("<u><b>Primary key</b></u>");
         if (pkInfo.size() > 0)
         {
            l.add("<table>");
            for (String columnName : pkInfo)
            {
               String typeName = getTypeName(tableData.get(columnName));
               if (typeName == null)
               {
                  typeName = getTypeName(c, tableName, columnName);
               }
               if ("CHAR".equals(typeName) || "VARCHAR".equals(typeName))
               {
                  typeName = typeName + "(" + columnSize.get(columnName) + ")";
               }
               l.add("<tr>");
               l.add("<td><b>" + columnName + "</b></td>");
               l.add("<td><b>" + typeName + "</b></td>");
               l.add("</tr>");
            }
            l.add("</table>");
         }
         else
         {
            l.add("<p>");
            l.add("None");
         }
         
         Map<String, List<String>> indexData = indexes.get(tableName);

         if (partition)
         {
            for (String part : partitions.get(tableName))
            {
               if (!tables.containsKey(part))
                  initTable(c, part);
            }
         }

         l.add("<p>");
         l.add("<u><b>Indexes</b></u>");
         if (indexData.size() > 0)
         {
            Set<String> s = set.get(tableName);
            Set<List<String>> seen = new HashSet<>();
            l.add("<table>");
            for (Map.Entry<String, List<String>> idx : indexData.entrySet())
            {
               if (partitionIndexChildren.containsKey(idx.getKey()) &&
                   Boolean.FALSE.equals(Boolean.valueOf(configuration.getProperty("show_partitions", "false"))))
                  continue;

               l.add("<tr>");
               if (indexData.get(idx.getKey()).equals(pkInfo))
               {
                  boolean duplicated = false;
                  for (List<String> seenIdx : seen)
                  {
                     if (seenIdx.containsAll(idx.getValue()))
                     {
                        duplicated = true;
                     }
                  }

                  seen.add(idx.getValue());

                  l.add("<td><b>" + idx.getKey() + "</b></td>");
                  if (duplicated && !partition)
                  {
                     l.add("<td style=\"color : " + COLOR_RED + "\"><b>" + idx.getValue() + "</b></td>");
                  }
                  else
                  {
                     l.add("<td><b>" + idx.getValue() + "</b></td>");
                  }

                  l.add("<td><b>" + getIndexSize(c, idx.getKey()) + "</b></td>");

                  if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("row_information", "false"))))
                  {
                     StringBuilder sb = new StringBuilder();
                     sb.append(getIndexCount(c, tableName, idx.getValue()));
                     sb.append(" / ");
                     sb.append(getRowCount(c, tableName));

                     l.add("<td>" + sb.toString() + "</td>");
                  }
               }
               else
               {
                  String color = COLOR_BLACK;
                  if (s != null)
                  {
                     for (String col : idx.getValue())
                     {
                        if (s.contains(col))
                           color = COLOR_RED;
                     }
                  }

                  indexColor.put(idx.getKey(), color);

                  boolean duplicated = false;
                  for (List<String> seenIdx : seen)
                  {
                     if (seenIdx.containsAll(idx.getValue()))
                     {
                        duplicated = true;
                     }
                  }

                  seen.add(idx.getValue());

                  l.add("<td style=\"color : " + color + "\">" + idx.getKey() + "</td>");
                  if (duplicated && !partition)
                  {
                     l.add("<td style=\"color : " + COLOR_RED + "\">" + idx.getValue() + "</td>");
                  }
                  else
                  {
                     l.add("<td>" + idx.getValue() + "</td>");
                  }

                  l.add("<td>" + getIndexSize(c, idx.getKey()) + "</td>");

                  if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("row_information", "false"))))
                  {
                     StringBuilder sb = new StringBuilder();
                     sb.append(getIndexCount(c, tableName, idx.getValue()));
                     sb.append(" / ");
                     sb.append(getRowCount(c, tableName));
                     l.add("<td>" + sb.toString() + "</td>");
                  }
               }

               if (isPrimaryKey(tableName, idx.getValue()))
               {
                  boolean asComment = validPrimaryKeyName(idx.getKey(), tableName, idx.getValue());
                  if (!asComment)
                     asComment = isForeignKeyExport(tableName, idx.getValue());

                  dropPrimaryKey(tableName, idx.getKey(), idx.getValue(), asComment);
                  createPrimaryKey(tableName, idx.getValue(), asComment);
               }
               else if (isUnique(idx.getKey()))
               {
                  boolean asComment = validUniqueIndexName(idx.getKey(), tableName, idx.getValue());
                  dropUniqueIndex(tableName, idx.getKey(), idx.getValue(), asComment);
                  createUniqueIndex(tableName, idx.getValue(), asComment);
               }
               else
               {
                  dropIndex(tableName, idx.getKey(), idx.getValue(), false,
                            validIndexName(idx.getKey(), tableName, idx.getValue()));
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

         if ((exp != null && exp.size() > 0) || (imp != null && imp.size() > 0))
         {
            l.add("<p>");
            l.add("<u><b>Foreign key constraints</b></u>");
            if (exp != null && exp.size() > 0)
            {
               l.add("<p>");
               l.add("<b>Exports</b><br/>");
               l.add("<table>");
               for (Map.Entry<String, Map<String, String>> e : exp.entrySet())
               {
                  Map<String, String> v = e.getValue();
                  l.add("<tr>");
                  l.add("<td>" + e.getKey() + "</td>");
                  l.add("<td>" + v.get("FKTABLE_NAME") + ":" + v.get("FKCOLUMN_NAME") + " -> " +
                        v.get("PKTABLE_NAME") + ":" + v.get("PKCOLUMN_NAME") + "</td>");
                  l.add("</tr>");
               }
               l.add("</table>");
            }
            if (imp != null && imp.size() > 0)
            {
               l.add("<p>");
               l.add("<b>Imports</b><br/>");
               l.add("<table>");
               for (Map.Entry<String, Map<String, String>> e : imp.entrySet())
               {
                  Map<String, String> v = e.getValue();
                  l.add("<tr>");
                  l.add("<td>" + e.getKey() + "</td>");
                  l.add("<td>" + v.get("FKTABLE_NAME") + ":" + v.get("FKCOLUMN_NAME") + " -> " +
                        v.get("PKTABLE_NAME") + ":" + v.get("PKCOLUMN_NAME") + "</td>");
                  l.add("</tr>");
               }
               l.add("</table>");
            }
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
               l.add("<p><b><u>Primary Key</u></b><p>Exception: " + e.getMessage() + "<br/>");
               for (StackTraceElement ste : e.getStackTrace())
               {
                  l.add(ste.getClassName() + ":" + ste.getLineNumber() + "<br/>");
               }
            }
            try
            {
               l.addAll(suggestionIndexes(c, tableName, tableOn, tableWhere, tableSet, imports.get(tableName)));
            }
            catch (Exception e)
            {
               l.add("<p><b><u>Indexes</u></b><p>Exception: " + e.getMessage() + "<br/>");
               for (StackTraceElement ste : e.getStackTrace())
               {
                  l.add(ste.getClassName() + ":" + ste.getLineNumber() + "<br/>");
               }
            }
         }

         if (debug)
         {
            l.add("<h3>DEBUG</h3>");

            if (selects.get(tableName) != null && selects.get(tableName).size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>SELECT</b></u>");
               l.add("<pre>");
               l.add(selects.get(tableName).toString());
               l.add("</pre>");
            }

            if (tableOn != null && tableOn.size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>ON</b></u>");
               l.add("<pre>");
               l.add(tableOn.toString());
               l.add("</pre>");
            }

            if (tableWhere != null && tableWhere.size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>WHERE</b></u>");
               l.add("<pre>");
               l.add(tableWhere.toString());
               l.add("</pre>");
            }

            if (tableSet != null && tableSet.size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>SET</b></u>");
               l.add("<pre>");
               l.add(set.get(tableName).toString());
               l.add("</pre>");
            }

            if (inserts.get(tableName) != null && inserts.get(tableName).size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>INSERT</b></u>");
               l.add("<pre>");
               l.add(inserts.get(tableName).toString());
               l.add("</pre>");
            }

            if (updates.get(tableName) != null && updates.get(tableName).size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>UPDATE</b></u>");
               l.add("<pre>");
               l.add(updates.get(tableName).toString());
               l.add("</pre>");
            }

            if (deletes.get(tableName) != null && deletes.get(tableName).size() > 0)
            {
               l.add("<p>");
               l.add("<u><b>DELETE</b></u>");
               l.add("<pre>");
               l.add(deletes.get(tableName).toString());
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
    * @param c The connection
    */
   private static void writeReport(String queryId,
                                   String origQuery, String query,
                                   Set<String> usedTables,
                                   String plan,
                                   List<Integer> types,
                                   List<String> values,
                                   Connection c) throws Exception
   {
      boolean replay = false;

      // Replay integration
      if (queryId.startsWith("query.select") ||
          queryId.startsWith("query.update") ||
          queryId.startsWith("query.delete"))
      {
         replay = true;

         boolean commit = true;
         if (queryId.startsWith("query.update") || queryId.startsWith("query.delete"))
         {
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

               if (v != null)
               {
                  if (v.startsWith("'") && v.endsWith("'"))
                     v = v.substring(1, v.length() - 1);

                  sb = sb.append(v);
               }
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

         writeFile(Paths.get("report", queryId + ".cli"), cli);
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

      if (replay)
      {
         l.add("<p>");
         l.add("<b>Replay:</b>");
         l.add("<p>");
         l.add("<a href=\"" + queryId + ".cli\">File</a>");
      }

      if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("issues", "true"))) && issues.containsKey(queryId))
      {
         l.add("<h2>Issues</h2>");
         l.add("<ul>");
         List<Issue> ls = issues.get(queryId);
         Collections.sort(ls,
                          new Comparator<Issue>()
                          {
                             @Override
                             public int compare(Issue o1, Issue o2)
                             {
                                return o1.getType() - o2.getType();
                             }
                          });
         for (Issue issue : ls)
         {
            String color = issue.getType() == ISSUE_TYPE_HIGH_PRIORITY ? COLOR_RED : COLOR_YELLOW;
            l.add("<li style=\"background-color: black; color: " + color + "\">");
            l.add(issue.toString());
            l.add("</li>");
         }
         l.add("</ul>");
      }

      if (plan != null && !"".equals(plan))
      {
         l.add("<h2>Plan</h2>");
         l.add("<pre>");
         l.add(plan);
         l.add("</pre>");
      }

      l.add("<h2>Tables</h2>");
      for (String tableName : usedTables)
      {
         Map<String, Integer> tableData = tables.get(tableName);
         Map<String, Integer> columnSize = columnSizes.get(tableName);
         List<String> pkInfo = primaryKeys.get(tableName);
         if (tableData != null)
         {
            l.add("<h3>" + tableName + "</h3>");
            l.add("<table>");
            for (String columnName : tableData.keySet())
            {
               l.add("<tr>");
               String typeName = getTypeName(tableData.get(columnName));
               if (typeName == null)
               {
                  typeName = getTypeName(c, tableName, columnName);
               }
               if ("CHAR".equals(typeName) || "VARCHAR".equals(typeName))
               {
                  typeName = typeName + "(" + columnSize.get(columnName) + ")";
               }
               if (pkInfo.contains(columnName))
               {
                  l.add("<td><b>" + columnName + "</b></td>");
                  l.add("<td><b>" + typeName + "</b></td>");
               }
               else
               {
                  l.add("<td>" + columnName + "</td>");
                  l.add("<td>" + typeName + "</td>");
               }
               l.add("</tr>");
            }
            l.add("</table>");

            if (partitions.containsKey(tableName))
            {
               l.add("<p>");

               StringBuilder sb = new StringBuilder();
               sb.append(Integer.toString(partitions.get(tableName).size()));
               sb.append(" x ");
               sb.append(partitionType.get(tableName));

               l.add("Partitions: " + sb.toString());
            }
         }
      }
      
      l.add("<h2>Indexes</h2>");
      for (String tableName : usedTables)
      {
         Map<String, List<String>> indexData = indexes.get(tableName);
         List<String> pkInfo = primaryKeys.get(tableName);

         if (partitions.containsKey(tableName))
         {
            for (String partition : partitions.get(tableName))
            {
               if (!tables.containsKey(partition))
                  initTable(c, partition);
            }
         }

         if (indexData != null && indexData.size() > 0)
         {
            l.add("<h3>" + tableName + "</h3>");
            l.add("<table>");
            for (String indexName : indexData.keySet())
            {
               if (partitionIndexChildren.containsKey(indexName) &&
                   Boolean.FALSE.equals(Boolean.valueOf(configuration.getProperty("show_partitions", "false"))))
                  continue;

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

      boolean fkcTitle = false;
      for (String tableName : usedTables)
      {
         Map<String, Map<String, String>> exp = exports.get(tableName);
         Map<String, Map<String, String>> imp = imports.get(tableName);
         if ((exp != null && exp.size() > 0) || (imp != null && imp.size() > 0))
         {
            if (!fkcTitle)
            {
               l.add("<h2>Foreign key constraints</h2>");
               fkcTitle = true;
            }

            l.add("<h3>" + tableName + "</h3>");
            if (exp != null && exp.size() > 0)
            {
               l.add("<b>Exports</b><br/>");
               l.add("<table>");
               for (Map.Entry<String, Map<String, String>> e : exp.entrySet())
               {
                  Map<String, String> v = e.getValue();
                  l.add("<tr>");
                  l.add("<td>" + e.getKey() + "</td>");
                  l.add("<td>" + v.get("FKTABLE_NAME") + ":" + v.get("FKCOLUMN_NAME") + " -> " +
                        v.get("PKTABLE_NAME") + ":" + v.get("PKCOLUMN_NAME") + "</td>");
                  l.add("</tr>");
               }
               l.add("</table>");
            }
            if (imp != null && imp.size() > 0)
            {
               l.add("<b>Imports</b><br/>");
               l.add("<table>");

               for (Map.Entry<String, Map<String, String>> e : imp.entrySet())
               {
                  Map<String, String> v = e.getValue();
                  l.add("<tr>");
                  l.add("<td>" + e.getKey() + "</td>");
                  l.add("<td>" + v.get("FKTABLE_NAME") + ":" + v.get("FKCOLUMN_NAME") + " -> " +
                        v.get("PKTABLE_NAME") + ":" + v.get("PKCOLUMN_NAME") + "</td>");
                  l.add("</tr>");
               }
               l.add("</table>");
            }
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

      for (Map.Entry<String, Map<String, Integer>> table : tables.entrySet())
      {
         if (partitionMap.containsKey(table.getKey()) &&
             Boolean.FALSE.equals(Boolean.valueOf(configuration.getProperty("show_partitions", "false"))))
            continue;

         l.add("<h2>" + table.getKey().toUpperCase() + "</h2>");

         Set<String> setColumns = set.get(table.getKey());
         if (setColumns == null)
            setColumns = new TreeSet<>();
         Set<String> indexColumns = new TreeSet<>();

         Map<String, List<String>> idxs = indexes.get(table.getKey());
         if (idxs != null)
         {
            for (List<String> cs : idxs.values())
            {
               indexColumns.addAll(cs);
            }
         }
         
         l.add("<table>");
         l.add("<tr>");
         l.add("<td><b>Columns</b></td>");
         l.add("</tr>");

         for (String column : table.getValue().keySet())
         {
            String color = COLOR_BLACK;
            if (indexColumns.contains(column) && setColumns.contains(column))
            {
               color = COLOR_RED;
            }
            else if (setColumns.contains(column))
            {
               color = COLOR_GREEN;
            }
            
            l.add("<tr>");
            l.add("<td style=\"color : " + color + "\">" + column + "</td>");
            l.add("</tr>");
         }
         l.add("</table>");

         if (idxs != null)
         {
            l.add("<p>");
            
            l.add("<table>");
            l.add("<tr>");
            l.add("<td><b>Index</b></td>");
            l.add("<td><b>Columns</b></td>");
            l.add("</tr>");
            for (Map.Entry<String, List<String>> idx : idxs.entrySet())
            {
               boolean hot = true;

               l.add("<tr>");

               StringBuilder sb = new StringBuilder();
               for (int i = 0; i < idx.getValue().size(); i++)
               {
                  sb = sb.append(idx.getValue().get(i));
                  if (setColumns.contains(idx.getValue().get(i)))
                     hot = false;
                  if (i < idx.getValue().size() - 1)
                     sb = sb.append(", ");
               }
               if (hot)
               {
                  indexColor.put(idx.getKey(), COLOR_BLACK);

                  l.add("<td>" + idx.getKey() + "</td>");
                  l.add("<td>" + sb.toString() + "</td>");
               }
               else
               {
                  indexColor.put(idx.getKey(), COLOR_RED);

                  l.add("<td style=\"color : " + COLOR_RED + "\">" + idx.getKey() + "</td>");
                  l.add("<td style=\"color : " + COLOR_RED + "\">" + sb.toString() + "</td>");
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

      writeFile(Paths.get("report", "hot.html"), l);
   }

   /**
    * Write indexes.html
    */
   private static void writeIndexes() throws Exception
   {
      List<String> l = new ArrayList<>();
      Set<String> suggested = new TreeSet<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Index information</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Index information</h1>");
      l.add("");

      l.add("<h2>Used indexes</h2>");
      l.add("<table>");
      for (Map.Entry<String, List<String>> entry : usedIndexes.entrySet())
      {
         String table = "";
         for (Map.Entry<String, Map<String, List<String>>> ientry : indexes.entrySet())
         {
            if (ientry.getValue().keySet().contains(entry.getKey()))
            {
               table = ientry.getKey();
            }
         }

         String color = COLOR_BLACK;
         if (indexColor.get(entry.getKey()) != null)
            color = indexColor.get(entry.getKey());

         l.add("<tr>");
         l.add("<td style=\"color : " + color + "\">");
         l.add(entry.getKey());
         l.add("</td>");
         l.add("<td>");
         l.add(table);
         l.add("</td>");
         l.add("<td>");
         l.add(entry.getValue().toString());
         l.add("</td>");
         l.add("</tr>");

         List<String> cols = (indexes.get(table)).get(entry.getKey());
         boolean btree = isBTreeIndex(table, cols);
         String idxName = getIndexName(table, cols);
         suggested.add(idxName);

         if (!isPrimaryKey(table, cols))
            createIndex(table, idxName, btree, cols, null, true, true);
      }
      l.add("</table>");

      SortedSet<String> unusedIndexes = new TreeSet<>();
      for (Map.Entry<String, Map<String, List<String>>> entry : indexes.entrySet())
      {
         unusedIndexes.addAll(entry.getValue().keySet());
      }
      unusedIndexes.removeAll(usedIndexes.keySet());

      if (unusedIndexes.size() > 0)
      {
         l.add("<h2>Unused indexes</h2>");
         l.add("<table>");
         for (String unused : unusedIndexes)
         {
            if (partitionIndexChildren.containsKey(unused))
               continue;

            String table = "";
            for (Map.Entry<String, Map<String, List<String>>> entry : indexes.entrySet())
            {
               if (entry.getValue().keySet().contains(unused))
               {
                  table = entry.getKey();
               }
            }

            String color = COLOR_BLACK;
            if (indexColor.get(unused) != null)
               color = indexColor.get(unused);

            l.add("<tr>");
            l.add("<td style=\"color : " + color + "\">");
            l.add(unused);
            l.add("</td>");
            l.add("<td>");
            l.add(table);
            l.add("</td>");
            l.add("</tr>");

            List<String> cols = (indexes.get(table)).get(unused);
            String idxName = getIndexName(table, cols);
            dropIndex(table, idxName, cols, true,
                      suggested.contains(idxName) || isPrimaryKey(table, cols) || isForeignKeyImport(table, cols));
         }
         l.add("</table>");
      }

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "indexes.html"), l);
   }

   /**
    * Write environment.html
    * @parma c The connection
    */
   private static void writeEnvironment(Connection c) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Environment</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Environment</h1>");
      l.add("");

      Statement stmt = c.createStatement();
      stmt.execute("SELECT version()");
      ResultSet rs = stmt.getResultSet();
      rs.next();
      l.add(rs.getString(1));
      rs.close();

      l.add("<p>");
      l.add("<table>");
      stmt.execute("SHOW all");
      rs = stmt.getResultSet();
      while (rs.next())
      {
         l.add("<tr>");
         l.add("<td>" + rs.getString(1) + "</td>");
         l.add("<td>" + rs.getString(2) + "</td>");
         l.add("</tr>");
      }
      rs.close();
      stmt.close();

      l.add("</table>");

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "environment.html"), l);
   }

   /**
    * Write suggestions.html
    */
   private static void writeSuggestions() throws Exception
   {
      List<String> file = new ArrayList<>();
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Suggestions</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Suggestions</h1>");
      l.add("");
      l.add("File: <a href=\"suggestions.sql\">SQL</a>");

      l.add("<h2>Content</h2>");
      l.add("<pre>");

      for (Map.Entry<String, List<String>> entry : sql.entrySet())
      {
         l.add("-- " + entry.getKey());
         l.addAll(entry.getValue());
         l.add("");

         file.add("-- " + entry.getKey());
         file.addAll(entry.getValue());
         file.add("");
      }

      l.add("ANALYZE;");
      file.add("ANALYZE;");

      l.add("</pre>");

      l.add("<p>");
      l.add("<a href=\"index.html\">Back</a>");

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "suggestions.html"), l);
      writeFile(Paths.get("report", "suggestions.sql"), file);
   }

   /**
    * Is primary key
    * @param tableName The table name
    * @param columns The columns
    * @return True if primary key, otherwise false
    */
   private static boolean isPrimaryKey(String tableName, List<String> columns)
   {
      List<String> l = primaryKeys.get(tableName);
      if (l != null)
      {
         return l.equals(columns);
      }

      return false;
   }

   /**
    * Get primary key name
    * @param tableName The table name
    * @param columns The columns
    * @return The primary key name
    */
   private static String getPrimaryKeyName(String tableName, List<String> columns)
   {
      StringBuilder indexName = new StringBuilder();
      indexName.append("pk_");
      indexName.append(tableName);
      indexName.append('_');
      for (int i = 0; i < columns.size(); i++)
      {
         indexName.append(columns.get(i));
         if (i < columns.size() - 1)
            indexName.append('_');
      }
      return indexName.toString();
   }

   /**
    * Valid primary key name
    * @param idxName The index name
    * @param tableName The table name
    * @param columns The columns
    * @return True if valid, otherwise false
    */
   private static boolean validPrimaryKeyName(String idxName, String tableName, List<String> columns)
   {
      return idxName.equals(getPrimaryKeyName(tableName, columns));
   }

   /**
    * Create PRIMARY KEY
    * @param tableName The table name
    * @param columns The index columns
    * @param asComment Is this a comment
    */
   private static void createPrimaryKey(String tableName, List<String> columns, boolean asComment)
   {
      if (!asComment || debug)
      {
         List<String> l = sql.get(tableName);
         if (l == null)
            l = new ArrayList<>();

         String idxName = getPrimaryKeyName(tableName, columns);

         StringBuilder sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("CREATE UNIQUE INDEX ");
         sb = sb.append(idxName);
         sb = sb.append(" ON ");
         sb = sb.append(tableName);
         sb = sb.append(" (");
         for (int i = 0; i < columns.size(); i++)
         {
            sb = sb.append(columns.get(i));
            if (i < columns.size() - 1)
               sb = sb.append(", ");
         }
         sb = sb.append(");");
         l.add(sb.toString());

         sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("ALTER TABLE ");
         sb = sb.append(tableName);
         sb = sb.append(" ADD PRIMARY KEY USING INDEX ");
         sb = sb.append(idxName);
         sb = sb.append(";");
         l.add(sb.toString());

         sql.put(tableName, l);
      }
   }

   /**
    * Drop PRIMARY KEY
    * @param tableName The table name
    * @param idxName The index name
    * @param columns The index columns
    * @param asComment Is this a comment
    */
   private static void dropPrimaryKey(String tableName, String idxName, List<String> columns,
                                      boolean asComment)
   {
      if (!asComment || debug)
      {
         List<String> l = sql.get(tableName);
         if (l == null)
            l = new ArrayList<>();

         StringBuilder sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("ALTER TABLE ");
         sb = sb.append(tableName);
         sb = sb.append(" DROP CONSTRAINT ");
         sb = sb.append(idxName);
         sb = sb.append(";");
         if (!asComment)
         {
            sb = sb.append(" -- ");
            sb = sb.append(columns.toString());
         }
         l.add(sb.toString());

         sql.put(tableName, l);
      }
   }

   /**
    * Is unique
    * @param name The name
    * @return True if unique, otherwise false
    */
   private static boolean isUnique(String name)
   {
      return constraints.contains(name);
   }

   /**
    * Get unique index name
    * @param tableName The table name
    * @param columns The columns
    * @return The unique index name
    */
   private static String getUniqueIndexName(String tableName, List<String> columns)
   {
      StringBuilder indexName = new StringBuilder();
      indexName.append("uidx_");
      indexName.append(tableName);
      indexName.append('_');
      for (int i = 0; i < columns.size(); i++)
      {
         indexName.append(columns.get(i));
         if (i < columns.size() - 1)
            indexName.append('_');
      }
      return indexName.toString();
   }

   /**
    * Valid unique index name
    * @param idxName The index name
    * @param tableName The table name
    * @param columns The columns
    * @return True if valid, otherwise false
    */
   private static boolean validUniqueIndexName(String idxName, String tableName, List<String> columns)
   {
      return idxName.equals(getUniqueIndexName(tableName, columns));
   }

   /**
    * Create UNIQUE INDEX
    * @param tableName The table name
    * @param columns The index columns
    * @param asComment Is this a comment
    */
   private static void createUniqueIndex(String tableName, List<String> columns, boolean asComment)
   {
      if (!asComment || debug)
      {
         List<String> l = sql.get(tableName);
         if (l == null)
            l = new ArrayList<>();

         String idxName = getUniqueIndexName(tableName, columns);

         StringBuilder sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("CREATE UNIQUE INDEX ");
         sb = sb.append(idxName);
         sb = sb.append(" ON ");
         sb = sb.append(tableName);
         sb = sb.append(" (");
         for (int i = 0; i < columns.size(); i++)
         {
            sb = sb.append(columns.get(i));
            if (i < columns.size() - 1)
               sb = sb.append(", ");
         }
         sb = sb.append(");");
         l.add(sb.toString());

         sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("ALTER TABLE ");
         sb = sb.append(tableName);
         sb = sb.append(" ADD UNIQUE USING INDEX ");
         sb = sb.append(idxName);
         sb = sb.append(";");
         l.add(sb.toString());

         sql.put(tableName, l);
      }
   }

   /**
    * Drop UNIQUE INDEX
    * @param tableName The table name
    * @param idxName The index name
    * @param columns The index columns
    * @param asComment Is this a comment
    */
   private static void dropUniqueIndex(String tableName, String idxName, List<String> columns,
                                       boolean asComment)
   {
      if (!asComment || debug)
      {
         List<String> l = sql.get(tableName);
         if (l == null)
            l = new ArrayList<>();

         StringBuilder sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("ALTER TABLE ");
         sb = sb.append(tableName);
         sb = sb.append(" DROP CONSTRAINT ");
         sb = sb.append(idxName);
         sb = sb.append(";");
         if (!asComment)
         {
            sb = sb.append(" -- ");
            sb = sb.append(columns.toString());
         }
         l.add(sb.toString());

         sql.put(tableName, l);
      }
   }

   /**
    * Has index
    * @param tableName The table name
    * @param idxName The index name
    * @param columns The columns
    * @return True if index exists, otherwise false
    */
   private static boolean hasIndex(String tableName, String idxName, List<String> columns)
   {
      Map<String, List<String>> m = indexes.get(tableName);
      if (m != null)
      {
         List<String> cols = m.get(idxName);
         if (cols != null)
         {
            return cols.equals(columns);
         }
      }

      return false;
   }

   /**
    * Is BTREE index
    * @param tableName The table name
    * @param columns The columns
    * @return True if BTREE, otherwise false (HASH)
    */
   private static boolean isBTreeIndex(String tableName, List<String> columns)
   {
      if (columns.size() == 1)
      {
         Integer type = tables.get(tableName).get(columns.get(0));
         if (type == Types.CHAR || type == Types.LONGVARCHAR || type == Types.VARCHAR)
         {
            return false;
         }
      }

      return true;
   }

   /**
    * Get index name
    * @param tableName The table name
    * @param columns The columns
    * @return The index name
    */
   private static String getIndexName(String tableName, List<String> columns)
   {
      StringBuilder indexName = new StringBuilder();
      indexName.append("idx_");
      indexName.append(tableName);
      indexName.append('_');
      for (int i = 0; i < columns.size(); i++)
      {
         indexName.append(columns.get(i));
         if (i < columns.size() - 1)
            indexName.append('_');
      }
      return indexName.toString();
   }

   /**
    * Get covering index name
    * @param tableName The table name
    * @param columns The columns
    * @param includes The includes
    * @return The index name
    */
   private static String getCoveringIndexName(String tableName, List<String> columns, List<String> includes)
   {
      StringBuilder indexName = new StringBuilder();
      indexName.append("cidx_");
      indexName.append(tableName);
      indexName.append('_');
      for (int i = 0; i < columns.size(); i++)
      {
         indexName.append(columns.get(i));
         indexName.append('_');
      }
      for (int i = 0; i < includes.size(); i++)
      {
         indexName.append(includes.get(i));
         if (i < includes.size() - 1)
            indexName.append('_');
      }
      return indexName.toString();
   }

   /**
    * Valid index name
    * @param idxName The index name
    * @param tableName The table name
    * @param columns The columns
    * @return True if valid, otherwise false
    */
   private static boolean validIndexName(String idxName, String tableName, List<String> columns)
   {
      return idxName.equals(getIndexName(tableName, columns));
   }

   /**
    * Valid covering index name
    * @param idxName The index name
    * @param tableName The table name
    * @param columns The columns
    * @param includes The includes
    * @return True if valid, otherwise false
    */
   private static boolean validCoveringIndexName(String idxName, String tableName,
                                                 List<String> columns, List<String> includes)
   {
      return idxName.equals(getCoveringIndexName(tableName, columns, includes));
   }

   /**
    * CREATE INDEX
    * @param tableName The table name
    * @param idxName The index name
    * @param btree Is the index a BTREE, or HASH
    * @param columns The columns
    * @param includes The INCLUDE columns
    * @param ifNotExists IF NOT EXISTS
    * @param asComment Is this a comment
    */
   private static void createIndex(String tableName, String idxName, boolean btree,
                                   List<String> columns, List<String> includes,
                                   boolean ifNotExists, boolean asComment)
   {
      if (!asComment || debug)
      {
         List<String> l = sql.get(tableName);
         if (l == null)
            l = new ArrayList<>();

         StringBuilder sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("CREATE INDEX ");
         if (ifNotExists)
            sb = sb.append("IF NOT EXISTS ");
         sb = sb.append(idxName);
         sb = sb.append(" ON ");
         sb = sb.append(tableName);
         sb = sb.append(" USING ");
         if (btree || !is10)
         {
            sb = sb.append("BTREE");
         }
         else
         {
            sb = sb.append("HASH");
         }
         sb = sb.append(" (");
         for (int i = 0; i < columns.size(); i++)
         {
            sb = sb.append(columns.get(i));
            if (i < columns.size() - 1)
               sb = sb.append(", ");
         }
         sb = sb.append(")");
         if (includes != null)
         {
            sb = sb.append(" INCLUDE ");
            sb = sb.append("(");
            for (int i = 0; i < includes.size(); i++)
            {
               sb = sb.append(includes.get(i));
               if (i < includes.size() - 1)
                  sb = sb.append(", ");
            }
            sb = sb.append(")");
         }
         sb = sb.append(";");
         l.add(sb.toString());

         sql.put(tableName, l);
      }
   }

   /**
    * DROP INDEX
    * @param tableName The table name
    * @param idxName The index name
    * @param columns The index columns
    * @param ifExists IF EXISTS
    * @param asComment Is this a comment
    */
   private static void dropIndex(String tableName, String idxName, List<String> columns,
                                 boolean ifExists, boolean asComment)
   {
      if (!asComment || debug)
      {
         List<String> l = sql.get(tableName);
         if (l == null)
            l = new ArrayList<>();

         StringBuilder sb = new StringBuilder();
         if (asComment)
            sb = sb.append("-- ");
         sb = sb.append("DROP INDEX ");
         if (ifExists)
            sb = sb.append("IF EXISTS ");
         sb = sb.append(idxName);
         sb = sb.append(";");
         if (!asComment)
         {
            sb = sb.append(" -- ");
            sb = sb.append(columns.toString());
         }
         l.add(sb.toString());

         sql.put(tableName, l);
      }
   }

   /**
    * Is foreign key (EXPORT)
    * @param tableName The table name
    * @param columns The columns
    * @return True if foreign key, otherwise false
    */
   private static boolean isForeignKeyExport(String tableName, List<String> columns)
   {
      Map<String, Map<String, String>> e = exports.get(tableName);
      if (e != null)
      {
         for (Map<String, String> entry : e.values())
         {
            List<String> pk = Arrays.asList(entry.get("PKCOLUMN_NAME"));
            if (columns.equals(pk))
               return true;
         }
      }

      return false;
   }

   /**
    * Is foreign key (IMPORT)
    * @param tableName The table name
    * @param columns The columns
    * @return True if foreign key, otherwise false
    */
   private static boolean isForeignKeyImport(String tableName, List<String> columns)
   {
      Map<String, Map<String, String>> e = imports.get(tableName);
      if (e != null)
      {
         for (Map<String, String> entry : e.values())
         {
            List<String> pk = Arrays.asList(entry.get("FKCOLUMN_NAME"));
            if (columns.equals(pk))
               return true;
         }
      }

      return false;
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
      
      initPartitions(c);

      for (String key : keys)
      {
         String origQuery = configuration.getProperty(key);
         String query = origQuery;
         String plan = "";
         List<Integer> types = new ArrayList<>();
         List<String> values = new ArrayList<>();

         try
         {
            Set<String> usedTables = getUsedTables(c, origQuery);
            net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(query);

            if (query.indexOf("?") != -1)
            {
               query = rewriteQuery(c, key, query, types, values);
            }
            else
            {
               rewriteQuery(c, key, query, types, values);
            }

            if (query != null)
            {
               boolean eavb = true;
               if (!(statement instanceof Select))
               {
                  for (String table : usedTables)
                  {
                     if (exports.containsKey(table.toLowerCase()))
                        eavb = false;

                     if (imports.containsKey(table.toLowerCase()))
                        eavb = false;
                  }
               }

               for (int i = 0; i < planCount; i++)
               {
                  if (eavb)
                  {
                     executeStatement(c, EXPLAIN_ANALYZE_VERBOSE_BUFFERS + " " + query, false);
                  }
                  else
                  {
                     executeStatement(c, EXPLAIN_VERBOSE + " " + query, false);
                  }
               }
                  
               List<String> l = null;
               if (eavb)
               {
                  l = executeStatement(c, EXPLAIN_ANALYZE_VERBOSE_BUFFERS + " " + query);
               }
               else
               {
                  l = executeStatement(c, EXPLAIN_VERBOSE + " " + query);
               }

               if (statement instanceof Select)
               {
                  StringBuilder sb = new StringBuilder();

                  String firstLine = l.get(0);
                  if (firstLine.indexOf("using") != -1)
                  {
                     String s = firstLine.substring(0, firstLine.indexOf("using")).trim();
                     sb.append(s);
                     if (s.indexOf("Index") != -1)
                     {
                        String idx = firstLine.substring(firstLine.indexOf("using") + 6, firstLine.indexOf(" on "));

                        if (partitionIndexChildren.containsKey(idx))
                           idx = partitionIndexChildren.get(idx);

                        List<String> idxList = usedIndexes.get(idx);
                        if (idxList == null)
                           idxList = new ArrayList<>();
                        if (!idxList.contains(key))
                           idxList.add(key);
                        usedIndexes.put(idx, idxList);
                     }
                  }
                  else if (firstLine.indexOf("on") != -1)
                  {
                     sb.append(firstLine.substring(0, firstLine.indexOf("on")).trim());
                  }
                  else
                  {
                     sb.append(firstLine.substring(0, firstLine.indexOf("  (")).trim());
                  }

                  for (int i = 1; i < l.size(); i++)
                  {
                     String line = l.get(i);
                     if (line.indexOf("->") != -1)
                     {
                        if (line.indexOf("using") != -1)
                        {
                           String s = line.substring(line.indexOf("->") + 3, line.indexOf("using")).trim();
                           sb.append(" | ");
                           if (line.indexOf("(never executed)") == -1)
                           {
                              sb.append(s);
                           }
                           else
                           {
                              String table = line.substring(line.indexOf(" on ") + 4, line.indexOf("  ("));
                              if (table.indexOf(".") != -1)
                              {
                                 table = table.substring(table.indexOf(".") + 1);
                              }

                              if (!partitionMap.containsKey(table))
                              {
                                 sb.append("<i>");
                                 sb.append(s);
                                 sb.append("</i>");

                                 List<Issue> ls = issues.get(key);
                                 if (ls == null)
                                    ls = new ArrayList<>();

                                 Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_NEVER_EXECUTED, line);
                                 if (!ls.contains(is))
                                 {
                                    ls.add(is);
                                    issues.put(key, ls);
                                 }
                              }
                              else
                              {
                                 sb.append("<del>");
                                 sb.append(s);
                                 sb.append("</del>");
                              }
                           }
                           if (s.indexOf("Index") != -1 && line.indexOf("(never executed)") == -1)
                           {
                              String idx = line.substring(line.indexOf("using") + 6, line.indexOf(" on "));

                              if (partitionIndexChildren.containsKey(idx))
                                 idx = partitionIndexChildren.get(idx);

                              List<String> idxList = usedIndexes.get(idx);
                              if (idxList == null)
                                 idxList = new ArrayList<>();
                              if (!idxList.contains(key))
                                 idxList.add(key);
                              usedIndexes.put(idx, idxList);
                           }
                        }
                        else if (line.indexOf("on") != -1)
                        {
                           String s = line.substring(line.indexOf("->") + 3, line.indexOf("on")).trim();
                           sb.append(" | ");
                           if (line.indexOf("(never executed)") == -1)
                           {
                              sb.append(s);
                           }
                           else
                           {
                              String table = line.substring(line.indexOf(" on ") + 4, line.indexOf("  ("));
                              if (table.indexOf(".") != -1)
                              {
                                 table = table.substring(table.indexOf(".") + 1);
                              }

                              if (!partitionMap.containsKey(table))
                              {
                                 sb.append("<i>");
                                 sb.append(s);
                                 sb.append("</i>");

                                 List<Issue> ls = issues.get(key);
                                 if (ls == null)
                                    ls = new ArrayList<>();

                                 Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_NEVER_EXECUTED, line);
                                 if (!ls.contains(is))
                                 {
                                    ls.add(is);
                                    issues.put(key, ls);
                                 }
                              }
                              else
                              {
                                 sb.append("<del>");
                                 sb.append(s);
                                 sb.append("</del>");
                              }
                           }
                           if (s.indexOf("Index") != -1 && line.indexOf("(never executed)") == -1)
                           {
                              String idx = line.substring(line.indexOf(" on ") + 4, line.indexOf("  ("));

                              if (partitionIndexChildren.containsKey(idx))
                                 idx = partitionIndexChildren.get(idx);

                              List<String> idxList = usedIndexes.get(idx);
                              if (idxList == null)
                                 idxList = new ArrayList<>();
                              if (!idxList.contains(key))
                                 idxList.add(key);
                              usedIndexes.put(idx, idxList);
                           }
                        }
                        else
                        {
                           String s = line.substring(line.indexOf("->") + 3, line.indexOf("  (")).trim();
                           sb.append(" | ");
                           if (line.indexOf("(never executed)") == -1)
                           {
                              sb.append(s);
                           }
                           else
                           {
                              sb.append("<i>");
                              sb.append(s);
                              sb.append("</i>");

                              List<Issue> ls = issues.get(key);
                              if (ls == null)
                                 ls = new ArrayList<>();

                              Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_NEVER_EXECUTED, line);
                              if (!ls.contains(is))
                              {
                                 ls.add(is);
                                 issues.put(key, ls);
                              }
                           }
                        }
                     }

                     if (line.indexOf("Sort ") != -1 && line.indexOf("Disk:") != -1)
                     {
                        List<Issue> ls = issues.get(key);
                        if (ls == null)
                           ls = new ArrayList<>();

                        Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_DISK_SORT, line);
                        if (!ls.contains(is))
                        {
                           ls.add(is);
                           issues.put(key, ls);
                        }
                     }
                  }

                  plans.put(key, sb.toString());
               }
               else if (statement instanceof Update || statement instanceof Delete)
               {
                  StringBuilder sb = new StringBuilder();

                  for (int i = 1; i < l.size(); i++)
                  {
                     String line = l.get(i);
                     if (line.indexOf("->") != -1)
                     {
                        if (line.indexOf("using") != -1)
                        {
                           String s = line.substring(line.indexOf("->") + 3, line.indexOf("using")).trim();
                           if (sb.length() > 0)
                              sb.append(" | ");
                           sb.append(s);
                           if (s.indexOf("Index") != -1)
                           {
                              String idx = line.substring(line.indexOf("using") + 6, line.indexOf(" on "));

                              if (partitionIndexChildren.containsKey(idx))
                                 idx = partitionIndexChildren.get(idx);

                              List<String> idxList = usedIndexes.get(idx);
                              if (idxList == null)
                                 idxList = new ArrayList<>();
                              if (!idxList.contains(key))
                                 idxList.add(key);
                              usedIndexes.put(idx, idxList);
                           }
                        }
                        else if (line.indexOf("on") != -1)
                        {
                           if (sb.length() > 0)
                              sb.append(" | ");
                           sb.append(line.substring(line.indexOf("->") + 3, line.indexOf("on")).trim());
                        }
                     }

                     if (line.indexOf("Sort ") != -1 && line.indexOf("Disk:") != -1)
                     {
                        List<Issue> ls = issues.get(key);
                        if (ls == null)
                           ls = new ArrayList<>();

                        Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_DISK_SORT, line);
                        if (!ls.contains(is))
                        {
                           ls.add(is);
                           issues.put(key, ls);
                        }
                     }
                  }

                  if (statement instanceof Update)
                  {
                     Update update = (Update)statement;
                     if (update.getTables() != null && update.getTables().size() == 1)
                     {
                        String tableName = update.getTables().get(0).getName().toLowerCase();
                        Map<Integer, String> cols = columns.get(tableName);
                        if (cols != null && cols.size() > 0)
                        {
                           int counter = 1;
                           List<String> pkCols = primaryKeys.get(tableName);
                           if (pkCols != null && pkCols.size() > 1)
                              counter = pkCols.size();

                           if (update.getColumns().size() == cols.size() ||
                               update.getColumns().size() == cols.size() - counter)
                           {
                              List<Issue> ls = issues.get(key);
                              if (ls == null)
                                 ls = new ArrayList<>();

                              StringBuilder colsDesc = new StringBuilder();
                              for (int i = 0; i < update.getColumns().size(); i++)
                              {
                                 colsDesc.append(update.getColumns().get(i).getColumnName());
                                 if (i < update.getColumns().size() - 1)
                                    colsDesc.append(", ");
                              }

                              Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_UPDATE_ALL_COLUMNS,
                                                   colsDesc.toString());
                              if (!ls.contains(is))
                              {
                                 ls.add(is);
                                 issues.put(key, ls);
                              }
                           }

                           if (pkCols != null && pkCols.size() > 0)
                           {
                              for (int i = 0; i < update.getColumns().size(); i++)
                              {
                                 String colName = update.getColumns().get(i).getColumnName().toLowerCase();
                                 if (pkCols.contains(colName))
                                 {
                                    List<Issue> ls = issues.get(key);
                                    if (ls == null)
                                       ls = new ArrayList<>();

                                    Issue is = new Issue(ISSUE_TYPE_HIGH_PRIORITY, ISSUE_CODE_UPDATE_PRIMARY_KEY,
                                                         colName);
                                    if (!ls.contains(is))
                                    {
                                       ls.add(is);
                                       issues.put(key, ls);
                                    }
                                 }
                              }
                           }
                        }
                     }
                  }

                  plans.put(key, sb.toString());
               }

               for (String s : l)
               {
                  plan += s;
                  plan += "\n";

                  if (s.startsWith("Planning"))
                  {
                     int index = s.indexOf(" ", 15);
                     plannerTimes.put(key, Double.valueOf(s.substring(15, index)));
                  }
                  else if (s.startsWith("Execution"))
                  {
                     int index = s.indexOf(" ", 16);
                     executorTimes.put(key, Double.valueOf(s.substring(16, index)));
                  }
               }
            }

            writeReport(key, origQuery, query, usedTables, plan, types, values, c);
         }
         catch (Exception e)
         {
            System.out.println("Original query: " + origQuery);
            System.out.println("Key           : " + key);
            System.out.println("Data          :");
            System.out.println(data);
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
                                                    List<String> pkInfo,
                                                    Map<String, Set<String>> tableOn,
                                                    Map<String, List<String>> tableWhere,
                                                    Set<String> tableSet)
   {
      List<String> result = new ArrayList<>();

      SortedSet<String> existingColumns = new TreeSet<>(pkInfo);
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
            String typeName = tableData.get(columnName) != null ? getTypeName(tableData.get(columnName)) : null;
            if (typeName != null)
            {
               result.add("<tr>");
               result.add("<td><b>" + columnName + "</b></td>");
               result.add("<td><b>" + typeName + "</b></td>");
               result.add("</tr>");
            }
         }

         result.add("</table>");
         result.add("<p>");

         if (pkColumns.equals(existingColumns))
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
    * @parma c The connection
    * @param tableName The table name
    * @param tableOn The columns accessed in ON per query
    * @param tableWhere The columns accessed in WHERE per query
    * @param tableSet The columns accessed in SET
    * @param imp Foreign index columns
    * @return The report data
    */
   private static List<String> suggestionIndexes(Connection c,
                                                 String tableName,
                                                 Map<String, Set<String>> tableOn,
                                                 Map<String, List<String>> tableWhere,
                                                 Set<String> tableSet,
                                                 Map<String, Map<String, String>> imp)
      throws Exception
   {
      List<String> result = new ArrayList<>();
      Set<List<String>> suggested = new HashSet<>();
      TreeMap<Integer, List<List<String>>> counts = new TreeMap<>();

      result.add("<p>");
      result.add("<b><u>Indexes</u></b>");
      result.add("<table>");

      if (tableWhere != null)
      {
         for (List<String> l : tableWhere.values())
         {
            Integer count = l.size();
            List<List<String>> ll = counts.get(count);
            if (ll == null)
               ll = new ArrayList<>();

            if (!ll.contains(l))
               ll.add(l);
            counts.put(count, ll);
         }
      }

      if (tableOn != null)
      {
         for (Set<String> ss : tableOn.values())
         {
            for (String s : ss)
            {
               List l = new ArrayList<>(1);
               l.add(s);

               Integer count = Integer.valueOf(1);
               List<List<String>> ll = counts.get(count);
               if (ll == null)
                  ll = new ArrayList<>();

               if (!ll.contains(l))
                  ll.add(l);
               counts.put(count, ll);
            }
         }
      }

      if (imp != null)
      {
         for (Map<String, String> me : imp.values())
         {
            List l = new ArrayList<>(1);
            l.add(me.get("FKCOLUMN_NAME"));

            Integer count = Integer.valueOf(1);
            List<List<String>> ll = counts.get(count);
            if (ll == null)
               ll = new ArrayList<>();

            if (!ll.contains(l))
               ll.add(l);
            counts.put(count, ll);
         }
      }

      int idx = 1;
      Set<String> prefixes = new TreeSet<>();

      for (Integer count : counts.descendingKeySet())
      {
         List<List<String>> ll = counts.get(count);
         for (List<String> l : ll)
         {
            if (!suggested.contains(l))
            {
               List<String> newIndex = new ArrayList<>();
               for (int i = 0; i < l.size(); i++)
               {
                  String col = l.get(i);
                  if (tableSet != null && i > 0)
                  {
                     if (!tableSet.contains(col))
                        newIndex.add(col);
                  }
                  else
                  {
                     newIndex.add(col);
                  }
               }

               if (newIndex.size() > 0 && !suggested.contains(newIndex))
               {
                  boolean hot = true;
                  boolean btree = true;

                  if (tableSet != null)
                  {
                     for (int i = 0; hot && i < newIndex.size(); i++)
                     {
                        if (tableSet.contains(newIndex.get(i)))
                           hot = false;
                     }
                  }

                  String indexName = getIndexName(tableName, newIndex);

                  result.add("<tr>");
                  result.add("<td>IDX" + idx + "</td>");
                  result.add("<td>" +
                             (hot ? "" : "<div style=\"color : " + COLOR_RED + "\">") +
                             indexName +
                             (hot ? "" : "</div>") +
                             "</td>");
                  result.add("<td>" + newIndex + "</td>");

                  btree = isBTreeIndex(tableName, newIndex);
                  if (btree || !is10)
                  {
                     result.add("<td>btree</td>");
                  }
                  else
                  {
                     result.add("<td>hash</td>");
                  }

                  if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("row_information", "false"))))
                  {
                     StringBuilder sb = new StringBuilder();
                     sb.append(getIndexCount(c, tableName, newIndex));
                     sb.append(" / ");
                     sb.append(getRowCount(c, tableName));

                     result.add("<td>" + sb.toString() + "</td>");
                  }

                  if (!isPrimaryKey(tableName, newIndex))
                  {
                     boolean asComment = prefixes.contains(newIndex.get(0));
                     if (!asComment)
                        asComment = hasIndex(tableName, indexName, newIndex);
                     createIndex(tableName, indexName, btree, newIndex, null, false, asComment);
                  }

                  result.add("</tr>");
                  suggested.add(newIndex);
                  prefixes.add(newIndex.get(0));
                  idx++;
               }
            }
         }
      }

      // Consider covering indexes for PostgreSQL 11+
      if (is11)
      {
         int numberOfColumns = columns.get(tableName).keySet().size();
         if (numberOfColumns > 0)
         {
            for (Map.Entry<String, List<String>> entry : tableWhere.entrySet())
            {
               if (entry.getValue().size() == 1)
               {
                  String query = configuration.getProperty(entry.getKey());
                  net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(query);
                  if (statement instanceof Select)
                  {
                     Select select = (Select)statement;
                     if (select.getSelectBody() instanceof PlainSelect)
                     {
                        PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
                        if (plainSelect.getDistinct() == null && plainSelect.getJoins() == null)
                        {
                           if (plainSelect.getSelectItems().size() == 1 &&
                               plainSelect.getSelectItems().get(0) instanceof SelectExpressionItem)
                           {
                              boolean hot = true;
                              SelectExpressionItem sei = (SelectExpressionItem)plainSelect.getSelectItems().get(0);
                              List<String> includes = new ArrayList<>();
                              String columnName = sei.getExpression().toString();

                              if (columnName.indexOf(".") != -1)
                                 columnName = columnName.substring(columnName.indexOf(".") + 1);

                              includes.add(columnName);
                              String indexName = getCoveringIndexName(tableName, entry.getValue(), includes);

                              if (tableSet != null)
                              {
                                 if (tableSet.contains(entry.getValue().get(0)) || tableSet.contains(columnName))
                                    hot = false;
                              }

                              if (hot)
                              {
                                 result.add("<tr>");
                                 result.add("<td>IDX" + idx + "</td>");
                                 result.add("<td>" + indexName + "</td>");
                                 result.add("<td>" + entry.getValue() + "</td>");
                                 result.add("<td>covering</td>");

                                 if (Boolean.TRUE.equals(Boolean.valueOf(configuration.getProperty("row_information", "false"))))
                                 {
                                    result.add("<td></td>");
                                 }

                                 result.add("</tr>");

                                 createIndex(tableName, indexName, true, entry.getValue(), includes, true, false);
                                 idx++;
                              }
                           }
                        }
                     }
                  }
               }
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
         Map<String, List<String>> extraIndexes = new TreeMap<>();
         
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
               try
               {
                  Object data = null;
                  Integer type = null;

                  data = getData(c, currentColumn);
                  type = getType(c, currentColumn, query);

                  if (type != null)
                  {
                     if (data == null)
                     {
                        data = getDefaultValue(type);
                        if (data == null)
                           System.out.println("Unsupported type " + type + " for " + query);
                        if (needsQuotes(data))
                           data = "'" + data + "'";
                     }

                     values.add(data.toString());
                     types.add(type);

                     this.getBuffer().append(data);
                  }
                  else
                  {
                     System.out.println("Unsupported column/type in " + query);
                  }
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
                  aliases.put(table.getAlias().getName().toLowerCase(), currentTableName.toLowerCase());

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
                        tableName = tables.get(0).toLowerCase();
                  }
                  
                  if (tableName != null)
                  {
                     List<String> cols = extraIndexes.get(tableName);
                     if (cols == null)
                        cols = new ArrayList<>();
                  
                     cols.add(0, column.getColumnName());
                     extraIndexes.put(tableName, cols);
                  }
               }

               @Override
               public void visit(InExpression inExpression)
               {
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
                     tableName = tables.get(0).toLowerCase();
                  
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

            if (plainSelect.getSelectItems() != null)
            {
               Set<String> fullyQualifiedColumns = new TreeSet<>();
               Set<String> nameColumns = new TreeSet<>();
               SelectItemVisitorAdapter siva = new SelectItemVisitorAdapter()
               {
                  @Override
                  public void visit(SelectExpressionItem sei)
                  {
                     if (sei.getExpression() instanceof Column)
                     {
                        Column column = (Column)sei.getExpression();
                        String fQKey = column.getFullyQualifiedName().toLowerCase();
                        String nameKey = column.getColumnName().toLowerCase();
                        boolean add = true;

                        if (fullyQualifiedColumns.contains(fQKey))
                        {
                           List<Issue> ls = issues.get(queryId);
                           if (ls == null)
                              ls = new ArrayList<>();

                           Issue is = new Issue(ISSUE_TYPE_HIGH_PRIORITY, ISSUE_CODE_DUPLICATED_COLUMN, fQKey);
                           if (!ls.contains(is))
                           {
                              ls.add(is);
                              issues.put(queryId, ls);
                           }
                           add = false;
                        }
                        else
                        {
                           fullyQualifiedColumns.add(fQKey);
                        }

                        if (add && nameColumns.contains(nameKey))
                        {
                           List<Issue> ls = issues.get(queryId);
                           if (ls == null)
                              ls = new ArrayList<>();

                           Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_IDENTICAL_COLUMN, nameKey);
                           if (!ls.contains(is))
                           {
                              ls.add(is);
                              issues.put(queryId, ls);
                           }
                        }
                        else
                        {
                           nameColumns.add(nameKey);
                        }
                     }
                  }
               };

               for (SelectItem si : plainSelect.getSelectItems())
               {
                  si.accept(siva);
               }
            }

            if (plainSelect.getJoins() != null)
            {
               for (Join join : plainSelect.getJoins())
               {
                  if (join.getOnExpression() != null)
                     join.getOnExpression().accept(new JoinVisitor(queryId, s));
               }
            }
            
            if (plainSelect.getWhere() != null)
            {
               plainSelect.getWhere().accept(extraIndexExpressionDeParser);

               Set<Column> nonParameterColumns = new HashSet<>();
               ExpressionDeParser whereScanner = new ExpressionDeParser()
               {
                  private Column currentColumn = null;

                  @Override
                  public void visit(Column column)
                  {
                     if (currentColumn != null)
                     {
                        nonParameterColumns.add(currentColumn);
                     }

                     currentColumn = column;
                  }

                  @Override
                  public void visit(JdbcNamedParameter jdbcNamedParameter)
                  {
                     currentColumn = null;
                  }

                  @Override
                  public void visit(JdbcParameter jdbcParameter)
                  {
                     currentColumn = null;
                  }
               };

               plainSelect.getWhere().accept(whereScanner);

               for (Column column : nonParameterColumns)
               {
                  List<Issue> ls = issues.get(queryId);
                  if (ls == null)
                     ls = new ArrayList<>();

                  Issue is = new Issue(ISSUE_TYPE_NORMAL_PRIORITY, ISSUE_CODE_NON_PARAMETER_COLUMN, column.getColumnName());
                  if (!ls.contains(is))
                  {
                     ls.add(is);
                     issues.put(queryId, ls);
                  }
               }
            }
            
            if (plainSelect.getLimit() != null)
            {
               Limit limit = new Limit();
               limit.setRowCount(new LongValue(1L));

               values.add(Integer.toString(1));
               types.add(Integer.valueOf(Types.INTEGER));

               if (plainSelect.getLimit().getOffset() != null)
               {
                  limit.setOffset(new LongValue(0));
                  values.add(Integer.toString(0));
                  types.add(Integer.valueOf(Types.INTEGER));
               }

               plainSelect.setLimit(limit);
            }

            if (plainSelect.getOffset() != null)
            {
               Offset offset = new Offset();
               offset.setOffset(0);
               plainSelect.setOffset(offset);
            }

            select.setSelectBody(plainSelect);
         }

         StatementDeParser sdp = new StatementDeParser(expressionDeParser, deparser, buffer);
         select.accept(sdp);

         for (String tableName : extraIndexes.keySet())
         {
            List<String> vals = extraIndexes.get(tableName);
            
            if (aliases.containsKey(tableName))
               tableName = aliases.get(tableName);

            Map<String, List<String>> m = where.get(tableName);
            if (m == null)
               m = new TreeMap<>();

            List<String> l = m.get(queryId);
            if (l == null)
               l = new ArrayList<>();

            for (String col : vals)
               l.add(0, col.toLowerCase());

            m.put(queryId, l);
            where.put(tableName, m);
         }

         return buffer.toString();
      }
      else if (s instanceof Update)
      {
         Update update = (Update)s;

         for (Table table : update.getTables())
         {
            Set<String> qids = updates.get(table.getName().toLowerCase());
            if (qids == null)
               qids = new TreeSet<>();
            qids.add(queryId);
            updates.put(table.getName().toLowerCase(), qids);

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
                  String tbl = currentTableName.toLowerCase();

                  if (partitionMap.containsKey(tbl))
                     tbl = partitionMap.get(tbl);

                  Set<String> s = set.get(tbl);
                  if (s == null)
                     s = new TreeSet<>();

                  s.add(column.getColumnName().toLowerCase());
                  set.put(tbl, s);
               }
               else
               {
                  Map<String, List<String>> m = where.get(currentTableName.toLowerCase());
                  if (m == null)
                     m = new TreeMap<>();

                  List<String> l = m.get(queryId);
                  if (l == null)
                     l = new ArrayList<>();

                  l.add(column.getColumnName().toLowerCase());
                  m.put(queryId, l);
                  where.put(currentTableName.toLowerCase(), m);
               }

               this.getBuffer().append(column);
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               try
               {
                  Object data = getData(c, currentColumn);
                  Integer type = getType(c, currentColumn, query);

                  if (data == null)
                  {
                     data = getDefaultValue(type);
                     if (data == null)
                        System.out.println("Unsupported type " + type + " for " + query);
                     if (needsQuotes(data))
                        data = "'" + data + "'";
                  }
                  values.add(data.toString());
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
                  aliases.put(table.getAlias().getName().toLowerCase(), currentTableName.toLowerCase());

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

         Set<String> qids = deletes.get(delete.getTable().getName().toLowerCase());
         if (qids == null)
            qids = new TreeSet<>();
         qids.add(queryId);
         deletes.put(delete.getTable().getName().toLowerCase(), qids);

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

               l.add(0, column.getColumnName().toLowerCase());
               m.put(queryId, l);
               where.put(currentTableName, m);

               this.getBuffer().append(column);
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               try
               {
                  Object data = getData(c, currentColumn, currentTableName);
                  Integer type = getType(c, currentColumn, query, currentTableName);

                  if (data == null)
                  {
                     data = getDefaultValue(type);
                     if (data == null)
                        System.out.println("Unsupported type " + type + " for " + query);
                     if (needsQuotes(data))
                        data = "'" + data + "'";
                  }

                  values.add(data.toString());
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
               inExpressionColumn = null;

               if (inExpression.getLeftExpression() != null)
               {
                  ExpressionDeParser inExpressionDeParser = new ExpressionDeParser()
                  {
                     @Override
                     public void visit(Column column)
                     {
                        inExpressionColumn = column.getColumnName().toLowerCase();
                        this.getBuffer().append(column);
                     }
                  };

                  inExpression.getLeftExpression().accept(inExpressionDeParser);
               }

               if (currentTableName != null && inExpressionColumn != null)
               {
                  Map<String, List<String>> m = where.get(currentTableName);
                  if (m == null)
                     m = new TreeMap<>();

                  List<String> l = m.get(queryId);
                  if (l == null)
                     l = new ArrayList<>();

                  l.add(0, inExpressionColumn.toLowerCase());
                  m.put(queryId, l);
                  where.put(currentTableName, m);
               }

               StringBuilder sb = new StringBuilder();
               ExpressionDeParser rightExpressionDeParser = new ExpressionDeParser()
               {
                  @Override
                  public void visit(net.sf.jsqlparser.statement.select.SubSelect ss)
                  {
                     SelectBodyVisitor sbv = new SelectBodyVisitor(queryId, ss.getSelectBody(),
                                                                   c, query, types, values);
                     sbv.process();
                     this.getBuffer().append(sbv.getBuffer());
                  }
               };
               rightExpressionDeParser.setBuffer(sb);
               inExpression.getRightItemsList().accept(rightExpressionDeParser);

               this.getBuffer().append(inExpression.getLeftExpression());
               this.getBuffer().append(" IN ");
               this.getBuffer().append("(");
               this.getBuffer().append(rightExpressionDeParser.getBuffer());
               this.getBuffer().append(")");
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

         return buffer.toString();
      }
      else if (s instanceof Insert)
      {
         Insert insert = (Insert)s;

         Set<String> qids = inserts.get(insert.getTable().getName().toLowerCase());
         if (qids == null)
            qids = new TreeSet<>();
         qids.add(queryId);
         inserts.put(insert.getTable().getName().toLowerCase(), qids);

         if (query.toUpperCase().indexOf("SELECT") != -1)
            return null;

         initTableData(c, insert.getTable().getName());
         currentTableName = insert.getTable().getName().toLowerCase();

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private int index = 1;

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               try
               {
                  Table table = new Table(currentTableName);
                  Column column = null;
                  if (insert.getColumns() != null && insert.getColumns().size() > 0)
                  {
                     column = new Column(table, insert.getColumns().get(index - 1).getColumnName().toLowerCase());
                  }
                  else
                  {
                     column = new Column(table, columns.get(currentTableName).get(Integer.valueOf(index)));
                  }
                  Object data = getData(c, column);
                  Integer type = getType(c, column, query);

                  if (data == null)
                  {
                     data = getDefaultValue(type);
                     if (data == null)
                        System.out.println("Unsupported type " + type + " for " + query);
                     if (needsQuotes(data))
                        data = "'" + data + "'";
                  }

                  values.add(data.toString());
                  types.add(type);

                  this.getBuffer().append(data);
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }

               index++;
            }
         };
         expressionDeParser.setBuffer(buffer);

         net.sf.jsqlparser.util.deparser.InsertDeParser insertDeParser =
            new net.sf.jsqlparser.util.deparser.InsertDeParser(expressionDeParser, null, buffer);

         net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
            new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer)
         {
            @Override
            public void visit(Insert insert)
            {
               insertDeParser.deParse(insert);
            }
         };

         insert.accept(statementDeParser);

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
      return getData(c, column, null);
   }

   /**
    * Get data
    * @param c The connection
    * @param column The column
    * @param table The table name
    * @return The value
    */
   private static String getData(Connection c, Column column, String table) throws Exception
   {
      String tableName;

      if (column.getTable() != null)
      {
         tableName = column.getTable().getName();
      }
      else
      {
         tableName = table;
      }

      if (tableName != null && aliases.containsKey(tableName.toLowerCase()))
         tableName = aliases.get(tableName.toLowerCase());

      if (tableName == null)
         tableName = currentTableName;

      tableName = tableName.toLowerCase();
      
      Map<String, Object> values = data.get(tableName);

      if (values == null)
         values = initTableData(c, tableName);
      
      Object o = values.get(column.getColumnName().toLowerCase());

      if (o == null)
         return null;

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
      return getType(c, column, query, null);
   }

   /**
    * Get type
    * @param c The connection
    * @param column The column
    * @param query The query
    * @param table The table name
    * @return The value
    */
   private static Integer getType(Connection c, Column column, String query, String table) throws Exception
   {
      String tableName;

      if (column.getTable() != null)
      {
         tableName = column.getTable().getName();
      }
      else
      {
         tableName = table;
      }

      if (tableName != null && aliases.containsKey(tableName))
         tableName = aliases.get(tableName);

      if (tableName == null)
         tableName = currentTableName;

      tableName = tableName.toLowerCase();

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
      Map<Integer, String> mapping = new TreeMap<>();

      tableName = tableName.toLowerCase();

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
               Object o = getResultSetValue(rs, i, columnType);

               columnName = columnName.toLowerCase();
               if (o == null)
                  o = getDefaultValue(columnType);

               mapping.put(Integer.valueOf(i), columnName);
               values.put(columnName, o);
            }
         }

         data.put(tableName, values);
         columns.put(tableName, mapping);
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
            break;
      }

      return null;
   }
   
   /**
    * Get a default value
    * @param type The type
    * @return The value
    */
   private static Object getDefaultValue(int type) throws Exception
   {
      switch (type)
      {
         case Types.BINARY:
            return new byte[] {};
         case Types.BIT:
            return Boolean.FALSE;
         case Types.BIGINT:
            return Long.valueOf(0);
         case Types.BOOLEAN:
            return Boolean.FALSE;
         case Types.CHAR:
            return ' ';
         case Types.DATE:
            return new java.sql.Date(System.currentTimeMillis());
         case Types.DECIMAL:
            return new java.math.BigDecimal(0);
         case Types.DOUBLE:
            return Double.valueOf(0.0);
         case Types.FLOAT:
            return Double.valueOf(0.0);
         case Types.INTEGER:
            return Integer.valueOf(0);
         case Types.LONGVARBINARY:
            return new byte[] {};
         case Types.LONGVARCHAR:
            return "";
         case Types.NUMERIC:
            return new java.math.BigDecimal(0);
         case Types.REAL:
            return Float.valueOf(0.0f);
         case Types.SMALLINT:
            return Short.valueOf((short)0);
         case Types.TIME:
         case Types.TIME_WITH_TIMEZONE:
            return new java.sql.Time(System.currentTimeMillis());
         case Types.TIMESTAMP:
         case Types.TIMESTAMP_WITH_TIMEZONE:
            return new java.sql.Timestamp(System.currentTimeMillis());
         case Types.TINYINT:
            return Short.valueOf((short)0);
         case Types.VARBINARY:
            return new byte[] {};
         case Types.VARCHAR:
            return "";
         default:
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
            break;
      }

      return null;
   }

   /**
    * Get type name
    * @param c The connection
    * @param table The table name
    * @param column The column name
    * @return The name
    */
   private static String getTypeName(Connection c, String table, String column)
   {
      String result = "UNKNOWN";
      ResultSet rs = null;
      try
      {
         DatabaseMetaData dmd = c.getMetaData();

         rs = dmd.getColumns(null, null, table, "");
         while (rs.next())
         {
            String columnName = rs.getString("COLUMN_NAME");
            columnName = columnName.toLowerCase();

            if (columnName.equals(column))
               result = rs.getString("TYPE_NAME");
         }
      }
      catch (Exception e)
      {
         // Nothing we can do
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
      return result.toUpperCase();
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
                  Object o = getResultSetValue(rs, i, type);
                  if (o == null)
                  {
                     System.out.println("Unsupported type " + type + " for " + s);
                  }
                  sb = sb.append(o);

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
         System.out.println("Query         : " + s);
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
      List<String> tableNames = new TablesNamesFinder().getTableList(s);

      for (String t : tableNames)
         result.add(t.toLowerCase());
      
      for (String tableName : result)
      {
         if (!tables.containsKey(tableName))
         {
            initTable(c, tableName);
         }
      }

      return result;
   }

   /**
    * Initialize table information
    * @param c The connection
    * @param tableName The table name
    */
   private static void initTable(Connection c, String tableName) throws Exception
   {
      Map<String, Integer> tableData = new TreeMap<>();
      Map<String, Integer> columnSize = new TreeMap<>();
      ResultSet rs = null;

      if (tableName.contains("."))
         tableName = tableName.substring(tableName.lastIndexOf(".") + 1);

      try
      {
         DatabaseMetaData dmd = c.getMetaData();

         rs = dmd.getColumns(null, null, tableName, "");
         while (rs.next())
         {
            String columnName = rs.getString("COLUMN_NAME");
            columnName = columnName.toLowerCase();

            int dataType = rs.getInt("DATA_TYPE");
            tableData.put(columnName, dataType);

            int size = rs.getInt("COLUMN_SIZE");
            columnSize.put(columnName, size);
         }
            
         tables.put(tableName, tableData);
         columnSizes.put(tableName, columnSize);
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
         Map<String, List<String>> indexInfo = new TreeMap<>();

         rs = dmd.getIndexInfo(null, null, tableName, false, false);
         while (rs.next())
         {
            String indexName = rs.getString("INDEX_NAME");
            String columnName = rs.getString("COLUMN_NAME");

            indexName = indexName.toLowerCase();
            columnName = columnName.toLowerCase();

            List<String> existing = indexInfo.get(indexName);
            if (existing == null)
               existing = new ArrayList<>();

            if (!existing.contains(columnName))
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
         List<String> pkInfo = new ArrayList<>();

         rs = dmd.getPrimaryKeys(null, null, tableName);
         while (rs.next())
         {
            String columnName = rs.getString("COLUMN_NAME");
            columnName = columnName.toLowerCase();

            if (!pkInfo.contains(columnName))
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

      rs = null;
      try
      {
         DatabaseMetaData dmd = c.getMetaData();

         rs = dmd.getExportedKeys(null, null, tableName);
         while (rs.next())
         {
            Map<String, Map<String, String>> m = exports.get(tableName);
            if (m == null)
               m = new TreeMap<>();

            TreeMap<String, String> v = new TreeMap<>();
            v.put("FKTABLE_NAME", rs.getString("FKTABLE_NAME").toLowerCase());
            v.put("FKCOLUMN_NAME", rs.getString("FKCOLUMN_NAME").toLowerCase());
            v.put("PKTABLE_NAME", rs.getString("PKTABLE_NAME").toLowerCase());
            v.put("PKCOLUMN_NAME", rs.getString("PKCOLUMN_NAME").toLowerCase());

            m.put(rs.getString("FK_NAME").toLowerCase(), v);
            exports.put(tableName, m);
         }
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

         rs = dmd.getImportedKeys(null, null, tableName);
         while (rs.next())
         {
            Map<String, Map<String, String>> m = imports.get(tableName);
            if (m == null)
               m = new TreeMap<>();

            TreeMap<String, String> v = new TreeMap<>();
            v.put("FKTABLE_NAME", rs.getString("FKTABLE_NAME").toLowerCase());
            v.put("FKCOLUMN_NAME", rs.getString("FKCOLUMN_NAME").toLowerCase());
            v.put("PKTABLE_NAME", rs.getString("PKTABLE_NAME").toLowerCase());
            v.put("PKCOLUMN_NAME", rs.getString("PKCOLUMN_NAME").toLowerCase());

            m.put(rs.getString("FK_NAME").toLowerCase(), v);
            imports.put(tableName, m);
         }
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

   /**
    * Get the size of a table
    * @param c The connection
    * @param name The name
    * @return The size
    */
   private static String getTableSize(Connection c, String name) throws Exception
   {
      String result = "";
      Statement stmt = c.createStatement();

      if (partitions.containsKey(name))
      {
         Long size = Long.valueOf(0);
         for (String partition : partitions.get(name))
         {
            stmt.execute("SELECT pg_table_size(\'" + partition + "\')");
            ResultSet rs = stmt.getResultSet();
            rs.next();
            size += Long.valueOf(rs.getString(1));
            rs.close();
         }

         stmt.execute("SELECT pg_size_pretty(" + size + "::bigint)");
         ResultSet rs = stmt.getResultSet();
         rs.next();
         result = rs.getString(1);
         rs.close();
      }
      else
      {
         stmt.execute("SELECT pg_size_pretty(pg_table_size(\'" + name + "\'))");
         ResultSet rs = stmt.getResultSet();
         rs.next();
         result = rs.getString(1);
         rs.close();
      }

      stmt.close();
      return result;
   }

   /**
    * Get the size of an index
    * @param c The connection
    * @param idx The index name
    * @return The size
    */
   private static String getIndexSize(Connection c, String idx) throws Exception
   {
      String result = "";
      Statement stmt = c.createStatement();

      if (partitionIndexes.containsKey(idx))
      {
         Long size = Long.valueOf(0);
         for (String pidx : partitionIndexes.get(idx))
         {
            stmt.execute("SELECT pg_relation_size(quote_ident(\'" + pidx + "\')::text)");
            ResultSet rs = stmt.getResultSet();
            rs.next();
            size += Long.valueOf(rs.getString(1));
            rs.close();
         }

         stmt.execute("SELECT pg_size_pretty(" + size + "::bigint)");
         ResultSet rs = stmt.getResultSet();
         rs.next();
         result = rs.getString(1);
         rs.close();
      }
      else
      {
         ResultSet rs = null;
         try
         {
            stmt.execute("SELECT pg_size_pretty(pg_relation_size(quote_ident(\'" + idx + "\')::text))");
            rs = stmt.getResultSet();
            rs.next();
            result = rs.getString(1);
         }
         catch (Exception e)
         {
            result = "0";
         }
         finally
         {
            if (rs != null)
            {
               try
               {
                  rs.close();
               }
               catch (Exception ignore)
               {
               }
            }
         }
      }

      stmt.close();
      return result;
   }

   /**
    * Get the number of unique rows for an index
    * @param c The connection
    * @param tableName The table name
    * @param cols The index columns
    * @return The size
    */
   private static String getIndexCount(Connection c, String tableName, List<String> cols) throws Exception
   {
      String id = tableName + "_" + cols.toString();
      String result = indexCounts.get(id);

      if (result == null)
      {
         StringBuilder sb = new StringBuilder();
         sb.append("SELECT COUNT(DISTINCT(");
         for (int i = 0; i < cols.size(); i++)
         {
            sb.append(cols.get(i));
            if (i < cols.size() - 1)
               sb.append(", ");
         }
         sb.append(")) FROM ");
         sb.append(tableName);

         Statement stmt = c.createStatement();
         stmt.execute(sb.toString());
         ResultSet rs = stmt.getResultSet();
         rs.next();
         result = rs.getString(1);
         rs.close();
         stmt.close();

         indexCounts.put(id, result);
      }

      return result;
   }

   /**
    * Get the number of rows for a table
    * @param c The connection
    * @param tableName The table name
    * @return The size
    */
   private static String getRowCount(Connection c, String tableName) throws Exception
   {
      String result = rowCounts.get(tableName);

      if (result == null)
      {
         Statement stmt = c.createStatement();
         stmt.execute("SELECT COUNT(*) FROM " + tableName);
         ResultSet rs = stmt.getResultSet();
         rs.next();
         result = rs.getString(1);
         rs.close();
         stmt.close();

         rowCounts.put(tableName, result);
      }

      return result;
   }

   /**
    * Initialize partition information
    * @param c The connection
    */
   private static void initPartitions(Connection c) throws Exception
   {
      Statement stmt = c.createStatement();
      stmt.execute("SELECT relname, oid FROM pg_class WHERE relnamespace = 2200::oid AND relkind = \'p\'");
      ResultSet rs = stmt.getResultSet();
      while (rs.next())
      {
         String parentName = rs.getString(1);
         String parentOid = rs.getString(2);
         List<String> children = new ArrayList<>();

         Statement type = c.createStatement();
         type.execute("SELECT pg_get_partkeydef(\'" + parentOid+ "\'::oid)");
         ResultSet typeRs = type.getResultSet();
         typeRs.next();
         partitionType.put(parentName, typeRs.getString(1));
         typeRs.close();
         type.close();

         Statement inh = c.createStatement();
         inh.execute("SELECT inhrelid FROM pg_inherits WHERE inhparent = " + parentOid + "::oid");
         ResultSet inhRs = inh.getResultSet();
         while (inhRs.next())
         {
            String childOid = inhRs.getString(1);

            Statement name = c.createStatement();
            name.execute("SELECT relname FROM pg_class WHERE oid = " + childOid + "::oid");
            ResultSet nameRs = name.getResultSet();
            nameRs.next();
            String child = nameRs.getString(1);

            children.add(child);
            partitionMap.put(child, parentName);

            nameRs.close();
            name.close();
         }
         inhRs.close();
         inh.close();

         partitions.put(parentName, children);
      }
      rs.close();
      stmt.close();

      stmt = c.createStatement();
      stmt.execute("SELECT relname,oid FROM pg_class WHERE relnamespace = 2200::oid AND relkind = \'I\'");
      rs = stmt.getResultSet();
      while (rs.next())
      {
         String parentIndexName = rs.getString(1);
         String parentIndexOid = rs.getString(2);
         List<String> children = new ArrayList<>();

         Statement inh = c.createStatement();
         inh.execute("SELECT inhrelid FROM pg_inherits WHERE inhparent = " + parentIndexOid + "::oid");
         ResultSet inhRs = inh.getResultSet();
         while (inhRs.next())
         {
            String childOid = inhRs.getString(1);

            Statement name = c.createStatement();
            name.execute("SELECT relname FROM pg_class WHERE oid = " + childOid + "::oid");
            ResultSet nameRs = name.getResultSet();
            nameRs.next();
            String child = nameRs.getString(1);

            children.add(child);
            partitionIndexChildren.put(child, parentIndexName);

            nameRs.close();
            name.close();
         }
         inhRs.close();
         inh.close();

         partitionIndexes.put(parentIndexName, children);
      }
      rs.close();
      stmt.close();
   }

   /**
    * Startup
    * @param c The connection
    */
   private static void startup(Connection c) throws Exception
   {
      Statement stmt = c.createStatement();
      stmt.execute("ANALYZE");
      stmt.close();

      stmt = c.createStatement();
      stmt.execute("SELECT conname FROM pg_constraint");
      ResultSet rs = stmt.getResultSet();
      while (rs.next())
      {
         String name = rs.getString(1);
         constraints.add(name);
      }
      rs.close();
      stmt.close();

      stmt = c.createStatement();
      stmt.execute("SELECT version()");
      rs = stmt.getResultSet();
      if (rs.next())
      {
         String ver = rs.getString(1);
         int offset = ver.indexOf(" ");
         ver = ver.substring(offset + 1, ver.indexOf(" ", offset + 1));
         if (ver.indexOf(".") != -1)
         {
            ver = ver.substring(0, ver.indexOf("."));
            if (Integer.valueOf(ver) == 10)
            {
               is10 = true;
            }
            else if (Integer.valueOf(ver) >= 11)
            {
               is10 = true;
               is11 = true;
            }
         }
         else
         {
            if (ver.startsWith("10"))
            {
               is10 = true;
            }
            else if (ver.startsWith("1"))
            {
               is10 = true;
               is11 = true;
            }
         }
      }
      rs.close();
      stmt.close();
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
      if (report.exists())
      {
         Files.walk(Paths.get("report"))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
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

         startup(c);

         SortedSet<String> queries = processQueries(c);
         writeIndex(queries);
         writeTables(c);
         writeCSV();
         writeHOT();
         writeIndexes();
         writeEnvironment(c);
         writeSuggestions();
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
    * SelectBody visitor
    */
   static class SelectBodyVisitor
   {
      private String queryId;
      private net.sf.jsqlparser.statement.select.SelectBody selectBody;
      private Connection c;
      private String query;
      private List<Integer> types;
      private List<String> values;
      private StringBuilder buffer;

      SelectBodyVisitor(String qid, net.sf.jsqlparser.statement.select.SelectBody sb,
                        Connection c, String query, List<Integer> types, List<String> values)
      {
         this.queryId = qid;
         this.selectBody = sb;
         this.c = c;
         this.query = query;
         this.types = types;
         this.values = values;
         this.buffer = new StringBuilder();
      }

      StringBuilder getBuffer()
      {
         return buffer;
      }

      void process()
      {
         Map<String, List<String>> extraIndexes = new TreeMap<>();

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
               try
               {
                  Object data = getData(c, currentColumn);
                  Integer type = getType(c, currentColumn, query);

                  if (data == null)
                  {
                     data = getDefaultValue(type);
                     if (data == null)
                        System.out.println("Unsupported type " + type + " for " + query);
                     if (needsQuotes(data))
                        data = "'" + data + "'";
                  }

                  values.add(data.toString());
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
                  aliases.put(table.getAlias().getName().toLowerCase(), currentTableName.toLowerCase());

               this.getBuffer().append(table);
            }
         };
         expressionDeParser.setSelectVisitor(deparser);
         expressionDeParser.setBuffer(buffer);

         if (selectBody instanceof PlainSelect)
         {
            PlainSelect plainSelect = (PlainSelect)selectBody;

            ExpressionDeParser extraIndexExpressionDeParser = new ExpressionDeParser()
            {
               @Override
               public void visit(Column column)
               {
                  String tableName = null;
                  if (column.getTable() != null)
                     tableName = column.getTable().getName();

                  if (tableName != null)
                  {
                     List<String> cols = extraIndexes.get(tableName);
                     if (cols == null)
                        cols = new ArrayList<>();

                     cols.add(0, column.getColumnName());
                     extraIndexes.put(tableName, cols);
                  }
               }
            };

            if (plainSelect.getWhere() != null)
            {
               plainSelect.getWhere().accept(extraIndexExpressionDeParser);
            }
         }

         selectBody.accept(deparser);

         for (String tableName : extraIndexes.keySet())
         {
            List<String> vals = extraIndexes.get(tableName);

            if (aliases.containsKey(tableName))
               tableName = aliases.get(tableName);

            Map<String, List<String>> m = where.get(tableName);
            if (m == null)
               m = new TreeMap<>();

            List<String> l = m.get(queryId);
            if (l == null)
               l = new ArrayList<>();

            for (String col : vals)
               l.add(0, col.toLowerCase());

            m.put(queryId, l);
            where.put(tableName, m);
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

         if (statement instanceof Select)
         {
            SelectDeParser deparser = new SelectDeParser()
            {
               @Override
               public void visit(Table table)
               {
                  if (table.getAlias() != null && !table.getAlias().getName().equals(""))
                     aliases.put(table.getAlias().getName().toLowerCase(), table.getName().toLowerCase());
               }
            };
            ((Select)statement).getSelectBody().accept(deparser);
         }
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
               tableName = tables.get(0).toLowerCase();
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

            cols.add(column.getColumnName().toLowerCase());
            m.put(queryId, cols);
            on.put(tableName, m);
         }
      }
   }

   /**
    * Issue
    */
   static class Issue
   {
      /** Issue type */
      private int type;

      /** Issue code */
      private String code;

      /** Description */
      private String description;

      /**
       * Constructor
       * @param type The type
       * @param code The code
       * @param description The description
       */
      Issue(int type, String code, String description)
      {
         this.type = type;
         this.code = code;
         this.description = description;
      }

      /**
       * Get the type
       * @return The value
       */
      int getType()
      {
         return type;
      }

      /**
       * Get the code
       * @return The value
       */
      String getCode()
      {
         return code;
      }

      /**
       * Get the description
       * @return The value
       */
      String getDescription()
      {
         return description;
      }

      /**
       * Equals
       * @param other The other object
       * @return The override
       */
      @Override
      public boolean equals(Object other)
      {
         if (other == null || !(other instanceof Issue))
            return false;

         Issue is = (Issue)other;
         if (getCode().equals(is.getCode()))
         {
            if (getDescription().equals(is.getDescription()))
               return true;
         }

         return false;
      }

      /**
       * toString
       * @return The override
       */
      @Override
      public String toString()
      {
         return code + ": " + description;
      }
   }
}
