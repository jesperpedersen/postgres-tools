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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
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
   
   /** EXPLAIN (ANALYZE, VERBOSE) */
   private static final String EXPLAIN_ANALYZE_VERBOSE = "EXPLAIN (ANALYZE, VERBOSE)";

   /** The configuration */
   private static Properties configuration;

   /** Plan count */
   private static int planCount;

   /** Data:          Table       Column  Value */        
   private static Map<String, Map<String, Object>> data = new HashMap<>();
   
   /** Aliases:       Alias   Name */
   private static Map<String, String> aliases = new HashMap<>();

   /** Current table name */
   private static String currentTableName = null;

   /** Tables:        Name        Column  Type */
   private static Map<String, Map<String, Integer>> tables = new HashMap<>();
   
   /** Indexes:       Table       Index   Columns */
   private static Map<String, Map<String, Set<String>>> indexes = new HashMap<>();
   
   /** Primary key:   Table   Columns */
   private static Map<String, Set<String>> primaryKeys = new HashMap<>();
   
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
    * @parma queryData The query data
    */
   private static void writeIndex(SortedMap<String, String> queryData) throws Exception
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

      l.add("<h2>Queries</h2>");
      l.add("<ul>");
      for (String q : queryData.keySet())
      {
         l.add("<li><a href=\"" + q + ".html\">" + q +"</a>" +
               (!"".equals(queryData.get(q)) ? " (" + queryData.get(q) + ") " : "") + "</li>");
      }
      l.add("</ul>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Write HTML report
    * @parma queryId The query identifier
    * @param origQuery The original query
    * @param query The executed query
    * @param usedTables The used tables
    * @param plan The plan
    */
   private static void writeReport(String queryId,
                                   String origQuery, String query,
                                   Set<String> usedTables,
                                   String plan) throws Exception
   {
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
         if (indexData != null)
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
      
      l.add("");
      l.add("<a href=\"index.html\">Back</a>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", queryId + ".html"), l);
   }

   /**
    * Process the queries
    * @param c The connection
    * @return The query identifiers
    */
   private static SortedMap<String, String> processQueries(Connection c) throws Exception
   {
      SortedMap<String, String> result = new TreeMap<>();

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
         String planData = "";
         
         try
         {
            if (query.indexOf("?") != -1)
               query = rewriteQuery(c, query);

            if (query != null)
            {
               for (int i = 0; i < planCount; i++)
                  executeStatement(c, EXPLAIN_ANALYZE_VERBOSE + " " + query, false);
                  
               List<String> l = executeStatement(c, EXPLAIN_ANALYZE_VERBOSE + " " + query);
               for (String s : l)
               {
                  plan += s;
                  plan += "\n";

                  if (s.startsWith("Planning time:"))
                  {
                     planData += s.substring(15);
                     planData += " / ";
                  }
                  else if (s.startsWith("Execution time:"))
                  {
                     planData += s.substring(16);
                  }
               }
            }

            Set<String> usedTables = getUsedTables(c, origQuery);
            
            writeReport(key, origQuery, query, usedTables, plan);
            result.put(key, planData);
         }
         catch (Exception e)
         {
            System.out.println("Original query: " + origQuery);
            System.out.println("Data: " + data);
            throw e;
         }
      }

      return result;
   }

   /**
    * Rewrite the query
    * @param c The connection
    * @param query The query
    * @return The new query
    */
   private static String rewriteQuery(Connection c, String query) throws Exception
   {
      net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil.parse(query);
      
      if (s instanceof Select)
      {
         Select select = (Select)s;
         
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
                  this.getBuffer().append(getData(c, currentColumn));
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

            if (plainSelect.getLimit() != null)
            {
               Limit limit = new Limit();
               limit.setRowCount(1L);
               plainSelect.setLimit(limit);
            }

            select.setSelectBody(plainSelect);
         }

         select.getSelectBody().accept(deparser);
         
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
            private int index = 0;
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
                  // Remove hack after 0.9.6 is out
                  boolean override = false;
                  if (currentColumn == null)
                  {
                     currentColumn = update.getColumns().get(index);
                     override = true;
                     index++;
                  }
                  
                  this.getBuffer().append(getData(c, currentColumn));

                  if (override)
                     currentColumn = null;
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
               this.getBuffer().append(column);
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               try
               {
                  this.getBuffer().append(getData(c, currentColumn));
               }
               catch (Exception e)
               {
                  e.printStackTrace();
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

      if (aliases.containsKey(tableName))
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
    * Init data for a table
    * @param c The connection
    * @param tableName The name of the table
    * @return The values
    */
   private static Map<String, Object> initTableData(Connection c, String tableName) throws Exception
   {
      Map<String, Object> values = new HashMap<>();

      Statement stmt = null;
      ResultSet rs = null;
      String query = "SELECT * FROM " + tableName + " LIMIT 1";
      try
      {
         stmt = c.createStatement();
         stmt.execute(query);
         rs = stmt.getResultSet();
         rs.next();
            
         ResultSetMetaData rsmd = rs.getMetaData();

         for (int i = 1; i <= rsmd.getColumnCount(); i++)
         {
            String columnName = rsmd.getColumnName(i);
            int columnType = rsmd.getColumnType(i);

            columnName = columnName.toUpperCase();
            values.put(columnName, getResultSetValue(rs, i, columnType));
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
      try
      {
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
      
      if (s instanceof Select)
      {
         Select select = (Select)s;

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
         };

         SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer)
         {
            @Override
            public void visit(Table table)
            {
               result.add(table.getName());
            }
         };
         expressionDeParser.setSelectVisitor(deparser);
         expressionDeParser.setBuffer(buffer);

         select.getSelectBody().accept(deparser);
      }
      else if (s instanceof Update)
      {
         Update update = (Update)s;

         for (Table table : update.getTables())
         {
            result.add(table.getName());
         }

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
         };

         SelectDeParser selectDeParser = new SelectDeParser()
         {
            @Override
            public void visit(Table table)
            {
               result.add(table.getName());
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
      }
      else if (s instanceof Delete)
      {
         Delete delete = (Delete)s;

         result.add(delete.getTable().getName());
      }

      for (String tableName : result)
      {
         if (!tables.containsKey(tableName))
         {
            Map<String, Integer> tableData = new HashMap<>();
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
               Set<String> pkInfo = new HashSet<>();

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

      if (f.exists())
      {
         configuration = new Properties();
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
         
         c = DriverManager.getConnection(url, user, password);

         SortedMap<String, String> queries = processQueries(c);
         writeIndex(queries);
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
}
