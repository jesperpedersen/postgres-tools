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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Generate a SQL workload for Replay
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class SQLLoadGenerator
{
   /** Default profile */
   private static final String DEFAULT_PROFILE = "sqlloadgenerator";

   /** Default number of rows */
   private static final int DEFAULT_ROWS = 1000;

   /** Default number of clients */
   private static final int DEFAULT_CLIENTS = 10;

   /** Default number of statements */
   private static final int DEFAULT_STATEMENTS = 10000;

   /** Default max number of statements per transaction */
   private static final int DEFAULT_MAX_STATEMENTS_PER_TRANSACTION = 5;

   /** Default SELECT mix */
   private static final int DEFAULT_MIX_SELECT = 70;

   /** Default UPDATE mix */
   private static final int DEFAULT_MIX_UPDATE = 15;

   /** Default INSERT mix */
   private static final int DEFAULT_MIX_INSERT = 10;

   /** Default DELETE mix */
   private static final int DEFAULT_MIX_DELETE = 5;

   /** Default COMMIT */
   private static final int DEFAULT_COMMIT = 100;

   /** Default ROLLBACK */
   private static final int DEFAULT_ROLLBACK = 0;

   /** Profile */
   private static Properties profile;

   /** Random */
   private static Random random = new Random();

   /** Column names   Table   Names */
   private static Map<String, List<String>> columnNames = new TreeMap<>();
   
   /** Column types   Table   Types */
   private static Map<String, List<String>> columnTypes = new TreeMap<>();
   
   /** Primary keys   Table   Column */
   private static Map<String, String> primaryKeys = new TreeMap<>();

   /** Active PKs     Table           PKs */
   private static Map<String, TreeSet<String>> activePKs = new TreeMap<>();

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
    * Write ddl.sql
    * @param profileName The name of the profile
    */
   private static void writeDDL(String profileName) throws Exception
   {
      List<String> l = new ArrayList<>();
      Enumeration<?> e = profile.propertyNames();

      // Tables
      while (e.hasMoreElements())
      {
         String key = (String)e.nextElement();
         if (key.startsWith("table."))
         {
            String tableName = key.substring(key.indexOf(".") + 1);
            String description = profile.getProperty(key);
            String primaryKey = null;
            int counter = 1;

            List<String> colNames = new ArrayList<>();
            List<String> colTypes = new ArrayList<>();
            List<String> colDescriptions = new ArrayList<>();
            
            while (profile.getProperty(tableName + ".column." + counter) != null)
            {
               String name = profile.getProperty(tableName + ".column." + counter);
               String type = profile.getProperty(tableName + ".column." + counter + ".type");
               String desc = profile.getProperty(tableName + ".column." + counter + ".description");
               String pk = profile.getProperty(tableName + ".column." + counter + ".primarykey");

               if (type == null)
                  type = "int";

               verifyType(type);

               if (desc != null && !"".equals(desc.trim()))
               {
                  StringBuilder sb = new StringBuilder();
                  sb.append("COMMENT ON COLUMN ");
                  sb.append(tableName);
                  sb.append(".");
                  sb.append(name);
                  sb.append(" IS \'");
                  sb.append(desc.trim());
                  sb.append("\';");
                  colDescriptions.add(sb.toString());
               }
               
               if (pk != null && !"".equals(pk.trim()))
               {
                  if (Boolean.parseBoolean(pk))
                  {
                     if (primaryKey == null)
                     {
                        primaryKey = name;
                        primaryKeys.put(tableName, primaryKey);
                     }
                     else
                     {
                        throw new Exception("Already have primary key \'" + primaryKey + "\' on table " + tableName);
                     }
                  }
               }

               colNames.add(name);
               colTypes.add(type);

               counter++;
            }

            if (colNames.size() > 0)
            {
               columnNames.put(tableName, colNames);
               columnTypes.put(tableName, colTypes);
            
               StringBuilder sb = new StringBuilder();
               sb.append("CREATE TABLE ");
               sb.append(tableName);
               sb.append(" (");
               for (int i = 0; i < colNames.size(); i++)
               {
                  sb.append(colNames.get(i));
                  sb.append(" ");
                  sb.append(colTypes.get(i));
                  if (primaryKey != null && primaryKey.equals(colNames.get(i)))
                     sb.append(" PRIMARY KEY");
                  if (i < colNames.size() - 1)
                     sb.append(", ");
               }
               sb.append(");");
            
               l.add(sb.toString());

               if (description != null && !"".equals(description.trim()))
               {
                  sb = new StringBuilder();
                  sb.append("COMMENT ON TABLE ");
                  sb.append(tableName);
                  sb.append(" IS \'");
                  sb.append(description.trim());
                  sb.append("\';");
                  l.add(sb.toString());
               }

               l.addAll(colDescriptions);

               if (primaryKey == null)
               {
                  sb = new StringBuilder();
                  sb.append("CREATE INDEX idx_");
                  sb.append(tableName);
                  sb.append("_");
                  sb.append(colNames.get(0));
                  sb.append(" ON ");
                  sb.append(tableName);
                  if (!isBTreeIndex(colTypes.get(0)))
                  {
                     sb.append(" USING HASH");
                  }
                  sb.append(" (");
                  sb.append(colNames.get(0));
                  sb.append(");");

                  l.add(sb.toString());
               }
            }
            else
            {
               System.out.println("No columns for " + tableName);
            }
         }
      }
      
      e = profile.propertyNames();

      // Indexes
      while (e.hasMoreElements())
      {
         String key = (String)e.nextElement();
         if (key.startsWith("index."))
         {
            String tableName = key.substring(key.indexOf(".") + 1, key.lastIndexOf("."));
            String cols = profile.getProperty(key);

            String colNames = cols;
            colNames = colNames.replace(" ", "_");
            colNames = colNames.replace(",", "");

            boolean hash = false;
            if (cols.indexOf(",") == -1)
            {
               List<String> names = columnNames.get(tableName);
               List<String> types = columnTypes.get(tableName);
               int index = names.indexOf(cols);

               if (index != -1 && !isBTreeIndex(types.get(index)))
                  hash = true;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("CREATE INDEX IF NOT EXISTS idx_");
            sb.append(tableName);
            sb.append("_");
            sb.append(colNames);
            sb.append(" ON ");
            sb.append(tableName);
            if (hash)
            {
               sb.append(" USING HASH");
            }
            sb.append(" (");
            sb.append(cols);
            sb.append(");");
            
            l.add(sb.toString());
         }
      }
      
      writeFile(Paths.get(profileName, "ddl.sql"), l);
   }

   /**
    * Write data.sql
    * @param profileName The name of the profile
    */
   private static void writeData(String profileName) throws Exception
   {
      List<String> l = new ArrayList<>();

      for (Map.Entry<String, List<String>> entry : columnNames.entrySet())
      {
         String tableName = entry.getKey();
         List<String> colNames = entry.getValue();
         List<String> colTypes = columnTypes.get(tableName);
         int rows = getRows(tableName);
         String pk = primaryKeys.get(tableName);

         for (int row = 1; row <= rows; row++)
         {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(tableName);
            sb.append(" (");
            for (int i = 0; i < colNames.size(); i++)
            {
               sb.append(colNames.get(i));
               if (i < colNames.size() - 1)
                  sb.append(", ");
            }
            sb.append(") VALUES (");
            for (int i = 0; i < colTypes.size(); i++)
            {
               if (mustEscape(colTypes.get(i)))
                  sb.append("\'");
               if (pk != null && colNames.get(i).equals(pk))
               {
                  sb.append(generatePrimaryKey(tableName, colTypes.get(i), row, false));
               }
               else
               {
                  sb.append(getData(colTypes.get(i), row, i == 0 ? false : true));
               }
               if (mustEscape(colTypes.get(i)))
                  sb.append("\'");
               if (i < colTypes.size() - 1)
                  sb.append(", ");
            }
            sb.append(");");
            
            l.add(sb.toString());
         }
      }
      
      l.add("");
      l.add("ANALYZE;");
      writeFile(Paths.get(profileName, "data.sql"), l);
   }

   /**
    * Write workload
    * @param profileName The name of the profile
    */
   private static void writeWorkload(String profileName) throws Exception
   {
      int clients = profile.getProperty("clients") != null ?
         Integer.parseInt(profile.getProperty("clients")) : DEFAULT_CLIENTS;
      int statements = profile.getProperty("statements") != null ?
         Integer.parseInt(profile.getProperty("statements")) : DEFAULT_STATEMENTS;
      int mspt = profile.getProperty("mspt") != null ?
         Integer.parseInt(profile.getProperty("mspt")) : DEFAULT_MAX_STATEMENTS_PER_TRANSACTION;
      int mixSelect = profile.getProperty("mix.select") != null ?
         Integer.parseInt(profile.getProperty("mix.select")) : DEFAULT_MIX_SELECT;
      int mixUpdate = profile.getProperty("mix.update") != null ?
         Integer.parseInt(profile.getProperty("mix.update")) : DEFAULT_MIX_UPDATE;
      int mixInsert = profile.getProperty("mix.insert") != null ?
         Integer.parseInt(profile.getProperty("mix.insert")) : DEFAULT_MIX_INSERT;
      int mixDelete = profile.getProperty("mix.delete") != null ?
         Integer.parseInt(profile.getProperty("mix.delete")) : DEFAULT_MIX_DELETE;
      int commit = profile.getProperty("commit") != null ?
         Integer.parseInt(profile.getProperty("commit")) : DEFAULT_COMMIT;
      int rollback = profile.getProperty("rollback") != null ?
         Integer.parseInt(profile.getProperty("rollback")) : DEFAULT_ROLLBACK;

      int totalSelect = 0;
      int totalUpdate = 0;
      int totalInsert = 0;
      int totalDelete = 0;
      int[] totalDistTx = new int[mspt];
      int totalTx = 0;
      int totalTxC = 0;
      int totalTxR = 0;
      int select = 0;
      int update = 0;
      int insert = 0;
      int delete = 0;
      int tx = 0;
      int txC = 0;
      int txR = 0;
      int[] distTx = new int[mspt];

      List<String> tableNames = new ArrayList<>();
      for (String tableName : columnNames.keySet())
      {
         tableNames.add(tableName);
      }
      
      for (int i = 1; i <= clients; i++)
      {
         select = 0;
         update = 0;
         insert = 0;
         delete = 0;
         tx = 0;
         txC = 0;
         txR = 0;
         distTx = new int[mspt];

         int cStatements = profile.getProperty("client." + i + ".statements") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".statements")) : statements;
         int cMixSelect = profile.getProperty("client." + i + ".mix.select") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.select")) : mixSelect;
         int cMixUpdate = profile.getProperty("client." + i + ".mix.update") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.update")) : mixUpdate;
         int cMixInsert = profile.getProperty("client." + i + ".mix.insert") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.insert")) : mixInsert;
         int cMixDelete = profile.getProperty("client." + i + ".mix.delete") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.delete")) : mixDelete;
         int cCommit = profile.getProperty("client." + i + ".commit") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".commit")) : commit;
         int cRollback = profile.getProperty("client." + i + ".rollback") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".rollback")) : rollback;

         List<String> l = new ArrayList<>();
         int statement = 0;
         while (statement <= cStatements)
         {
            int numberOfStatements = random.nextInt(mspt + 1);
            if (numberOfStatements == 0)
               numberOfStatements = 1;

            l.add("P");
            l.add("BEGIN");
            l.add("");
            l.add("");
            totalTx++;
            tx++;

            distTx[numberOfStatements - 1]++;
            totalDistTx[numberOfStatements - 1]++;

            for (int s = 0; s < numberOfStatements; s++)
            {
               String table = tableNames.get(random.nextInt(tableNames.size()));
               List<String> colNames = columnNames.get(table);
               List<String> colTypes = columnTypes.get(table);
               int type = 0;
               List<String> result = null;

               int m = random.nextInt(cMixSelect + cMixUpdate + cMixInsert + cMixDelete + 1);
               if (m <= mixSelect)
               {
                  type = 0;
               }
               else if (m <= cMixSelect + cMixUpdate)
               {
                  type = 1;
                  if (colNames.size() <= 1)
                     type = 0;
               }
               else if (m <= cMixSelect + cMixUpdate + cMixInsert)
               {
                  type = 2;
               }
               else
               {
                  type = 3;
               }

               switch (type)
               {
                  case 0:
                  {
                     result = generateSELECT(table, colNames, colTypes);
                     totalSelect++;
                     select++;
                     break;
                  }
                  case 1:
                  {
                     result = generateUPDATE(table, colNames, colTypes);
                     totalUpdate++;
                     update++;
                     break;
                  }
                  case 2:
                  {
                     result = generateINSERT(table, colNames, colTypes);
                     totalInsert++;
                     insert++;
                     break;
                  }
                  case 3:
                  {
                     result = generateDELETE(table, colNames, colTypes);
                     totalDelete++;
                     delete++;
                     break;
                  }
               }

               if (result != null)
               {
                  l.add("P");
                  l.add(result.get(0));
                  l.add(result.get(1));
                  l.add(result.get(2));
               }
            }

            l.add("P");
            int c = random.nextInt(cCommit + cRollback + 1);
            if (c <= cCommit)
            {
               l.add("COMMIT");
               totalTxC++;
               txC++;
            }
            else
            {
               l.add("ROLLBACK");
               totalTxR++;
               txR++;
            }
            l.add("");
            l.add("");

            statement += numberOfStatements + 2;
         }

         writeFile(Paths.get(profileName, i + ".cli"), l);

         System.out.println("Client: " + i);
         System.out.println("      TX: " + tx + " (" + txC + "/" + txR + ")");
         System.out.println("                 " + Arrays.toString(distTx));
         System.out.println("  SELECT: " + select);
         System.out.println("  UPDATE: " + update);
         System.out.println("  INSERT: " + insert);
         System.out.println("  DELETE: " + delete);
      }

      System.out.println("Total: ");
      System.out.println("      TX: " + totalTx + " (" + totalTxC + "/" + totalTxR + ")");
      System.out.println("                 " + Arrays.toString(totalDistTx));
      System.out.println("  SELECT: " + totalSelect);
      System.out.println("  UPDATE: " + totalUpdate);
      System.out.println("  INSERT: " + totalInsert);
      System.out.println("  DELETE: " + totalDelete);
   }

   /**
    * Generate SELECT statement
    */
   private static List<String> generateSELECT(String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int index = 0;

      if (pk != null)
         index = colNames.indexOf(pk);

      StringBuilder sql = new StringBuilder();
      sql.append("SELECT ");
      for (int col = 0; col < colNames.size(); col++)
      {
         sql.append(colNames.get(col));
         if (col < colNames.size() - 1)
            sql.append(", ");
      }
      sql.append(" FROM ");
      sql.append(table);
      sql.append(" WHERE ");
      sql.append(colNames.get(index));
      sql.append(" = ?");
               
      result.add(sql.toString());
      result.add(getJavaType(colTypes.get(index)));
      result.add(getData(colTypes.get(index), random.nextInt(getRows(table))));

      return result;
   }

   /**
    * Generate UPDATE statement
    */
   private static List<String> generateUPDATE(String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int index = 0;
      int rows = getRows(table);

      if (pk != null)
         index = colNames.indexOf(pk);

      StringBuilder sql = new StringBuilder();
      sql.append("UPDATE ");
      sql.append(table);
      sql.append(" SET ");
      for (int col = 0; col < colNames.size(); col++)
      {
         if (pk == null && col != 0)
         {
            sql.append(colNames.get(col));
            sql.append(" = ?");

            if (col < colNames.size() - 1)
               sql.append(", ");
         }
         else
         {
            if (col != index)
            {
               sql.append(colNames.get(col));
               sql.append(" = ?");

               if (col < colNames.size() - 1)
                  sql.append(", ");
            }
         }
      }
      sql.append(" WHERE ");
      sql.append(colNames.get(index));
      sql.append(" = ?");
               
      result.add(sql.toString());

      StringBuilder types = new StringBuilder();
      for (int col = 0; col < colNames.size(); col++)
      {
         if (pk == null && col != 0)
         {
            types.append(getJavaType(colTypes.get(col)));
            if (col < colNames.size() - 1)
               types.append("|");
         }
         else
         {
            if (col != index)
            {
               types.append(getJavaType(colTypes.get(col)));
               if (col < colNames.size() - 1)
                  types.append("|");
            }
         }
      }
      if (colNames.size() > 1)
         types.append("|");
      types.append(getJavaType(colTypes.get(index)));

      result.add(types.toString());

      StringBuilder values = new StringBuilder();
      for (int col = 0; col < colNames.size(); col++)
      {
         if (pk == null && col != 0)
         {
            values.append(getData(colTypes.get(col), random.nextInt(rows)));
            if (col < colNames.size() - 1)
               values.append("|");
         }
         else
         {
            if (col != index)
            {
               values.append(getData(colTypes.get(col), random.nextInt(rows)));
               if (col < colNames.size() - 1)
                  values.append("|");
            }
         }
      }
      if (colNames.size() > 1)
         values.append("|");
      values.append(getData(colTypes.get(index), random.nextInt(rows)));
      
      result.add(values.toString());

      return result;
   }

   /**
    * Generate INSERT statement
    */
   private static List<String> generateINSERT(String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int rows = getRows(table);

      StringBuilder sql = new StringBuilder();
      StringBuilder types = new StringBuilder();
      StringBuilder values = new StringBuilder();

      sql.append("INSERT INTO ");
      sql.append(table);
      sql.append(" (");
      for (int i = 0; i < colNames.size(); i++)
      {
         sql.append(colNames.get(i));
         if (i < colNames.size() - 1)
            sql.append(", ");
      }
      sql.append(") VALUES (");
      for (int i = 0; i < colTypes.size(); i++)
      {
         String colType = colTypes.get(i);
         sql.append("?");
         types.append(getJavaType(colTypes.get(i)));
         if (pk != null && colNames.get(i).equals(pk))
         {
            values.append(generatePrimaryKey(table, colType));
         }
         else
         {
            values.append(getData(colType, random.nextInt(rows)));
         }
         if (i < colTypes.size() - 1)
         {
            sql.append(", ");
            types.append("|");
            values.append("|");
         }
      }
      sql.append(")");
               
      result.add(sql.toString());
      result.add(types.toString());
      result.add(values.toString());

      return result;
   }

   /**
    * Generate DELETE statement
    */
   private static List<String> generateDELETE(String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int index = 0;
      int rows = getRows(table);

      if (pk != null)
         index = colNames.indexOf(pk);

      StringBuilder sql = new StringBuilder();
      sql.append("DELETE FROM ");
      sql.append(table);
      sql.append(" WHERE ");
      sql.append(colNames.get(index));
      sql.append(" = ?");
               
      result.add(sql.toString());
      result.add(getJavaType(colTypes.get(index)));
      if (pk != null)
      {
         String value = getPrimaryKey(table, colTypes.get(index), random.nextInt(rows));
         deletePrimaryKey(table, value);
         result.add(value);
      }
      else
      {
         result.add(getData(colTypes.get(index), random.nextInt(rows)));
      }

      return result;
   }
   
   /**
    * Verify type
    * @param type The type
    */
   private static void verifyType(String type) throws Exception
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
         case "bigserial":
         case "serial8":
         case "bit":
         case "bit varying":
         case "varbit":
         case "boolean":
         case "bool":
         case "bytea":
         case "character":
         case "char":
         case "character varying":
         case "varchar":
         case "date":
         case "double precision":
         case "float8":
         case "integer":
         case "int":
         case "int4":
         case "numeric":
         case "decimal":
         case "real":
         case "float4":
         case "smallint":
         case "int2":
         case "smallserial":
         case "serial2":
         case "serial":
         case "serial4":
         case "text":
         case "time":
         case "time without time zone":
         case "time with time zone":
         case "timetz":
         case "timestamp":
         case "timestamp without time zone":
         case "timestamp with time zone":
         case "timestamptz":
         case "uuid":
            break;
         default:
            throw new Exception("Unknown type: " + type);
      }
   }

   /**
    * Get Java type
    * @param type The type
    */
   private static String getJavaType(String type) throws Exception
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
            return Integer.toString(Types.BIGINT);
            //case "bigserial":
            //case "serial8":
         case "bit":
         case "bit varying":
         case "varbit":
            return Integer.toString(Types.BIT);
         case "boolean":
         case "bool":
            return Integer.toString(Types.BOOLEAN);
         case "bytea":
            return Integer.toString(Types.BINARY);
         case "character":
         case "char":
            return Integer.toString(Types.CHAR);
         case "character varying":
         case "varchar":
            return Integer.toString(Types.VARCHAR);
         case "date":
            return Integer.toString(Types.DATE);
         case "double precision":
         case "float8":
            return Integer.toString(Types.DOUBLE);
         case "integer":
         case "int":
         case "int4":
            return Integer.toString(Types.INTEGER);
         case "numeric":
            return Integer.toString(Types.NUMERIC);
         case "decimal":
            return Integer.toString(Types.DECIMAL);
         case "real":
         case "float4":
            return Integer.toString(Types.REAL);
         case "smallint":
         case "int2":
            return Integer.toString(Types.SMALLINT);
            //case "smallserial":
            //case "serial2":
            //case "serial":
            //case "serial4":
         case "text":
            return Integer.toString(Types.VARCHAR);
         case "time":
         case "time without time zone":
            return Integer.toString(Types.TIME);
         case "time with time zone":
         case "timetz":
            return Integer.toString(Types.TIME_WITH_TIMEZONE);
         case "timestamp":
         case "timestamp without time zone":
            return Integer.toString(Types.TIMESTAMP);
         case "timestamp with time zone":
         case "timestamptz":
            return Integer.toString(Types.TIMESTAMP_WITH_TIMEZONE);
         case "uuid":
            return Integer.toString(Types.OTHER);
      }
      throw new Exception("Unsupported type: " + type);
   }

   /**
    * Generate primary key
    * @param table The table
    * @param type The type
    */
   private static String generatePrimaryKey(String table, String type) throws Exception
   {
      return generatePrimaryKey(table, type, random.nextInt(Integer.MAX_VALUE));
   }

   /**
    * Generate primary key
    * @param table The table
    * @param type The type
    * @param row Hint for row number
    */
   private static String generatePrimaryKey(String table, String type, int row) throws Exception
   {
      return generatePrimaryKey(table, type, row, true);
   }

   /**
    * Generate primary key
    * @param table The table
    * @param type The type
    * @param row Hint for row number
    * @param r Random
    */
   private static String generatePrimaryKey(String table, String type, int row, boolean r) throws Exception
   {
      String newpk = getData(type, row, r);
      TreeSet<String> apks = activePKs.get(table);

      if (apks == null)
         apks = new TreeSet<>();

      while (apks.contains(newpk))
      {
         newpk = getData(type, row, r);
      }

      apks.add(newpk);
      activePKs.put(table, apks);

      return newpk;
   }

   /**
    * Get a random primary key
    * @param table The table
    * @param type The type
    * @param row Hint for row number
    * @return The primary key
    */
   private static String getPrimaryKey(String table, String type, int row) throws Exception
   {
      TreeSet<String> apks = activePKs.get(table);

      if (apks != null)
      {
         String[] vals = apks.toArray(new String[apks.size()]);
         return vals[random.nextInt(vals.length)];
      }
      else
      {
         return getData(type, row, true);
      }
   }

   /**
    * Delete primary key
    * @param table The table
    * @param pk The primary key
    */
   private static void deletePrimaryKey(String table, String pk) throws Exception
   {
      TreeSet<String> apks = activePKs.get(table);

      if (apks != null)
      {
         apks.remove(pk);
         activePKs.put(table, apks);
      }
   }

   /**
    * Get rows for a table
    * @param table The table name
    * @return The number of rows
    */
   private static int getRows(String table)
   {
      int defaultRows = profile.getProperty("rows") != null ?
         Integer.parseInt(profile.getProperty("rows")) : DEFAULT_ROWS;

      int rows = profile.getProperty(table + ".rows") != null ?
         Integer.parseInt(profile.getProperty(table + ".rows")) : defaultRows;

      return rows;
   }

   /**
    * Get data
    * @param type The type
    * @param row Hint for row number
    */
   private static String getData(String type, int row) throws Exception
   {
      return getData(type, row, true);
   }

   /**
    * Get data
    * @param type The type
    * @param row Hint for row number
    * @param r Should the data be random
    */
   private static String getData(String type, int row, boolean r) throws Exception
   {
      String validChars = "ABCDEFGHIJKLMNOPQRSTUVXWZabcdefghijklmnopqrstuvxwz0123456789";

      String base = type;
      int size = 0;
      if (base.indexOf("(") != -1)
      {
         size = Integer.parseInt(base.substring(base.indexOf("(") + 1, base.indexOf(")")));
         base = base.substring(0, base.indexOf("("));
      }
      switch (base.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
            if (r)
            {
               return Long.toString(random.nextLong());
            }
            else
            {
               return Long.toString(row);
            }
            //case "bigserial":
            //case "serial8":
         case "bit":
         case "bit varying":
         case "varbit":
         case "boolean":
         case "bool":
            return Boolean.toString(random.nextBoolean());
            //case "bytea":
         case "character":
         case "char":
            return Character.toString(validChars.charAt(random.nextInt(validChars.length())));
         case "character varying":
         case "varchar":
            if (size == 0)
               size = 16;
            StringBuilder sb = new StringBuilder(size);
            for (int i = 0; i < size; i++)
               sb.append(validChars.charAt(random.nextInt(validChars.length())));
            return sb.toString();
         case "date":
            return new java.sql.Date(System.currentTimeMillis()).toString();
         case "double precision":
         case "float8":
            if (r)
            {
               return Double.toString(random.nextDouble());
            }
            else
            {
               return Double.toString(row);
            }
         case "integer":
         case "int":
         case "int4":
            if (r)
            {
               return Integer.toString(random.nextInt(Integer.MAX_VALUE));
            }
            else
            {
               return Integer.toString(row);
            }
         case "numeric":
         case "decimal":
            if (r)
            {
               return new java.math.BigDecimal(random.nextInt(Integer.MAX_VALUE)).toPlainString();
            }
            else
            {
               return new java.math.BigDecimal(row).toPlainString();
            }
         case "real":
         case "float4":
            if (r)
            {
               return Float.toString(random.nextFloat());
            }
            else
            {
               return Float.toString(row);
            }
         case "smallint":
         case "int2":
            return Short.toString((short)random.nextInt(Short.MAX_VALUE));
            //case "smallserial":
            //case "serial2":
            //case "serial":
            //case "serial4":
         case "text":
            sb = new StringBuilder(256);
            for (int i = 0; i < 256; i++)
               sb.append(validChars.charAt(random.nextInt(validChars.length())));
            return sb.toString();
         case "time":
         case "time without time zone":
         case "time with time zone":
         case "timetz":
            return new java.sql.Time(System.currentTimeMillis()).toString();
         case "timestamp":
         case "timestamp without time zone":
         case "timestamp with time zone":
         case "timestamptz":
            return new java.sql.Timestamp(System.currentTimeMillis()).toString();
         case "uuid":
            return java.util.UUID.randomUUID().toString();
      }
      throw new Exception("Unsupported type: " + type);
   }
   
   /**
    * Must escape
    * @param type The type
    * @return True if escape needed, otherwise false
    */
   private static boolean mustEscape(String type) throws Exception
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "character":
         case "char":
         case "character varying":
         case "varchar":
         case "date":
         case "text":
         case "time":
         case "time without time zone":
         case "time with time zone":
         case "timetz":
         case "timestamp":
         case "timestamp without time zone":
         case "timestamp with time zone":
         case "timestamptz":
         case "uuid":
            return true;
      }
      return false;
   }

   /**
    * Is BTREE index
    * @param type The column type
    * @return True if BTREE, otherwise false (HASH)
    */
   private static boolean isBTreeIndex(String type)
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "character varying":
         case "varchar":
         case "text":
            return false;
      }

      return true;
   }

   /**
    * Setup
    * @param name The name of the profile
    */
   private static void setup(String name) throws Exception
   {
      File p = new File(name);
      if (p.exists())
      {
         Files.walk(Paths.get(name))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
      p.mkdir();
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length == 1 || args.length > 2)
         {
            System.out.println("Usage: SQLLoadGenerator [-c configuration.properties]");
            return;
         }
         
         String s = DEFAULT_PROFILE;
         if (args.length >= 1)
         {
            if (!"-c".equals(args[0]))
               throw new Exception("Unknown option: " + args[0]);
         }
         if (args.length == 2)
         {
            s = args[1];
            if (s.endsWith(".properties"))
               s = s.substring(0, s.lastIndexOf("."));
         }
         
         profile = new Properties();
         InputStream input = new FileInputStream(s + ".properties");
         profile.load(input);
         input.close();
         
         setup(s);
         writeDDL(s);
         writeData(s);
         writeWorkload(s);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
