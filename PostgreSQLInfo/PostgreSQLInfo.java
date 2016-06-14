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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Generate a HTML report of the PostgreSQL configuration
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class PostgreSQLInfo
{
   /**
    * Read data from a file
    * @param p The path of the file
    * @return The data
    */
   private static List<String> readFile(Path p) throws Exception
   {
      return Files.readAllLines(p);
   }

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
    * Write report.html
    */
   private static void writeReport(String dataPath, String xlogPath,
                                   List<String> pgHbaConf, SortedMap<String, String> postgresqlConf,
                                   String reportName) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>PostgreSQL Information</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>PostgreSQL Information</h1>");
      l.add("");

      l.add("DATA: " + dataPath);
      l.add("<br/>");
      l.add("XLOG: " + xlogPath);
      l.add("<br/>");

      l.add("<h2>pg_hba.conf</h2>");
      l.add("<pre>");
      for (String s : pgHbaConf)
      {
         s = s.replaceAll("\t", " ");
         s = s.replaceAll(" [ ]*", " ");
         l.add(s);
      }
      l.add("</pre>");
      
      l.add("<h2>postgresql.conf</h2>");
      l.add("<table>");
      l.add("  <thead>");
      l.add("    <tr align=\"left\">");
      l.add("      <th><b>Key</b></th>");
      l.add("      <th><b>Value</b></th>");
      l.add("    </tr>");
      l.add("  </thead>");

      l.add("  <tbody>");
      for (Map.Entry<String, String> entry : postgresqlConf.entrySet())
      {
         l.add("    <tr align=\"left\">");
         l.add("      <td>" + entry.getKey() + "</td>");
         l.add("      <td>" + entry.getValue() + "</td>");
         l.add("    </tr>");
      }
      l.add("  </tbody>");

      l.add("</table>");

      l.add("");
      l.add("<p/>");
      l.add("<div align=\"right\">");
      l.add("Generated on: " + new Date().toString());
      l.add("</div>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get(reportName), l);
   }

   /**
    * Parse access file
    * @param p The path
    * @return The data
    */
   private static List<String> parseAccess(Path p) throws Exception
   {
      List<String> l = new ArrayList<>();
      List<String> content = readFile(p);

      for (String s : content)
      {
         s = s.trim();
         if (!"".equals(s) && !s.startsWith("#"))
         {
            l.add(s);
         }
      }

      return l;
   }

   /**
    * Parse configuration file
    * @param p The path
    * @return The data
    */
   private static SortedMap<String, String> parseConfiguration(Path p) throws Exception
   {
      SortedMap<String, String> sm = new TreeMap<>();
      List<String> content = readFile(p);

      for (String s : content)
      {
         s = s.trim();
         if (!"".equals(s) && !s.startsWith("#"))
         {
            if (s.startsWith("include"))
            {
               if (s.startsWith("include_dir"))
               {
                  String directoryName = s.substring(s.indexOf("'") + 1, s.lastIndexOf("'"));
                  if (!directoryName.startsWith(File.pathSeparator))
                     directoryName = p.toAbsolutePath().toString() + File.pathSeparator + directoryName;

                  File directory = new File(directoryName);
                  for (File f : directory.listFiles())
                  {
                     sm.putAll(parseConfiguration(f.toPath()));
                  }
               }
               else if (s.startsWith("include_if_exists"))
               {
                  String fileName = s.substring(s.indexOf("'") + 1, s.lastIndexOf("'"));
                  Path include = null;
                  if (!fileName.startsWith(File.pathSeparator))
                  {
                     include = Paths.get(p.toAbsolutePath().toString(), fileName);
                  }
                  else
                  {
                     include = Paths.get(fileName);
                  }

                  if (include.toFile().exists())
                     sm.putAll(parseConfiguration(include));
               }
               else
               {
                  String fileName = s.substring(s.indexOf("'") + 1, s.lastIndexOf("'"));
                  Path include = null;
                  if (!fileName.startsWith(File.pathSeparator))
                  {
                     include = Paths.get(p.toAbsolutePath().toString(), fileName);
                  }
                  else
                  {
                     include = Paths.get(fileName);
                  }

                  sm.putAll(parseConfiguration(include));
               }
            }
            else
            {
               int index = s.indexOf("=");
               String key = s.substring(0, index);
               String value = s.substring(index + 1);

               if (value.indexOf("#") != -1)
                  value = value.substring(0, value.indexOf("#"));

               key = key.trim();
               value = value.trim();
                  
               sm.put(key, value);
            }
         }
      }

      return sm;
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length < 1 || args.length > 2)
         {
            System.out.println("Usage: PostgreSQLInfo <path/to/data_directory> [report.html]");
            return;
         }

         String pathToDataDirectory = args[0];
         String reportName = args.length == 2 ? args[1] : "report.html";
         
         Path pathToXLogDirectory = Paths.get(pathToDataDirectory, "pg_xlog");
         if (Files.isSymbolicLink(pathToXLogDirectory))
         {
            pathToXLogDirectory = Files.readSymbolicLink(pathToXLogDirectory);
         }
         
         Path hc = Paths.get(pathToDataDirectory, "pg_hba.conf");
         Path pc = Paths.get(pathToDataDirectory, "postgresql.conf");

         List<String> pgHbaConf = parseAccess(hc);
         SortedMap<String, String> postgresqlConf = parseConfiguration(pc);

         writeReport(pathToDataDirectory, pathToXLogDirectory.toAbsolutePath().toString(),
                     pgHbaConf, postgresqlConf, reportName);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
