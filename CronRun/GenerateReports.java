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
import java.io.FilenameFilter;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Generate HTML report based on the output from perf.sh
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class GenerateReports
{
   // The type of runs
   private static final String TYPE_1PC = "1pc";
   private static final String TYPE_1PCP = "1pcp";
   private static final String TYPE_2PC = "2pc";
   private static final String TYPE_RO = "ro";

   // The profile of runs
   private static final String PROFILE_OFF_LOGGED = "off-logged";
   private static final String PROFILE_OFF_UNLOGGED = "off-unlogged";
   private static final String PROFILE_ON_LOGGED = "on-logged";
   private static final String PROFILE_ON_UNLOGGED = "on-unlogged";

   /** The client counts */
   private static final Integer[] POINTS = new Integer[] {
      Integer.valueOf(1), Integer.valueOf(10), 
      Integer.valueOf(25), Integer.valueOf(50), 
      Integer.valueOf(75), Integer.valueOf(100), 
      Integer.valueOf(125), Integer.valueOf(150), 
      Integer.valueOf(175), Integer.valueOf(200)
   };

   /** All file data for on-logged runs */
   private static final List<String>[] FDATA_ON_LOGGED = new List[] {
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>()
   };
   
   /** All file data for on-unlogged runs */
   private static final List<String>[] FDATA_ON_UNLOGGED = new List[] {
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>()
   };
   
   /** All file data for off-logged runs */
   private static final List<String>[] FDATA_OFF_LOGGED = new List[] {
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>()
   };
   
   /** All file data for off-unlogged runs */
   private static final List<String>[] FDATA_OFF_UNLOGGED = new List[] {
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>()
   };

   //                       Date        Profile     Type        Count    TPS
   private static final Map<String, Map<String, Map<String, Map<Integer, Integer>>>> masterMap = new TreeMap<>();

   //                       Date    Commit
   private static final Map<String, String> commits = new TreeMap<>();

   //                       Date    Lines
   private static final Map<String, List<String>> environment = new TreeMap<>();
      
   //                       Date    Lines
   private static final Map<String, List<String>> configuration = new TreeMap<>();

   //                       Date    Lines
   private static final Map<String, List<String>> wals = new TreeMap<>();
      
   //                       Date    Change
   private static final Map<String, Integer> changes = new TreeMap<>();

   //                   Date=Note
   private static final Properties notes = new Properties();

   /**
    * Get the date from the file name
    * @param f The file
    * @return The date
    */
   private static String getDate(File f)
   {
      String[] s = f.getName().split("-");
      return s[0];
   }
   
   /**
    * Get the commit from the file name
    * @param f The file
    * @return The date
    */
   private static String getCommit(File f)
   {
      String[] s = f.getName().split("-");
      return s[1];
   }
   
   /**
    * Get the profile from the file name
    * @param f The file
    * @return The date
    */
   private static String getProfile(File f)
   {
      String[] s = f.getName().split("-");
      return s[2] + "-" + s[3];
   }

   /**
    * Get the type map for a specific date
    * @param date The date
    * @param profile The profile
    * @return The map
    */
   private static Map<String, Map<Integer, Integer>> getTypeMap(String date, String profile)
   {
      Map<String, Map<String, Map<Integer, Integer>>> profileMap = masterMap.get(date);
      if (profileMap == null)
      {
         profileMap = new TreeMap<>();
         masterMap.put(date, profileMap);
      }

      Map<String, Map<Integer, Integer>> typeMap = profileMap.get(profile);
      if (typeMap == null)
      {
         typeMap = new TreeMap<>();
         profileMap.put(profile, typeMap);
      }

      return typeMap;
   }

   /**
    * Get the data from a run
    * @param l The data from the run (output from perf.sh + pgbench)
    * @return A map with client to tps mapping
    */
   private static Map<Integer, Integer> getData(List<String> l)
   {
      Map<Integer, Integer> result = new TreeMap<>();
      Integer currentKey = null;
      for (String s : l)
      {
         if (s.startsWith("DATA"))
         {
            currentKey = Integer.valueOf(s.substring(s.indexOf(" ") + 1));
         }
         else if (s.contains("excluding connections"))
         {
            Integer value = Integer.valueOf(s.substring(6, s.indexOf(".")));
            result.put(currentKey, value);
         }
      }

      return result;
   }

   /**
    * Process the changes in environment / configuration
    * - 0: No changes
    * - 1: Change in configuration
    * - 2: Change in environment
    * - 3: Change in both
    * - 4: Note
    */
   private static void processChanges()
   {
      List<String> previousConfiguraton = null;
      List<String> previousEnvironment = null;
      
      for (String date : configuration.keySet())
      {
         List<String> c = configuration.get(date);
         List<String> e = environment.get(date);

         if (previousConfiguraton == null)
         {
            if (notes.getProperty(date) != null)
               changes.put(date, Integer.valueOf(4));
            else
               changes.put(date, Integer.valueOf(0));
         }
         else
         {
            if (notes.getProperty(date) != null)
            {
               changes.put(date, Integer.valueOf(4));
            }
            else
            {
               boolean changeConfiguration = false;
               boolean changeEnvironment = false;

               if (!c.equals(previousConfiguraton))
                  changeConfiguration = true;
            
               if (!e.equals(previousEnvironment))
                  changeEnvironment = true;

               if (changeConfiguration)
               {
                  if (changeEnvironment)
                     changes.put(date, Integer.valueOf(3));
                  else
                     changes.put(date, Integer.valueOf(1));
               }
               else if (changeEnvironment)
               {
                  changes.put(date, Integer.valueOf(2));
               }
               else
               {
                  changes.put(date, Integer.valueOf(0));
               }
            }
         }
         previousConfiguraton = c;
         previousEnvironment = e;
      }
   }
   
   /**
    * Process the data for a profile
    * @param date The date
    * @param profile The profile
    * @param d The data
    */
   private static void processData(String date, String profile, Map<String, Map<Integer, Integer>> d)
   {
      List<String>[] FDATA = null;

      if (PROFILE_OFF_LOGGED.equals(profile))
      {
         FDATA = FDATA_OFF_LOGGED;
      }
      else if (PROFILE_OFF_UNLOGGED.equals(profile))
      {
         FDATA = FDATA_OFF_UNLOGGED;
      }
      else if (PROFILE_ON_LOGGED.equals(profile))
      {
         FDATA = FDATA_ON_LOGGED;
      }
      else if (PROFILE_ON_UNLOGGED.equals(profile))
      {
         FDATA = FDATA_ON_UNLOGGED;
      }

      for (int i = 0; i < POINTS.length; i++)
      {
         Integer count = POINTS[i];
         Integer onepc = d.get(TYPE_1PC).get(count);
         Integer onepcp = d.get(TYPE_1PCP).get(count);
         Integer twopc = d.get(TYPE_2PC).get(count);
         Integer ro = d.get(TYPE_RO).get(count);
         
         if (onepc == null)
            onepc = Integer.valueOf(0);
         
         if (onepcp == null)
            onepcp = Integer.valueOf(0);
         
         if (twopc == null)
            twopc = Integer.valueOf(0);
         
         if (ro == null)
            ro = Integer.valueOf(0);
         
         StringBuilder sb = new StringBuilder();
         
         sb.append(date);
         sb.append(",");
         sb.append(onepc);
         sb.append(",");
         sb.append(onepcp);
         sb.append(",");
         sb.append(twopc);
         sb.append(",");
         sb.append(ro);
         
         if (FDATA[i].size() == 0)
            FDATA[i].add("Date,1PC,1PCP,2PC,RO");
         
         FDATA[i].add(sb.toString());
      }
   }

   /**
    * Write the data for the profiles (.csv)
    */
   private static void writeData() throws Exception
   {
      for (int i = 0; i < POINTS.length; i++)
      {
         Integer count = POINTS[i];
         String filename = "postgresql-off-logged-" + count + ".csv";
         
         writeFile(Paths.get("report", filename), FDATA_OFF_LOGGED[i]);
      }
      for (int i = 0; i < POINTS.length; i++)
      {
         Integer count = POINTS[i];
         String filename = "postgresql-off-unlogged-" + count + ".csv";
         
         writeFile(Paths.get("report", filename), FDATA_OFF_UNLOGGED[i]);
      }
      for (int i = 0; i < POINTS.length; i++)
      {
         Integer count = POINTS[i];
         String filename = "postgresql-on-logged-" + count + ".csv";
         
         writeFile(Paths.get("report", filename), FDATA_ON_LOGGED[i]);
      }
      for (int i = 0; i < POINTS.length; i++)
      {
         Integer count = POINTS[i];
         String filename = "postgresql-on-unlogged-" + count + ".csv";
         
         writeFile(Paths.get("report", filename), FDATA_ON_UNLOGGED[i]);
      }
   }

   /**
    * Process a profile for the daily report (.csv)
    * @param date The date
    * @param profile The profile
    * @param d The data
    */
   private static void processDaily(String date, String profile, Map<String, Map<Integer, Integer>> d) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("Count,1PC,1PCP,2PC,RO");
      
      for (int i = 0; i < POINTS.length; i++)
      {
         Integer count = POINTS[i];

         Integer onepc = d.get(TYPE_1PC).get(count);
         Integer onepcp = d.get(TYPE_1PCP).get(count);
         Integer twopc = d.get(TYPE_2PC).get(count);
         Integer ro = d.get(TYPE_RO).get(count);

         if (onepc == null)
            onepc = Integer.valueOf(0);

         if (onepcp == null)
            onepcp = Integer.valueOf(0);

         if (twopc == null)
            twopc = Integer.valueOf(0);

         if (ro == null)
            ro = Integer.valueOf(0);
         
         StringBuilder sb = new StringBuilder();

         sb.append(count);
         sb.append(",");
         sb.append(onepc);
         sb.append(",");
         sb.append(onepcp);
         sb.append(",");
         sb.append(twopc);
         sb.append(",");
         sb.append(ro);

         l.add(sb.toString());
      }

      writeFile(Paths.get("report", "postgresql-" + date + "-" + profile + ".csv"), l);
   }

   /**
    * Write a daily report
    * @param date The date
    * @param commit The commit identifier
    * @param env The environment
    * @param cfg The configuration
    * @param wal The WAL data
    */
   private static List<String> getDailyReport(String date, String commit, List<String> env, List<String> cfg, List<String> wal)
      throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<html>");
      l.add(" <head>");
      l.add("  <title>PostgreSQL Performance - " + date + "</title>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add(" </head>");
      l.add(" <body>");
      l.add("  <h1>Date: " + date + "</h1>");
      l.add("");
      l.add("  <a href=\"daily.html\">Back</a>");
      l.add("  <div id=\"graph-off-logged\" style=\"width:1024px; height:768px;\">");
      l.add("  </div>");
      l.add("  <div id=\"graph-off-unlogged\" style=\"width:1024px; height:768px;\">");
      l.add("  </div>");
      l.add("  <div id=\"graph-on-logged\" style=\"width:1024px; height:768px;\">");
      l.add("  </div>");
      l.add("  <div id=\"graph-on-unlogged\" style=\"width:1024px; height:768px;\">");
      l.add("  </div>");
      l.add("");

      l.add("  <h2>Commit</h2>");
      l.add("  <pre>");
      l.add(commit);
      l.add("  </pre>");
      l.add("  <a href=\"http://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=" + commit + "\">Link</a>");
      
      if (env != null)
      {
         l.add("<h2>Environment</h2>");
         l.add("<pre>");
         l.addAll(env);
         l.add("</pre>");
      }

      if (cfg != null)
      {
         l.add("<h2>Configuration</h2>");
         l.add("<pre>");

         Set<String> explicit = new TreeSet<>();
         
         for (String s : cfg)
         {
            String trimmed = s.trim();
            if (!"".equals(trimmed) && !trimmed.startsWith("#"))
            {
               int comment = trimmed.indexOf("#");
               if (comment != -1)
               {
                  explicit.add(trimmed.substring(0, comment));
               }
               else
               {
                  explicit.add(trimmed);
               }
            }
         }

         l.addAll(explicit);
         
         l.add("</pre>");
      }

      if (wal != null)
      {
         l.add("<h2>WAL</h2>");
         l.add("<pre>");
         l.addAll(wal);
         l.add("</pre>");
      }

      if (notes.getProperty(date) != null)
      {
         l.add("<h2>Note</h2>");
         l.add(notes.getProperty(date));
      }

      l.add("  <p/>");
      l.add("  <a href=\"daily.html\">Back</a>");

      l.addAll(readFooter());

      l.add("  <p/>");
      l.add("  <script type=\"text/javascript\">");
      l.add("   gOffLogged = new Dygraph(document.getElementById(\"graph-off-logged\"),");
      l.add("                            \"postgresql-" + date + "-off-logged" + ".csv\",");
      l.add("                            {");
      l.add("                              legend: 'always',");
      l.add("                              title: 'Off / Logged',");
      l.add("                              interactionModel: Dygraph.Interaction.nonInteractiveModel_,");
      l.add("                              ylabel: 'TPS',");
      l.add("                            }");
      l.add("   );");
      l.add("   gOffUnlogged = new Dygraph(document.getElementById(\"graph-off-unlogged\"),");
      l.add("                            \"postgresql-" + date + "-off-unlogged" + ".csv\",");
      l.add("                            {");
      l.add("                              legend: 'always',");
      l.add("                              title: 'Off / Unlogged',");
      l.add("                              interactionModel: Dygraph.Interaction.nonInteractiveModel_,");
      l.add("                              ylabel: 'TPS',");
      l.add("                            }");
      l.add("   );");
      l.add("   gOnLogged = new Dygraph(document.getElementById(\"graph-on-logged\"),");
      l.add("                            \"postgresql-" + date + "-on-logged" + ".csv\",");
      l.add("                            {");
      l.add("                              legend: 'always',");
      l.add("                              title: 'On / Logged',");
      l.add("                              interactionModel: Dygraph.Interaction.nonInteractiveModel_,");
      l.add("                              ylabel: 'TPS',");
      l.add("                            }");
      l.add("   );");
      l.add("   gOnUnlogged = new Dygraph(document.getElementById(\"graph-on-unlogged\"),");
      l.add("                            \"postgresql-" + date + "-on-unlogged" + ".csv\",");
      l.add("                            {");
      l.add("                              legend: 'always',");
      l.add("                              title: 'On / Unlogged',");
      l.add("                              interactionModel: Dygraph.Interaction.nonInteractiveModel_,");
      l.add("                              ylabel: 'TPS',");
      l.add("                            }");
      l.add("   );");
      l.add("  </script>");
      l.add(" </body>");
      l.add("</html>");

      return l;
   }

   /**
    * Write daily.html
    */
   private static void writeDaily() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<html>");
      l.add(" <head>");
      l.add("  <title>PostgreSQL Performance - Daily Reports</title>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add(" </head>");
      l.add(" <body>");
      l.add("  <h1>Daily Reports</h1>");
      l.add("");
      l.add("  <a href=\"index.html\">Back</a>");
      l.add("  <ul>");
      for (String date : masterMap.keySet())
      {
         StringBuilder sb = new StringBuilder();
         sb.append("    <li><a href=\"postgresql-");
         sb.append(date);
         sb.append(".html\">");
         sb.append(date);
         sb.append("</a>");

         Integer change = changes.get(date);
         if (change.intValue() == 1)
         {
            sb.append(" (Configuration)");
         }
         else if (change.intValue() == 2)
         {
            sb.append(" (Environment)");
         }
         else if (change.intValue() == 3)
         {
            sb.append(" (Both)");
         }
         else if (change.intValue() == 4)
         {
            sb.append(" (Note)");
         }
         
         sb.append("</li>");
         
         l.add(sb.toString());
      }
      l.add("  </ul>");
      l.add("  <a href=\"index.html\">Back</a>");

      l.addAll(readFooter());

      l.add(" </body>");
      l.add("</html>");

      writeFile(Paths.get("report", "daily.html"), l);
   }

   /**
    * Write max.html
    * - And each max report
    */
   private static void writeMax() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<html>");
      l.add(" <head>");
      l.add("  <title>PostgreSQL Performance - Max Reports</title>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add(" </head>");
      l.add(" <body>");
      l.add("  <h1>Max Reports</h1>");
      l.add("");
      l.add("  <a href=\"index.html\">Back</a>");
      l.add("  <ul>");
      l.add("    <li><a href=\"max-" + PROFILE_OFF_LOGGED + ".html\">Off / Logged</a></li>");
      l.add("    <li><a href=\"max-" + PROFILE_OFF_UNLOGGED + ".html\">Off / Unlogged</a></li>");
      l.add("    <li><a href=\"max-" + PROFILE_ON_LOGGED + ".html\">On / Logged</a></li>");
      l.add("    <li><a href=\"max-" + PROFILE_ON_UNLOGGED + ".html\">On / Unlogged</a></li>");
      l.add("  </ul>");
      l.add("  <a href=\"index.html\">Back</a>");

      l.addAll(readFooter());

      l.add(" </body>");
      l.add("</html>");

      writeFile(Paths.get("report", "max.html"), l);
      writeMax("Off / Logged", PROFILE_OFF_LOGGED);
      writeMax("Off / Unlogged", PROFILE_OFF_UNLOGGED);
      writeMax("On / Logged", PROFILE_ON_LOGGED);
      writeMax("On / Unlogged", PROFILE_ON_UNLOGGED);
   }

   /**
    * Create the data that represents if changes in environment/configuration has happended
    * @return The data
    */
   private static String createChanges()
   {
      StringBuilder sb = new StringBuilder();
      sb.append("     var changes = [");

      Collection<Integer> c = changes.values();
      Iterator<Integer> it = c.iterator();
      while (it.hasNext())
      {
         sb.append(it.next());
         if (it.hasNext())
            sb.append(", ");
      }
      
      sb.append("];");

      return sb.toString();
   }

   /**
    * Create the data that represents the graphs for a profile
    * @param name The name of the graph
    * @param id The id of the graph
    * @param file The file name which contains the graph data
    * @param title The title of the graph
    * @return The data
    */
   private static List<String> createGraph(String name, String id, String file, String title)
   {
      List<String> l = new ArrayList<>();

      l.add(name + " = new Dygraph(document.getElementById(\"" + id + "\"),");
      l.add("                     \"" + file + "\",");
      l.add("                     {");
      l.add("                       legend: 'always',");
      l.add("                       title: '" + title + "',");
      l.add("                       showRoller: true,");
      l.add("                       rollPeriod: 1,");
      l.add("                       ylabel: 'TPS',");
      l.add("                       underlayCallback: function(canvas, area, g) {");
      l.add("");
      l.add("                         function highlight_configuration(x_start, x_end) {");
      l.add("                           var canvas_left_x = g.toDomXCoord(x_start);");
      l.add("                           var canvas_right_x = g.toDomXCoord(x_end);");
      l.add("                           var canvas_width = canvas_right_x - canvas_left_x;");
      l.add("                           canvas.fillStyle = \"rgba(255, 0, 0, 1.0)\";");
      l.add("                           canvas.fillRect(canvas_left_x, area.y, canvas_width, area.h);");
      l.add("                         }");
      l.add("");
      l.add("                         function highlight_environment(x_start, x_end) {");
      l.add("                           var canvas_left_x = g.toDomXCoord(x_start);");
      l.add("                           var canvas_right_x = g.toDomXCoord(x_end);");
      l.add("                           var canvas_width = canvas_right_x - canvas_left_x;");
      l.add("                           canvas.fillStyle = \"rgba(0, 255, 0, 1.0)\";");
      l.add("                           canvas.fillRect(canvas_left_x, area.y, canvas_width, area.h);");
      l.add("                         }");
      l.add("");
      l.add("                         function highlight_both(x_start, x_end) {");
      l.add("                           var canvas_left_x = g.toDomXCoord(x_start);");
      l.add("                           var canvas_right_x = g.toDomXCoord(x_end);");
      l.add("                           var canvas_width = canvas_right_x - canvas_left_x;");
      l.add("                           canvas.fillStyle = \"rgba(0, 0, 255, 1.0)\";");
      l.add("                           canvas.fillRect(canvas_left_x, area.y, canvas_width, area.h);");
      l.add("                         }");
      l.add("");
      l.add("                         function highlight_note(x_start, x_end) {");
      l.add("                           var canvas_left_x = g.toDomXCoord(x_start);");
      l.add("                           var canvas_right_x = g.toDomXCoord(x_end);");
      l.add("                           var canvas_width = canvas_right_x - canvas_left_x;");
      l.add("                           canvas.fillStyle = \"rgba(255, 255, 0, 1.0)\";");
      l.add("                           canvas.fillRect(canvas_left_x, area.y, canvas_width, area.h);");
      l.add("                         }");
      l.add("");
      l.add("                         var min_data_x = g.getValue(0,0);");
      l.add("                         var max_data_x = g.getValue(g.numRows()-1,0);");
      l.add("                         var w = min_data_x;");
      l.add("");
      l.add("                         if (changes[0] > 0) {");
      l.add("                            if (changes[0] == 1)");
      l.add("                               highlight_environment(w, w + 12*3600*1000);");
      l.add("                            else if (changes[0] == 2)");
      l.add("                               highlight_configuration(w, w + 12*3600*1000);");
      l.add("                            else");
      l.add("                               highlight_both(w, w + 12*3600*1000);");
      l.add("                         }");
      l.add("");
      l.add("                         w += 12*3600*1000;");
      l.add("");
      l.add("                         var counter = 1;");
      l.add("");
      l.add("                         while (w < max_data_x) {");
      l.add("                            var start_x_highlight = w;");
      l.add("                            var end_x_highlight = w + 24*3600*1000;");
      l.add("");
      l.add("                            if (start_x_highlight < min_data_x) {");
      l.add("                               start_x_highlight = min_data_x;");
      l.add("                            }");
      l.add("                            if (end_x_highlight > max_data_x) {");
      l.add("                               end_x_highlight = max_data_x;");
      l.add("                            }");
      l.add("");
      l.add("                            if (changes[counter] > 0) {");
      l.add("                               if (changes[counter] == 1)");
      l.add("                                  highlight_environment(start_x_highlight,end_x_highlight);");
      l.add("                               else if (changes[counter] == 2)");
      l.add("                                  highlight_configuration(start_x_highlight,end_x_highlight);");
      l.add("                               else if (changes[counter] == 3)");
      l.add("                                  highlight_both(start_x_highlight,end_x_highlight);");
      l.add("                               else");
      l.add("                                  highlight_note(start_x_highlight,end_x_highlight);");
      l.add("                            }");
      l.add("");
      l.add("                            w += 24*3600*1000;");
      l.add("                            counter++;");
      l.add("                         }");
      l.add("                       }");
      l.add("                     }");
      l.add(");");

      return l;
   }

   /**
    * Write the max report based on the highest TPS for each daily run
    * @param name The name of the max report
    * @param profile The profile file name
    */
   private static void writeMax(String name, String profile) throws Exception
   {
      //  Date        Type    TPS
      Map<String, Map<String, Integer>> max = new TreeMap<>();
      Map<String, List<String>> d = new TreeMap<>();

      for (Map.Entry<String, Map<String, Map<String, Map<Integer, Integer>>>> entry : masterMap.entrySet())
      {
         String date = entry.getKey();
         Map<String, Integer> dateData = new TreeMap<>();
         
         for (Map.Entry<String, Map<Integer, Integer>> inner : entry.getValue().get(profile).entrySet())
         {
            String type = inner.getKey();
            Integer value = new Integer(0);
            for (Integer i : inner.getValue().values())
            {
               if (i.intValue() > value.intValue())
                  value = i;
            }
            dateData.put(type, value);
         }

         max.put(date, dateData);
      }

      for (Map.Entry<String, Map<String, Integer>> entry : max.entrySet())
      {
         String date = entry.getKey();
         Map<String, Integer> v = entry.getValue();

         StringBuilder sb = new StringBuilder();
         sb.append(date);
         sb.append(",");
         sb.append(v.get(TYPE_1PC));

         List<String> l = d.get(TYPE_1PC);
         if (l == null)
         {
            l = new ArrayList<>();
            l.add("Date,1PC");
            d.put(TYPE_1PC, l);
         }

         l.add(sb.toString());
         
         sb = new StringBuilder();
         sb.append(date);
         sb.append(",");
         sb.append(v.get(TYPE_1PCP));

         l = d.get(TYPE_1PCP);
         if (l == null)
         {
            l = new ArrayList<>();
            l.add("Date,1PCP");
            d.put(TYPE_1PCP, l);
         }

         l.add(sb.toString());
         
         sb = new StringBuilder();
         sb.append(date);
         sb.append(",");
         sb.append(v.get(TYPE_2PC));

         l = d.get(TYPE_2PC);
         if (l == null)
         {
            l = new ArrayList<>();
            l.add("Date,2PC");
            d.put(TYPE_2PC, l);
         }

         l.add(sb.toString());
         
         sb = new StringBuilder();
         sb.append(date);
         sb.append(",");
         sb.append(v.get(TYPE_RO));

         l = d.get(TYPE_RO);
         if (l == null)
         {
            l = new ArrayList<>();
            l.add("Date,RO");
            d.put(TYPE_RO, l);
         }

         l.add(sb.toString());
      }

      writeFile(Paths.get("report", "postgresql-max-" + profile + "-" + TYPE_1PC + ".csv"), d.get(TYPE_1PC));
      writeFile(Paths.get("report", "postgresql-max-" + profile + "-" + TYPE_1PCP + ".csv"), d.get(TYPE_1PCP));
      writeFile(Paths.get("report", "postgresql-max-" + profile + "-" + TYPE_2PC + ".csv"), d.get(TYPE_2PC));
      writeFile(Paths.get("report", "postgresql-max-" + profile + "-" + TYPE_RO + ".csv"), d.get(TYPE_RO));

      List<String> l = new ArrayList<>();
      l.add("<html>");
      l.add("<head>");
      l.add("<title>PostgreSQL Performance - Max: " + name + "</title>");
      l.add("<script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Max: " + name + "</h1>");
      l.add("<a href=\"max.html\">Back</a>");
      l.add("<p/>");
      l.add("<table border=\"5\">");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph1pc\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph1pcp\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph2pc\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graphro\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("</table>");
      l.add("<p/>");
      l.add("<a href=\"max.html\">Back</a>");
      l.add("");

      l.addAll(readFooter());

      l.add("<script type=\"text/javascript\">");

      l.add(createChanges());
      
      l.addAll(createGraph("g1pc", "graph1pc", "postgresql-max-" + profile + "-1pc.csv", "1PC"));
      l.addAll(createGraph("g1pcp", "graph1pcp", "postgresql-max-" + profile + "-1pcp.csv", "1PCP"));
      l.addAll(createGraph("g2pc", "graph2pc", "postgresql-max-" + profile + "-2pc.csv", "2PC"));
      l.addAll(createGraph("gro", "graphro", "postgresql-max-" + profile + "-ro.csv", "RO"));

      l.add("</script>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "max-" + profile + ".html"), l);
   }
   
   /**
    * Read data from machine.html
    * @return The data
    */
   private static List<String> readMachine() throws Exception
   {
      File machine = new File("machine.html");

      if (machine.exists())
      {
         return Files.readAllLines(machine.toPath());
      }

      return new ArrayList<String>();
   }
   
   /**
    * Read data from footer.html
    * @return The data
    */
   private static List<String> readFooter() throws Exception
   {
      File footer = new File("footer.html");

      if (footer.exists())
      {
         return Files.readAllLines(footer.toPath());
      }

      return new ArrayList<String>();
   }
   
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
    * Write profiles.html
    * - And each profile
    */
   private static void writeProfiles() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<html>");
      l.add(" <head>");
      l.add("  <title>PostgreSQL Performance - Profile Reports</title>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add(" </head>");
      l.add(" <body>");
      l.add("  <h1>Profile Reports</h1>");
      l.add("");
      l.add("  <a href=\"index.html\">Back</a>");
      l.add("  <ul>");
      l.add("    <li><a href=\"profile-" + PROFILE_OFF_LOGGED + ".html\">Off / Logged</a></li>");
      l.add("    <li><a href=\"profile-" + PROFILE_OFF_UNLOGGED + ".html\">Off / Unlogged</a></li>");
      l.add("    <li><a href=\"profile-" + PROFILE_ON_LOGGED + ".html\">On / Logged</a></li>");
      l.add("    <li><a href=\"profile-" + PROFILE_ON_UNLOGGED + ".html\">On / Unlogged</a></li>");
      l.add("  </ul>");
      l.add("  <a href=\"index.html\">Back</a>");

      l.addAll(readFooter());

      l.add(" </body>");
      l.add("</html>");

      writeFile(Paths.get("report", "profiles.html"), l);
      writeProfile("Off / Logged", PROFILE_OFF_LOGGED);
      writeProfile("Off / Unlogged", PROFILE_OFF_UNLOGGED);
      writeProfile("On / Logged", PROFILE_ON_LOGGED);
      writeProfile("On / Unlogged", PROFILE_ON_UNLOGGED);
   }

   /**
    * Write a profile report
    * @param name The name of the profile
    * @param profile The profile identifier
    */
   private static void writeProfile(String name, String profile) throws Exception
   {
      List<String> l = new ArrayList<>();
      l.add("<html>");
      l.add("<head>");
      l.add("<title>PostgreSQL Performance - Profile: " + name + "</title>");
      l.add("<script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Profile: " + name + "</h1>");
      l.add("<a href=\"profiles.html\">Back</a>");
      l.add("<p/>");
      l.add("<table border=\"5\">");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph1\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph10\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph25\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph50\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph75\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph100\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph125\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph150\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph175\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph200\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("</table>");
      l.add("<p/>");
      l.add("<a href=\"profiles.html\">Back</a>");
      l.add("");

      l.addAll(readFooter());

      l.add("<script type=\"text/javascript\">");

      l.add(createChanges());
      
      l.addAll(createGraph("g1", "graph1", "postgresql-" + profile + "-1.csv", "Client 1"));
      l.addAll(createGraph("g10", "graph10", "postgresql-" + profile + "-10.csv", "Client 10"));
      l.addAll(createGraph("g25", "graph25", "postgresql-" + profile + "-25.csv", "Client 25"));
      l.addAll(createGraph("g50", "graph50", "postgresql-" + profile + "-50.csv", "Client 50"));
      l.addAll(createGraph("g75", "graph75", "postgresql-" + profile + "-75.csv", "Client 75"));
      l.addAll(createGraph("g100", "graph100", "postgresql-" + profile + "-100.csv", "Client 100"));
      l.addAll(createGraph("g125", "graph125", "postgresql-" + profile + "-125.csv", "Client 125"));
      l.addAll(createGraph("g150", "graph150", "postgresql-" + profile + "-150.csv", "Client 150"));
      l.addAll(createGraph("g175", "graph175", "postgresql-" + profile + "-175.csv", "Client 175"));
      l.addAll(createGraph("g200", "graph200", "postgresql-" + profile + "-200.csv", "Client 200"));

      l.add("</script>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "profile-" + profile + ".html"), l);
   }
   
   /**
    * Write index.html
    */
   private static void writeIndex() throws Exception
   {
      List<String> l = new ArrayList<>();
      l.add("<html>");
      l.add("<head>");
      l.add("  <title>PostgreSQL Performance</title>");
      l.add("  <script type=\"text/javascript\" src=\"dygraph-combined-dev.js\"></script>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>PostgreSQL Performance</h1>");
      l.add("");
      l.add("<a href=\"daily.html\">Daily reports</a>");
      l.add("<a href=\"profiles.html\">Profile reports</a>");
      l.add("<a href=\"max.html\">Max reports</a>");
      l.add("<p/>");
      l.add("");
      l.add("<h2>Off / Logged </h2>");
      l.add("<table border=\"5\">");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph1\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph10\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph25\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph50\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph75\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph100\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph125\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph150\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("");
      l.add("<tr>");
      l.add("<td>");
      l.add("<div id=\"graph175\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("<td>");
      l.add("<div id=\"graph200\" style=\"width:700px; height:500px;\">");
      l.add("</div>");
      l.add("</td>");
      l.add("</tr>");
      l.add("</table>");
      l.add("");
      l.add("<h2>Description</h2>");
      l.add("<table>");
      l.add("<tr>");
      l.add("<td><b>Green</b></td>");
      l.add("<td>Change in configuration</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Red</b></td>");
      l.add("<td>Change in environment</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Blue</b></td>");
      l.add("<td>Change in both configuration and environment</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Yellow</b></td>");
      l.add("<td>Note</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Off</b></td>");
      l.add("<td>synchronous_commit = off</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>On</b></td>");
      l.add("<td>synchronous_commit = on</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Logged</b></td>");
      l.add("<td></td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>Unlogged</b></td>");
      l.add("<td>--unlogged-tables</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>1PC</b></td>");
      l.add("<td>pgbench standard</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>1PCP</b></td>");
      l.add("<td>pgbench w/ -M prepared</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>2PC</b></td>");
      l.add("<td>pgbench, like 1PC, but with PREPARE + COMMIT</td>");
      l.add("</tr>");
      l.add("<tr>");
      l.add("<td><b>RO</b></td>");
      l.add("<td>pgbench w/ -S</td>");
      l.add("</tr>");
      l.add("</table>");
      l.add("");

      l.addAll(readMachine());

      l.addAll(readFooter());

      l.add("");
      l.add("<script type=\"text/javascript\">");

      l.add(createChanges());
      
      l.addAll(createGraph("g1", "graph1", "postgresql-off-logged-1.csv", "Client 1"));
      l.addAll(createGraph("g10", "graph10", "postgresql-off-logged-10.csv", "Client 10"));
      l.addAll(createGraph("g25", "graph25", "postgresql-off-logged-25.csv", "Client 25"));
      l.addAll(createGraph("g50", "graph50", "postgresql-off-logged-50.csv", "Client 50"));
      l.addAll(createGraph("g75", "graph75", "postgresql-off-logged-75.csv", "Client 75"));
      l.addAll(createGraph("g100", "graph100", "postgresql-off-logged-100.csv", "Client 100"));
      l.addAll(createGraph("g125", "graph125", "postgresql-off-logged-125.csv", "Client 125"));
      l.addAll(createGraph("g150", "graph150", "postgresql-off-logged-150.csv", "Client 150"));
      l.addAll(createGraph("g175", "graph175", "postgresql-off-logged-175.csv", "Client 175"));
      l.addAll(createGraph("g200", "graph200", "postgresql-off-logged-200.csv", "Client 200"));

      l.add("</script>");
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Setup the report
    * - Create directory
    * - Copy dygraph
    */
   private static void setup() throws Exception
   {
      File report = new File("report");
      report.mkdir();
         
      Files.copy(Paths.get("dygraph-combined-dev.js"), Paths.get("report", "dygraph-combined-dev.js"), StandardCopyOption.REPLACE_EXISTING);
   }

   /**
    * Load the notes for the runs (notes.properties)
    */
   private static void loadNotes() throws Exception
   {
      File f = new File("notes.properties");

      if (f.exists())
      {
         FileInputStream fis = null;
         try
         {
            fis = new FileInputStream(f);
            notes.load(fis);
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
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         Path directory = FileSystems.getDefault().getPath(".");
         File[] txtFiles = directory.toFile().listFiles(new TxtFilter());

         Arrays.sort(txtFiles, new Comparator<File>() {
            public int compare(File f1, File f2)
            {
               return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            }
         });

         setup();
         loadNotes();
         
         for (File f : txtFiles)
         {
            boolean ok = true;
            String date = getDate(f);
            String commit = getCommit(f);
            
            if (!commits.containsKey(date))
            {
               commits.put(date, commit);
            }
            else
            {
               if (!commits.get(date).equals(commit))
                  ok = false;
            }

            if (ok)
            {
               List<String> lines = readFile(f.toPath());

               if (f.getName().endsWith("1pc-standard.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);

                  getTypeMap(date, profile).put(TYPE_1PC, data);
               }
               else if (f.getName().endsWith("1pc-prepared.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(date, profile).put(TYPE_1PCP, data);
               }
               else if (f.getName().endsWith("2pc-standard.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(date, profile).put(TYPE_2PC, data);
               }
               else if (f.getName().endsWith("-readonly.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(date, profile).put(TYPE_RO, data);
               }
               else if (f.getName().endsWith("-postgresql-conf.txt"))
               {
                  configuration.put(date, lines);
               }
               else if (f.getName().endsWith("-environment.txt"))
               {
                  environment.put(date, lines);
               }
               else if (f.getName().endsWith("-wal.txt"))
               {
                  wals.put(date, lines);
               }
            }
         }

         processChanges();
         
         for (Map.Entry<String, Map<String, Map<String, Map<Integer, Integer>>>> entry : masterMap.entrySet())
         {
            for (Map.Entry<String, Map<String, Map<Integer, Integer>>> profile : entry.getValue().entrySet())
            {
               processData(entry.getKey(), profile.getKey(), profile.getValue());
               processDaily(entry.getKey(), profile.getKey(), profile.getValue());
            }

            writeFile(Paths.get("report", "postgresql-" + entry.getKey() + ".html"),
                      getDailyReport(entry.getKey(),
                                     commits.get(entry.getKey()),
                                     environment.get(entry.getKey()),
                                     configuration.get(entry.getKey()),
                                     wals.get(entry.getKey())));
         }

         writeData();
         writeIndex();
         writeDaily();
         writeProfiles();
         writeMax();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   static class TxtFilter implements FilenameFilter
   {
      public boolean accept(File dir, String name)
      {
         return name.endsWith(".txt");
      }
   }
}
