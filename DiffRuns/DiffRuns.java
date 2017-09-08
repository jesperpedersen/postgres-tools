/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Jesper Pedersen <jesper.pedersen@comcast.net>
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Compare two performance runs with each other
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class DiffRuns
{
   // Types
   private static final String TYPE_1PC = "1pc";
   private static final String TYPE_1PCP = "1pcp";
   private static final String TYPE_2PC = "2pc";
   private static final String TYPE_RO = "ro";
   private static final String TYPE_SSUP = "ssup";

   // Profile configurations
   private static final String PROFILE_OFF_LOGGED = "off-logged";
   private static final String PROFILE_OFF_UNLOGGED = "off-unlogged";
   private static final String PROFILE_ON_LOGGED = "on-logged";
   private static final String PROFILE_ON_UNLOGGED = "on-unlogged";

   // Profiles
   private static final String[] PROFILES = new String[] {
      PROFILE_OFF_LOGGED, PROFILE_OFF_UNLOGGED,
      PROFILE_ON_LOGGED, PROFILE_ON_UNLOGGED
   };

   // Client counts
   private static SortedSet<Integer> POINTS = new TreeSet<>();
   
   //                       Commit      Profile     Type        Count    TPS
   private static final Map<String, Map<String, Map<String, Map<Integer, Integer>>>> masterMap = new TreeMap<>();
   
   /**
    * Get the commit from the file name
    * @param f The file
    * @return The commit
    */
   private static String getCommit(File f)
   {
      String[] s = f.getName().split("-");

      if (s.length == 0)
         return "";
      
      return s[1];
   }
   
   /**
    * Get the profile from the file name
    * @param f The file
    * @return The profile
    */
   private static String getProfile(File f)
   {
      String[] s = f.getName().split("-");
      return s[2] + "-" + s[3];
   }

   /**
    * Get the type map for a specific commit
    * @param commit The commit
    * @param profile The profile
    * @return The map
    */
   private static Map<String, Map<Integer, Integer>> getTypeMap(String commit, String profile)
   {
      Map<String, Map<String, Map<Integer, Integer>>> profileMap = masterMap.get(commit);
      if (profileMap == null)
      {
         profileMap = new TreeMap<>();
         masterMap.put(commit, profileMap);
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
    * @param l The data from the run (output from run.sh + pgbench)
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
            POINTS.add(currentKey);
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
    * Write the data
    * @param origCommit The original commit
    * @param patchCommit The patch commit
    */
   private static void writeData(String origCommit, String patchCommit) throws Exception
   {
      List<String> l = new ArrayList<>();

      for (String profile : PROFILES)
      {
         Map<String, Map<Integer, Integer>> origData = masterMap.get(origCommit).get(profile);
         Map<String, Map<Integer, Integer>> patchData = masterMap.get(patchCommit).get(profile);

         if (origData == null || patchData == null)
            continue;
         
         l.add(profile);
         l.add("Count,M1PC,M1PCP,M2PC,MSSUP,MRO,P1PC,P1PCP,P2PC,PSSUP,PRO,1PC%,1PCP%,2PC%,SSUP%,RO%");
            
         for (Integer count : POINTS)
         {
            StringBuilder sb = new StringBuilder();

            Integer o1pc = origData.get(TYPE_1PC) != null ? origData.get(TYPE_1PC).get(count) : Integer.valueOf(0);
            if (o1pc == null)
               o1pc = Integer.valueOf(0);
            Integer o1pcp = origData.get(TYPE_1PCP) != null ? origData.get(TYPE_1PCP).get(count) : Integer.valueOf(0);
            if (o1pcp == null)
               o1pcp = Integer.valueOf(0);
            Integer o2pc = origData.get(TYPE_2PC) != null ? origData.get(TYPE_2PC).get(count) : Integer.valueOf(0);
            if (o2pc == null)
               o2pc = Integer.valueOf(0);
            Integer ossup = origData.get(TYPE_SSUP) != null ? origData.get(TYPE_SSUP).get(count) : Integer.valueOf(0);
            if (ossup == null)
               ossup = Integer.valueOf(0);
            Integer oro = origData.get(TYPE_RO) != null ? origData.get(TYPE_RO).get(count) : Integer.valueOf(0);
            if (oro == null)
               oro = Integer.valueOf(0);
            Integer p1pc = patchData.get(TYPE_1PC) != null ? patchData.get(TYPE_1PC).get(count) : Integer.valueOf(0);
            if (p1pc == null)
               p1pc = Integer.valueOf(0);
            Integer p1pcp = patchData.get(TYPE_1PCP) != null ? patchData.get(TYPE_1PCP).get(count) : Integer.valueOf(0);
            if (p1pcp == null)
               p1pcp = Integer.valueOf(0);
            Integer p2pc = patchData.get(TYPE_2PC) != null ? patchData.get(TYPE_2PC).get(count) : Integer.valueOf(0);
            if (p2pc == null)
               p2pc = Integer.valueOf(0);
            Integer pssup = patchData.get(TYPE_SSUP) != null ? patchData.get(TYPE_SSUP).get(count) : Integer.valueOf(0);
            if (pssup == null)
               pssup = Integer.valueOf(0);
            Integer pro = patchData.get(TYPE_RO) != null ? patchData.get(TYPE_RO).get(count) : Integer.valueOf(0);
            if (pro == null)
               pro = Integer.valueOf(0);

            sb.append(count);
            sb.append(",");
            sb.append(o1pc);
            sb.append(",");
            sb.append(o1pcp);
            sb.append(",");
            sb.append(o2pc);
            sb.append(",");
            sb.append(ossup);
            sb.append(",");
            sb.append(oro);
            sb.append(",");
            sb.append(p1pc);
            sb.append(",");
            sb.append(p1pcp);
            sb.append(",");
            sb.append(p2pc);
            sb.append(",");
            sb.append(pssup);
            sb.append(",");
            sb.append(pro);
            sb.append(",");

            //
            int line = l.size() + 1;
            if (o1pc.intValue() != 0 && p1pc.intValue() != 0)
               sb.append("=((G").append(line).append("-B").append(line).append(")/B").append(line).append(")*100");
            sb.append(",");
            if (o1pcp.intValue() != 0 && p1pcp.intValue() != 0)
               sb.append("=((H").append(line).append("-C").append(line).append(")/C").append(line).append(")*100");
            sb.append(",");
            if (o2pc.intValue() != 0 && p2pc.intValue() != 0)
               sb.append("=((I").append(line).append("-D").append(line).append(")/D").append(line).append(")*100");
            sb.append(",");
            if (ossup.intValue() != 0 && pssup.intValue() != 0)
               sb.append("=((J").append(line).append("-E").append(line).append(")/E").append(line).append(")*100");
            sb.append(",");
            if (oro.intValue() != 0 && pro.intValue() != 0)
               sb.append("=((K").append(line).append("-F").append(line).append(")/F").append(line).append(")*100");
            
            l.add(sb.toString());
         }
      }

      l.add("");
      l.add("M: " + origCommit);
      l.add("P: " + patchCommit);
      
      writeFile(Paths.get("report", origCommit + "-" + patchCommit + ".csv"), l);
   }

   /**
    * Read a file
    * @param p The path to the file
    * @return The lines in the file
    */
   private static List<String> readFile(Path p) throws Exception
   {
      return Files.readAllLines(p);
   }
   
   /**
    * Write a file
    * @param p The path to the file
    * @param l The lines
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
    * Setup
    */
   private static void setup() throws Exception
   {
      File report = new File("report");
      report.mkdir();
   }

   /**
    * Main
    */
   public static void main(String[] args)
   {
      if (args.length < 2)
      {
         System.out.println("DiffRuns orig-commit patch-commit");
         System.exit(1);
      }

      String origCommit = args[0];
      String patchCommit = args[1];
      
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
         
         for (File f : txtFiles)
         {
            String commit = getCommit(f);

            if (origCommit.equals(commit) || patchCommit.equals(commit))
            {
               List<String> lines = readFile(f.toPath());

               if (f.getName().endsWith("1pc-standard.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);

                  getTypeMap(commit, profile).put(TYPE_1PC, data);
               }
               else if (f.getName().endsWith("1pc-prepared.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(commit, profile).put(TYPE_1PCP, data);
               }
               else if (f.getName().endsWith("2pc-standard.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(commit, profile).put(TYPE_2PC, data);
               }
               else if (f.getName().endsWith("ssu-prepared.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(commit, profile).put(TYPE_SSUP, data);
               }
               else if (f.getName().endsWith("-readonly.txt"))
               {
                  Map<Integer, Integer> data = getData(lines);
                  String profile = getProfile(f);
               
                  getTypeMap(commit, profile).put(TYPE_RO, data);
               }
               else if (f.getName().endsWith("-environment.txt"))
               {
                  for (String s : lines)
                  {
                     if (s.startsWith("Master:"))
                     {
                        String id = s.substring(s.indexOf(":") + 1);
                        if (!origCommit.equals(id))
                           System.out.println("Mismatch id: " + id);
                     }
                  }

               }
            }
         }

         writeData(origCommit, patchCommit);
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
