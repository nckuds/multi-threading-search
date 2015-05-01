package net.spright.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class MutiThreadSearch {

    @SuppressWarnings("empty-statement")
    public static void main(String[] args) throws InterruptedException, IOException {
        final int searchThreadCount = 10;
        final int pageCount = 500;
        Map<String, Integer> resultMap = new HashMap<>();
        final String keyword = args[0];
        final FileSystem fs = getFileSystem();
        
        ExecutorService service = Executors.newFixedThreadPool(
            searchThreadCount 
        );
        BlockingQueue<Path> pageQueue = new ArrayBlockingQueue(pageCount);
        long startTime = System.currentTimeMillis();
        
        try {
            initPageQueue(fs, pageQueue);
        } catch (IOException ex) {
           System.out.println(ex);
        }
        
        for (int i = 0; i != searchThreadCount; ++i) {
            service.execute(new Seacher(fs, keyword, pageQueue, resultMap));
        }
    
        service.shutdown();
        while(!service.awaitTermination(1, TimeUnit.SECONDS));
        
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        outputSearchResult(resultMap, totalTime);
        System.exit(0);
    }
    private static void waitSomething(int time) throws InterruptedException {
        TimeUnit.SECONDS.sleep(time);
    }
   
    private static class Seacher implements Runnable {

        private final BlockingQueue<Path> pageQueue;
        private final Map<String, Integer> resultMap;
        private final String keyword;
        private final FileSystem fs;
        
        public  Seacher(
            FileSystem fs,
            String keyword,
            BlockingQueue<Path> pageQueue, 
            Map<String, Integer> resultMap
        ) {
            this.fs = fs;
            this.keyword = keyword;
            this.pageQueue = pageQueue;
            this.resultMap = resultMap;
        }
        @Override
        public void run() {
            int score = 0;
            Path path;
            try {
                while (!pageQueue.isEmpty()) {
                    path = pageQueue.take();
                    score = 0;
                    HtmlPage page = getHtmlPage(fs, path);
                    if (page == null || page.link == null 
                            || page.title == null || page.content == null) {
                        continue;
                    }
                    // content, calculate corelation
                    //System.out.println("Search link: " + page.link);
                    //System.out.println(page.title);
                    if (page.title.toLowerCase().contains(keyword.toLowerCase())) {
                        score += 5;
                    }
                    String[] words = page.content.trim().split("\\s++");
                    for (int i = 0; i < words.length; ++ i) {
                        //System.out.println(words[i]);
                        if (words[i].contains(keyword.toLowerCase())) {
                            score += 1;
                        }
                    }
                           
                    if (score > 0) {
                        resultMap.put(page.link, score);
                    }
                   
                }       
                //System.out.println("Finish search");
            } catch (InterruptedException ex) {
                System.out.println(ex);
                Thread.currentThread().interrupt();
            } catch (IOException ex) {
                Logger.getLogger(MutiThreadSearch.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
    }
    
    private static void initPageQueue(FileSystem fs, 
            BlockingQueue<Path> pageQueue) throws IOException {
        FileStatus[] status = fs.listStatus(new Path("hdfs://course/user/course"));
        for (FileStatus s : status) {
            try {
                //System.out.println(s.toString());
                pageQueue.put(s.getPath());
            } catch (InterruptedException ex) {
                Logger.getLogger(MutiThreadSearch.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private static FileSystem getFileSystem() throws IOException {
       Configuration configuration = new Configuration();
       FileSystem fs = FileSystem.get(configuration);
       return fs;
    }
       
    private static HtmlPage getHtmlPage(FileSystem fs, Path path)
            throws IOException {     
        
        if (!fs.getFileStatus(path).isFile()){
            return null;
        }
                
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                fs.open(path)
                , Charset.forName("UTF-8")))) {
             
            String link = reader.readLine();
            String title = reader.readLine();
            String line;
        
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
               
            return new HtmlPage(link, title, content.toString());
        }
        
    }
    
    private static void outputSearchResult(Map<String, Integer> map, 
            long totalTime) {
        
        List<Map.Entry<String, Integer>> list =
            new LinkedList<>( map.entrySet() );
        System.out.println("output:");
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare( Map.Entry<String, Integer> o1, 
                    Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue() );
            }
        });
       Iterator it = list.iterator();
       System.out.println("Links found: ");
       while(it.hasNext()) {
          Map.Entry entry;  
          entry = (Map.Entry) it.next();
          
          System.out.println(entry.getKey().toString() + " / " 
                  + entry.getValue().toString());
        }               
        System.out.println("Search Time: " + totalTime);
        System.out.println("Match link count: " + list.size());
    }      
  
    private static class HtmlPage {
        private final String link;
        private final String title;
        private final String content;
        public HtmlPage(
            String link,
            String title,
            String content
        ) {
            this.link = link;
            this.title = title;
            this.content = content;
        }
        public String getTitle() {
            return title;
        }
        public String getLink() {
            return link;
        }
        public String getContent() {
            return content;
        }
    }
}
