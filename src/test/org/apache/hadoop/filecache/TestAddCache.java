package org.apache.hadoop.filecache;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;

public class TestAddCache {

    private static final int LOCAL_CACHE_LIMIT = 5 * 1024; // 5K

    public static void main(String[] args) throws IOException, URISyntaxException {

//        System.out.println("Main class to test TestAddCache");
//
//        JobConf job = new JobConf();
//        job.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
//
//        // DistributedCache.addCacheArchive(new URI("C:/purna/indir/mytar.tar"),
//        // job);
//        // DistributedCache.addCacheArchive(new
//        // URI("C:/purna/indir/mytar1.tar"), job);
//        // DistributedCache.addFileToClassPath(new
//        // Path("C:/purna/indir/mytar1.tar"), job);
//        // DistributedCache.addFileToClassPath(new
//        // Path("C:/purna/indir/mytar.tar"), job);
//        
//        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile.txt"), job);
//        URI[] localArchives = DistributedCache.getCacheFiles(job);
//
//        System.out.println(localArchives);
        
        
        System.out.println("Time before program start : " + getDate());
//        manipulateSingleCache();
        manipulateNCache();
        System.out.println("Time after program start : " + getDate());
    }
    
    
    public static String getDate()
    {
        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String stringDate = sdf.format(date);
        return stringDate;
    }
    
    public static void manipulateSingleCache() throws URISyntaxException, IOException
    {
        System.out.println("Main class to test TestAddCache");

        JobConf job = new JobConf();
        job.setLong("local.cache.size", LOCAL_CACHE_LIMIT);

        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile1.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile2.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile3.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile4.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile5.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile6.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile7.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile8.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile9.txt"), job);
        DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile10.txt"), job);
        URI[] localArchives = DistributedCache.getCacheFiles(job);
        
        for (int i = 0; i < 11; i++) {
            String uri = DistributedCache.getCacheFilesByIndex(job, i);
            System.out.println(uri);
        }
   
    }
    
    public static void manipulateNCache() throws URISyntaxException, IOException
    {
        System.out.println("Main class to test TestAddCache");

        Cache job = new NSetCache(20);

        URI[] uris = new URI[11];
        uris[0] = new URI("C:/purna/indir/myFile.txt");
        uris[1] = new URI("C:/purna/indir/myFile1.txt");
        uris[2] = new URI("C:/purna/indir/myFile2.txt");
        uris[3] = new URI("C:/purna/indir/myFile3.txt");
        uris[4] = new URI("C:/purna/indir/myFile4.txt");
        uris[5] = new URI("C:/purna/indir/myFile5.txt");
        uris[6] = new URI("C:/purna/indir/myFile6.txt");
        uris[7] = new URI("C:/purna/indir/myFile7.txt");
        uris[8] = new URI("C:/purna/indir/myFile8.txt");
        uris[9] = new URI("C:/purna/indir/myFile9.txt");
        uris[10] = new URI("C:/purna/indir/myFile10.txt");
        
        DistributedCache.setCacheFilesNSet(uris, job);
        
        for (int i = 0; i < uris.length; i++) {
            Object obj = DistributedCache.getCacheFilesNSet(job, uris[i].hashCode());
            System.out.println(obj);
        }

        
    }
}
