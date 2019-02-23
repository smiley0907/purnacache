package org.apache.hadoop.filecache;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;

import com.sun.org.apache.xerces.internal.impl.dv.dtd.NMTOKENDatatypeValidator;

public class TestAddNCache {

    private static final int LOCAL_CACHE_LIMIT = 5 * 1024; // 5K

    public static void main(String[] args) throws IOException, URISyntaxException {

        // System.out.println("Main class to test TestAddCache");
        //
        // JobConf job = new JobConf();
        // job.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
        //
        // // DistributedCache.addCacheArchive(new
        // URI("C:/purna/indir/mytar.tar"),
        // // job);
        // // DistributedCache.addCacheArchive(new
        // // URI("C:/purna/indir/mytar1.tar"), job);
        // // DistributedCache.addFileToClassPath(new
        // // Path("C:/purna/indir/mytar1.tar"), job);
        // // DistributedCache.addFileToClassPath(new
        // // Path("C:/purna/indir/mytar.tar"), job);
        //
        // DistributedCache.addCacheFile(new URI("C:/purna/indir/myFile.txt"),
        // job);
        // URI[] localArchives = DistributedCache.getCacheFiles(job);
        //
        // System.out.println(localArchives);

        // manipulateNCache(2, 2);
        // manipulateNCache(2, 3);
        // manipulateNCache(2, 4);
        // manipulateNCache(2, 5);
        // manipulateNCache(2, 6);
        // manipulateNCache(2, 7);
        // manipulateNCache(2, 8);
        // manipulateNCache(2, 9);
        // manipulateNCache(2, 10);

        // manipulateNCache(4, 2);
        // manipulateNCache(4, 3);
        // manipulateNCache(4, 4);
        // manipulateNCache(4, 5);
        // manipulateNCache(4, 6);
        // manipulateNCache(4, 7);
        // manipulateNCache(4, 8);
        // manipulateNCache(4, 9);
        // manipulateNCache(4, 10);

        // manipulateNCache(8, 2);
        // manipulateNCache(8, 3);
        // manipulateNCache(8, 4);
        // manipulateNCache(8, 5);
        // manipulateNCache(8, 6);
        // manipulateNCache(8, 7);
        // manipulateNCache(8, 8);
        // manipulateNCache(8, 9);

        // manipulateNCache(8, 1, 8);
        // manipulateNCache(4, 2, 8);
        // manipulateNCache(2, 4, 8);
        // manipulateNCache(1, 8, 8);

       //  manipulateNCache(16, 1, 8);
         manipulateNCache(8, 2, 8);
       //  manipulateNCache(4, 4, 8);
        // manipulateNCache(2, 8, 8);

         //manipulateNCache(32, 1, 8);
       //  manipulateNCache(16, 2, 8);
        // manipulateNCache(8, 4, 8);
        // // manipulateNCache(4, 8, 8);

        //manipulateNCache(64, 1, 8);
        //manipulateNCache(32, 2, 8);
         //manipulateNCache(16, 4, 8);
        // manipulateNCache(8, 8, 8);
         
        // manipulateNCache(128, 1, 8);
        // manipulateNCache(64, 2, 8);
         // manipulateNCache(32, 4, 8);
         // manipulateNCache(16, 8, 8);
        
     // manipulateNCache(256, 1, 8);
       // manipulateNCache(128, 2, 8);
        //manipulateNCache(64, 4, 8);
       //   manipulateNCache(32, 8, 8);
    }

    public static String getDate() {
        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String stringDate = sdf.format(date);
        return stringDate;
    }

    public static void manipulateNCache(int n, int numOfSets, int numFiles) throws URISyntaxException, IOException {
        System.out.println("----------------------------------------");

        System.out.println("N:" + n + "..Number of Sets :" + numOfSets + "..Cache Size : " + n * numOfSets);

        System.out.println("Time before program start : " + getDate());

        System.out.println("Main class to test TestAddCache");

        Cache job = new NSetCache(numOfSets, n);

        List<URI> uris = new ArrayList<URI>();

        for (int i = 1; i <= numFiles; i++) {
            String fileName = "/home/hduser/myFile" + i + ".txt";
            uris.add(new URI(fileName));
        }

        URI[] finalURIs = uris.toArray(new URI[uris.size()]);
        DistributedCache.setCacheFilesNSet(finalURIs, job);

        for (int i = 0; i < finalURIs.length; i++) {
            Object obj = DistributedCache.getCacheFilesNSet(job, finalURIs[i].hashCode());
            System.out.println(obj);
        }

        System.out.println("Time after program end : " + getDate());
        System.out.println("----------------------------------------");
    }

}
