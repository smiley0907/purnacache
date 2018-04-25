/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.filecache;


/**
 * N-way set associative cache implementation Class.
 * 
 * @author panchal
 */
public class NSetCacheExample {
    public static void main(String[] args) {
        System.out.println("NSetCacheExample");
        NSetCache cache = new NSetCache(5);
        cache.put("1", "abc");
        cache.put("2", "abc2");
        cache.put("3", "abc3");
        cache.put("4", "abc4");
        
        Object obj = cache.get("2");
        System.out.println(obj);
    }
}
