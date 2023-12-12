package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class MapsLocationMappingStorageType {

    public enum Location
    {
        HOSTLOCAL,
        RACKLOCAL
    }

    // Instead of StorageType we are using Integer (StorageType Order value for sorting)
    private final Map<String, Map<Integer, LinkedList<TaskAttemptId>>> mapsHostMappingStorageType;
    private final Map<String, Map<Integer, LinkedList<TaskAttemptId>>> mapsRackMappingStorageType;
    private final Map<String, Integer> hostLastTypeInserted;
    private int lastStorageTypeAccessed;

    public MapsLocationMappingStorageType() {
        this.mapsHostMappingStorageType = new HashMap<>();
        this.mapsRackMappingStorageType = new HashMap<>();
        this.hostLastTypeInserted = new HashMap<>();
        this.lastStorageTypeAccessed = StorageType.DEFAULT.getOrderValue();
    }

    public LinkedList<TaskAttemptId> getLocationStorageTypeTaskAttemptIDs(String host, Location location)
    {
        Map<String, Map<Integer, LinkedList<TaskAttemptId>>> mapsLocationStorageType;

        if (location == Location.HOSTLOCAL)
        {
            mapsLocationStorageType = this.mapsHostMappingStorageType;
        }
        else
        {
            mapsLocationStorageType = this.mapsRackMappingStorageType;
        }

        Map<Integer, LinkedList<TaskAttemptId>> mapTypeToAttempts = mapsLocationStorageType.get(host);
        if (mapTypeToAttempts == null && location == Location.HOSTLOCAL && isIP(host)) {
           // Special case: try to convert the IP to host and try again
           host = resolveHost(host);
           if (host != null)
              mapTypeToAttempts = mapsLocationStorageType.get(host);
        }
        
        if (mapTypeToAttempts != null)
        {
            // Each time return the list with fastest tier, if all of them are empty return null
            for (Map.Entry<Integer, LinkedList<TaskAttemptId>> storageTypeTaskAttempt : mapTypeToAttempts.entrySet() )
            {
                if(!storageTypeTaskAttempt.getValue().isEmpty())
                {
                   this.lastStorageTypeAccessed = storageTypeTaskAttempt.getKey();
                   return storageTypeTaskAttempt.getValue();
                }
            }
        }
        return null;
    }

    public void setLocationTaskAttemptID(String host, StorageType type, TaskAttemptId taskAttemptid, Location location)
    {
        Map<String, Map<Integer, LinkedList<TaskAttemptId>>> mapsLocationStorageType;

        if (location == Location.HOSTLOCAL)
        {
            hostLastTypeInserted.put(host, type.getOrderValue());
            mapsLocationStorageType = this.mapsHostMappingStorageType;
        }
        else
        {
            mapsLocationStorageType = this.mapsRackMappingStorageType;
        }

        Map<Integer, LinkedList<TaskAttemptId>> storageTypeTaskAttempt = mapsLocationStorageType.get(host);
        LinkedList<TaskAttemptId> taskAttemptIdList;
        if (storageTypeTaskAttempt == null)
        {
            storageTypeTaskAttempt = new TreeMap<>();
            taskAttemptIdList = new LinkedList<>();

            taskAttemptIdList.add(taskAttemptid);
            storageTypeTaskAttempt.put(type.getOrderValue(), taskAttemptIdList);
            mapsLocationStorageType.put(host, storageTypeTaskAttempt);
        }
        else
        {
            taskAttemptIdList = storageTypeTaskAttempt.get(type.getOrderValue());
            if (taskAttemptIdList == null)
            {
                taskAttemptIdList = new LinkedList<>();
                taskAttemptIdList.add(taskAttemptid);
                storageTypeTaskAttempt.put(type.getOrderValue(), taskAttemptIdList);
            }
            else
            {
                taskAttemptIdList.add(taskAttemptid);
            }
        }
    }

    public LinkedList<TaskAttemptId> lastTaskAttemptIDList(String host)
    {
       if (mapsHostMappingStorageType.containsKey(host))
          if (hostLastTypeInserted.containsKey(host))
             return mapsHostMappingStorageType.get(host).get(hostLastTypeInserted.get(host));
          else if (!mapsHostMappingStorageType.get(host).isEmpty())
             return mapsHostMappingStorageType.get(host).values().iterator().next();
          else
             return null;
       else return null;
    }

    public StorageType getLastStorageTypeAccessed()
    {
       return StorageType.valuesOrdered()[this.lastStorageTypeAccessed];
    }
    
    private static final Pattern ipPattern = // Pattern for matching ip
       Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
     
    private static boolean isIP(String src) {
       return ipPattern.matcher(src).matches();
    }

    private static String resolveHost(String src) {
       String result = null;
       try {
         InetAddress addr = InetAddress.getByName(src);
         result = addr.getHostName();
       } catch (UnknownHostException e) {
          // Ignore
       }
       return result;
    }
}
