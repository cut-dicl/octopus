package org.apache.hadoop.fs;

public class TestReplicationVector {

   public static void main(String[] args) {
      ReplicationVector rv1 = new ReplicationVector(0l);
      rv1.setReplication(StorageType.DISK, (short) 1);
      rv1.setReplication(StorageType.SSD, (short) 2);
      rv1.setReplication(StorageType.RAM_DISK, (short) 3);
      rv1.setReplication(StorageType.ARCHIVE, (short) 4);
      rv1.setReplication(StorageType.SSD, (short) 5);
      rv1.setReplication(StorageType.RAM_DISK, (short) 6);
      rv1.setReplication(null, (short) 7);

      System.out.println(rv1.getReplication(StorageType.DISK));
      System.out.println(rv1.getReplication(StorageType.SSD));
      System.out.println(rv1.getReplication(StorageType.RAM_DISK));
      System.out.println(rv1.getReplication(StorageType.ARCHIVE));
      System.out.println(rv1.getReplication(null));
      System.out.println(rv1.getReplicationSum());
      System.out.println(rv1.getTotalReplication());
      System.out.println(rv1);

      ReplicationVector rv2 = new ReplicationVector(rv1);
      System.out.println(rv1.equals(rv2));
      rv2.incrReplication(StorageType.DISK, (short) 2);
      rv2.decrReplication(StorageType.SSD, (short) 2);
      rv2.decrReplication(null, (short) 1);

      System.out.println(rv2.getReplication(StorageType.DISK));
      System.out.println(rv2.getReplication(StorageType.SSD));
      System.out.println(rv2.getReplication(StorageType.RAM_DISK));
      System.out.println(rv2.getReplication(StorageType.ARCHIVE));
      System.out.println(rv2.getReplication(null));
      System.out.println(rv2.getReplicationSum());
      System.out.println(rv2.getTotalReplication());
      System.out.println(rv2);

      ReplicationVector rv3 = new ReplicationVector(
          ReplicationVector.ParseReplicationVector(ReplicationVector
              .StringifyReplVector(rv2.getVectorEncoding())));
      System.out.println(rv3.equals(rv2));

      rv3.setVectorEncoding(ReplicationVector
          .ParseReplicationVector("DISK=2, SSD=1"));
      System.out.println(rv3.toString());
      rv3.setVectorEncoding(ReplicationVector
          .ParseReplicationVector("DISK=2, UNSPEC=1"));
      System.out.println(rv3.toString());
      rv3.setVectorEncoding(ReplicationVector.ParseReplicationVector("3"));
      System.out.println(rv3.toString());

      rv3.setVectorEncoding(ReplicationVector.ParseReplicationVector("[M=1,D=3]"));
      System.out.println(rv3.toString());
      rv3.setVectorEncoding(ReplicationVector.ParseReplicationVector("m=1,s=1,d=2,u=1"));
      System.out.println(rv3.toString());

      for (StorageType type : StorageType.valuesOrdered()) {
         System.out.println(type + " isDriveBased: " + type.isDriveBased());       
         System.out.println(type + " isMovable: " + type.isMovable());       
         System.out.println(type + " isRemote: " + type.isRemote());       
         System.out.println(type + " isTransient: " + type.isTransient());       
         System.out.println(type + " isVolatile: " + type.isVolatile());       
      }
   }

}
