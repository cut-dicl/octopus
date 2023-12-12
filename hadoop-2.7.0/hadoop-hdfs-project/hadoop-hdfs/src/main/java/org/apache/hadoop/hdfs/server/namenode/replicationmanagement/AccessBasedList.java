package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class keeps a sorted list of files based on recency.
 * 
 * @author elena.kakoulli
 */
public class AccessBasedList {

   private DoublyLinkedList fileList;
   private Map<Long, Node> fileMap;

   public AccessBasedList() {
      fileList = new DoublyLinkedList();
      fileMap = new HashMap<Long, Node>();
   }

   public void addFile(INodeFile file) {
      if (file != null && !fileMap.containsKey(file.getId())) {
         Node fileNode = new Node(file);
         fileNode = fileList.addToHead(fileNode);
         fileMap.put(file.getId(), fileNode);
      }
   }

   public void addFileLast(INodeFile file) {
      if (file != null && !fileMap.containsKey(file.getId())) {
         Node fileNode = new Node(file);
         fileNode = fileList.addToTail(fileNode);
         fileMap.put(file.getId(), fileNode);
      }
   }

   public void addFileNode(Node node) {
      if (!fileMap.containsKey(node.file.getId())) {
         node = fileList.addToHead(node);
         fileMap.put(node.file.getId(), node);
      }
   }

   public void addFileNodeLast(Node node) {
      if (!fileMap.containsKey(node.file.getId())) {
         node = fileList.addToTail(node);
         fileMap.put(node.file.getId(), node);
      }
   }

   public void accessFile(INodeFile file) {
      if (file != null && fileMap.containsKey(file.getId())) {
         Node fileNode = fileMap.get(file.getId());
         fileList.moveFileToHead(fileNode);
      }
   }

   public Node deleteFile(INodeFile file) {
      if (file != null && fileMap.containsKey(file.getId())) {
         fileList.removeFile(fileMap.get(file.getId()));
         Node n = fileMap.remove(file.getId());
         n.clear();
         return n;
      }
      
      return null;
   }
   
   public boolean containsFile(INodeFile file) {
      return file != null && fileMap.containsKey(file.getId());
   }

   public Node removeLRUFileNode() {
      Node tail = fileList.removeTail();
      if (tail != null) {
         Node n = fileMap.remove(tail.file.getId());
         n.clear();
         return n;
      } else
         return null;
   }
   
   public Node removeMRUFileNode() {
      Node head = fileList.removeHead();
      if (head != null) {
         Node n = fileMap.remove(head.file.getId());
         n.clear();
         return n;
      } else
         return null;
   }

   public void printListState() {
      fileList.printList();
      System.out.println();
   }

   public INodeFile getLRUFile(StorageType type) {
      return fileList.getLRUFileFromStorage(type);
   }

   public Iterator<INodeFile> getLRUFileIterator() {
      return fileList.getLRUFileIterator();
   }

   public ArrayList<INodeFile> getLRUFiles(StorageType type, int files) {
      return fileList.getCountLRUFileFromStorage(type, files);
   }

   public INodeFile getMRUFile(StorageType type) {
      return fileList.getMRUFileFromStorage(type);
   }

   public Iterator<INodeFile> getMRUFileIterator() {
      return fileList.getMRUFileIterator();
   }

   public int size() {
      return fileList.size();
   }

   @Override
   public String toString() {
      return fileList.toString();
   }
   
   /**
    * 
    */
   private class DoublyLinkedList {

      private int currSize;
      private Node head;
      private Node tail;

      public DoublyLinkedList() {
         currSize = 0;
         this.head = null;
         this.tail = null;
      }

      public INodeFile getMRUFileFromStorage(StorageType type) {
         Node tmp = head;
         while (tmp != null) {
            // and if it is in selected storageType
            INodeFile file = tmp.getFile();
            if (ReplicationVector.GetReplication(type,
                  file.getReplicationVector()) > 0
                  && !file.isUnderConstruction()) {
               return file;
            }
            tmp = tmp.getNext();
         }
         return null;
      }

      public INodeFile getLRUFileFromStorage(StorageType type) {
         Node tmp = tail;
         while (tmp != null) {
            // and if it is in selected storageType
            INodeFile file = tmp.getFile();
            if (file.getReplicationFactor(type) > 0
                  && !file.isUnderConstruction()) {
               return file;
            }
            tmp = tmp.getPrev();
         }
         return null;
      }

      public Iterator<INodeFile> getLRUFileIterator() {
         return new DoublyLinkedListLRUIterator(tail);
      }
      
      public Iterator<INodeFile> getMRUFileIterator() {
         return new DoublyLinkedListMRUIterator(head);
      }

      public ArrayList<INodeFile> getCountLRUFileFromStorage(StorageType type,
            int count) {
         ArrayList<INodeFile> LRUfiles = new ArrayList<INodeFile>(
               (currSize < count) ? currSize : count);
         Node tmp = tail;
         int c = 0;
         while (tmp != null && c < count) {
            // and if it is in selected storageType
            INodeFile file = tmp.getFile();
            if (file.getReplicationFactor(type) > 0
                  && !file.isUnderConstruction()) {
               LRUfiles.add(file);
            }
            tmp = tmp.getPrev();
            c++;
         }
         return LRUfiles;
      }

      public void removeFile(Node fileNode) {
         if (head == null) {
            return;
         }

         Node prev = fileNode.getPrev();
         Node next = fileNode.getNext();

         if (prev != null) {
            prev.setNext(next);
         } else { // is the head
            head = next;
         }
         if (next != null) {
            next.setPrev(prev);
         } else { // is the tail
            tail = prev;
         }
         
         --currSize;
      }
      
      public Node removeHead() {
         if (head == null)
            return null;
         
         Node toReturn = head;
         removeFile(head);
         return toReturn;
      }

      public Node removeTail() {
         if (tail == null)
            return null;
         
         Node toReturn = tail;
         removeFile(tail);
         return toReturn;
      }

      public void printList() {
         if (head == null) {
            return;
         }
         Node tmp = head;
         while (tmp != null) {
            System.out.print(tmp);
            System.out.print(" ");
            tmp = tmp.getNext();
         }
         System.out.println();
      }

      @Override
      public String toString() {
         if (head == null) {
            return "[]";
         }
         
         StringBuilder sb = new StringBuilder("[T--");
         Node curr = tail;
         while (curr != null) {
            sb.append(curr.toString());
            sb.append("--");
            curr = curr.getPrev();
         }
         sb.append("H]");
         
         return sb.toString();
      }
      
      public Node addToHead(Node fileNode) {
         if (head == null) {
            head = fileNode;
            tail = fileNode;
            currSize = 1;
            return fileNode;
         }
         currSize++;
         fileNode.setNext(head);
         head.setPrev(fileNode);
         head = fileNode;
         return fileNode;
      }

      public Node addToTail(Node fileNode) {
         if (head == null) {
            head = fileNode;
            tail = fileNode;
            currSize = 1;
            return fileNode;
         }
         currSize++;
         fileNode.setPrev(tail);
         tail.setNext(fileNode);
         tail = fileNode;
         return fileNode;
      }

      public void moveFileToHead(Node fileNode) {
         if (fileNode == null || fileNode == head) {
            return;
         }

         if (fileNode == tail) {
            tail = tail.getPrev();
            tail.setNext(null);
         }

         Node prev = fileNode.getPrev();
         Node next = fileNode.getNext();
         prev.setNext(next);

         if (next != null) {
            next.setPrev(prev);
         }

         fileNode.setPrev(null);
         fileNode.setNext(head);
         head.setPrev(fileNode);
         head = fileNode;
      }
      
      public int size() {
         return currSize;
      }
   }

   /**
    * An iterator to iterate the list from LRU to MRU
    * 
    * @author herodotos.herodotou
    */
   private class DoublyLinkedListLRUIterator implements Iterator<INodeFile> {

      private Node currNode;
      
      DoublyLinkedListLRUIterator(Node tail) {
         this.currNode = tail;
      }
      
      @Override
      public boolean hasNext() {
         return (currNode != null);
      }

      @Override
      public INodeFile next() {
         if (!hasNext()) throw new NoSuchElementException();
         
         INodeFile file = currNode.file;
         currNode = currNode.getPrev();
         return file;
      }
   }
   
   /**
    * An iterator to iterate the list from MRU to LRU
    * 
    * @author herodotos.herodotou
    */
   private class DoublyLinkedListMRUIterator implements Iterator<INodeFile> {

      private Node currNode;
      
      DoublyLinkedListMRUIterator(Node head) {
         this.currNode = head;
      }
      
      @Override
      public boolean hasNext() {
         return (currNode != null);
      }

      @Override
      public INodeFile next() {
         if (!hasNext()) throw new NoSuchElementException();
         
         INodeFile file = currNode.file;
         currNode = currNode.getNext();
         return file;
      }
   }

   /**
    * An internal linked-list node
    */
   class Node {

      private INodeFile file;
      private Node prev;
      private Node next;

      public Node(INodeFile f) {
         this.file = f;
         this.prev = null;
         this.next = null;
      }

      public INodeFile getFile() {
         return file;
      }

      public void clear() {
         prev = null;
         next = null;
      }
      
      public Node getPrev() {
         return prev;
      }

      public void setPrev(Node prev) {
         this.prev = prev;
      }

      public Node getNext() {
         return next;
      }

      public void setNext(Node next) {
         this.next = next;
      }

      @Override
      public String toString() {
         return file.toString();
      }

   }
}
