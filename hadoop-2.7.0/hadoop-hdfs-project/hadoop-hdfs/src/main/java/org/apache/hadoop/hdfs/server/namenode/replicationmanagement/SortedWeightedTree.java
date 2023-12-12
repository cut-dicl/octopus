package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class keeps a sorted tree based on Node's weight.
 * 
 * @author elena.kakoulli
 */
public class SortedWeightedTree<T extends WeightedNode> {

   private TreeSet<WeightedNode> dirWeighted;
   private Map<Long, WeightedNode> filesMap;

   public SortedWeightedTree() {
      this.dirWeighted = new TreeSet<WeightedNode>();
      this.filesMap = new HashMap<Long, WeightedNode>();
   }
   
   public SortedWeightedTree(Comparator<WeightedNode> comparator) {
      this.dirWeighted = new TreeSet<WeightedNode>(comparator);
      this.filesMap = new HashMap<Long, WeightedNode>();
   }

   public void addNode(WeightedNode f) {
      if (!existWeightedNode(f.file.getId())) {
         filesMap.put(f.file.getId(), f);
         dirWeighted.add(f);
      }
   }

   public Iterator<WeightedNode> descIter() {
      return new WeightedNodeIterator(dirWeighted.descendingIterator(), filesMap);
   }

   public Iterator<WeightedNode> ascIter() {
      return new WeightedNodeIterator(dirWeighted.iterator(), filesMap);
   }

   public Iterator<INodeFile> descFileIter() {
      return new WeightedFileIterator(dirWeighted.descendingIterator());
   }

   public Iterator<INodeFile> ascFileIter() {
      return new WeightedFileIterator(dirWeighted.iterator());
   }

   public boolean existWeightedNode(long id) {
      return filesMap.containsKey(id);
   }

   public WeightedNode getNode(long id) {
      return filesMap.get(id);
   }

   public void updateNode(INodeFile file) {
      if (file != null && filesMap.containsKey(file.getId())) {
         WeightedNode changedNode = filesMap.get(file.getId());
         dirWeighted.remove(changedNode);
         changedNode.updateWeight();
         dirWeighted.add(changedNode);
      }
   }

   public WeightedNode deleteNode(INodeFile file) {
      if (file != null && filesMap.containsKey(file.getId())) {
         dirWeighted.remove(filesMap.get(file.getId()));
         return filesMap.remove(file.getId());
      }
      
      return null;
   }

   public int size() {
      return dirWeighted.size();
   }

   @Override
   public String toString() {
      return dirWeighted.toString();
   }
   
   /**
    * File iterator
    */
   private class WeightedFileIterator implements Iterator<INodeFile> {

      Iterator<WeightedNode> iter;
      
      public WeightedFileIterator(Iterator<WeightedNode> iter) {
         this.iter = iter;
      }
      
      @Override
      public boolean hasNext() {
         return iter.hasNext();
      }

      @Override
      public INodeFile next() {
         return iter.next().file;
      }
   }
   
   /**
    * Weighted node iterator
    */
   private class WeightedNodeIterator implements Iterator<WeightedNode> {

      Iterator<WeightedNode> iter;
      Map<Long, WeightedNode> filesMap;
      WeightedNode curr;
      
      public WeightedNodeIterator(Iterator<WeightedNode> iter, 
            Map<Long, WeightedNode> filesMap) {
         this.iter = iter;
         this.filesMap = filesMap;
         this.curr = null;
      }
      
      @Override
      public boolean hasNext() {
         return iter.hasNext();
      }

      @Override
      public WeightedNode next() {
         curr = iter.next();
         return curr;
      }
      
      @Override
      public void remove() {
         if (curr == null)
            throw new IllegalStateException();
         
         iter.remove();
         filesMap.remove(curr.file.getId());
         curr = null;
      }
   }
}

/**
 * Base class for a weighted node
 */
abstract class WeightedNode implements Comparable<WeightedNode> {
   protected INodeFile file;

   public WeightedNode(INodeFile f) {
      this.file = f;
   }

   @Override
   public int compareTo(WeightedNode o1) {
      if (file.equals(o1.file))
         return 0;
      
      int comp = Double.compare(this.getWeight(), o1.getWeight());
      if (comp == 0)
         return (file.getId() < o1.file.getId()) ? -1 : 1;
      else
         return comp;
   }

   public INodeFile getFile() {
      return file;
   }

   abstract double getWeight();

   abstract void updateWeight();
}
