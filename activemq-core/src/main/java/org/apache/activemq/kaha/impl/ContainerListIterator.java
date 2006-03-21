/**
 * 
 */
package org.apache.activemq.kaha.impl;

import java.util.ListIterator;
/**
 * @author rajdavies
 * 
 */
public class ContainerListIterator implements ListIterator{
    private ListContainerImpl container;
    private ListIterator iterator;
    private LocatableItem current;

    protected ContainerListIterator(ListContainerImpl container,ListIterator iterator){
        this.container=container;
        this.iterator=iterator;
        this.current = container.internalGet(0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasNext()
     */
    public boolean hasNext(){
        return iterator.hasNext();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#next()
     */
    public Object next(){
        Object result=null;
        current=(LocatableItem) iterator.next();
        if(current!=null){
            result=container.getValue(current);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    public boolean hasPrevious(){
        return iterator.hasPrevious();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    public Object previous(){
        Object result=null;
        current=(LocatableItem) iterator.previous();
        if(current!=null){
            result=container.getValue(current);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#nextIndex()
     */
    public int nextIndex(){
        return iterator.nextIndex();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previousIndex()
     */
    public int previousIndex(){
        return iterator.previousIndex();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#remove()
     */
    public void remove(){
        iterator.remove();
        if(current!=null){
            container.remove(current);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(E)
     */
    public void set(Object o){
        LocatableItem item=container.internalSet(previousIndex()+1,o);
        iterator.set(item);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(E)
     */
    public void add(Object o){
        LocatableItem item=container.internalAdd(previousIndex()+1,o);
        iterator.set(item);
    }
}
