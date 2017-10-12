/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

@SuppressWarnings("restriction")
/**
 * Borrows heavily from <a href=
 * "http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/jdk8/java/util/concurrent/ConcurrentLinkedQueue.java">ConcurrentLinkedQueue</a>,
 * but does not require instantiation of Nodes on each offer by placing
 * restrictions on the input type.
 * 
 * @author robert
 *
 * @param <P>
 */
public class ResourcePool<P extends ResourcePool.Poolable> implements Queue<P> {

    public static interface Poolable {
        Poolable next();

        void unsetPolled();

        boolean casItem(Poolable cmp, Poolable val);

        void lazySetNext(Poolable val);

        boolean casNext(Poolable cmp, Poolable val);
    }

    public static class PoolableResource implements Poolable {
        volatile PoolableResource next;
        volatile boolean polled = false;

        @Override
        public Poolable next() {
            return this.next;
        }

        @Override
        public void unsetPolled() {
            this.polled = false;
        }

        @Override
        public boolean casItem(Poolable cmp, Poolable val) {
            return Unsafe.U.compareAndSwapObject(this, POLLED, cmp == this,
                    val == null);
        }

        @Override
        public void lazySetNext(Poolable val) {
            Unsafe.U.putOrderedObject(this, NEXT, val);
        }

        @Override
        public boolean casNext(Poolable cmp, Poolable val) {
            return Unsafe.U.compareAndSwapObject(this, NEXT, cmp, val);
        }

        private static final long POLLED;
        static final long NEXT;
        static {
            try {
                POLLED = Unsafe.U
                        .objectFieldOffset(ResourcePool.PoolableResource.class
                                .getDeclaredField("polled"));
                NEXT = Unsafe.U
                        .objectFieldOffset(ResourcePool.PoolableResource.class
                                .getDeclaredField("next"));
            } catch (ReflectiveOperationException e) {
                throw new Error(e);
            }
        }

    }

    transient volatile P head;
    private transient volatile P tail;

    public ResourcePool(P initial) {
        this.head = this.tail = initial;
    }

    @Override
    public boolean offer(P o) {
        o.unsetPolled();
        for (Poolable t = this.tail, p = t;;) {
            Poolable q = p.next();
            if (q == null) {
                if (p.casNext(null, o)) {
                    if (p != t)
                        casTail(t, o);
                    return true;
                }
            } else if (p == q)
                p = (t != (t = this.tail)) ? t : this.head;
            else
                p = (p != t && t != (t = this.tail)) ? t : q;
        }
    }

    @Override
    public P poll() {
        restartFromHead: for (;;) {
            for (Poolable h = this.head, p = h, q;; p = q) {
                if (h != null && p.casItem(h, null)) {
                    if (p != h)
                        updateHead(h, ((q = p.next()) != null) ? q : p);
                    return (P) p;
                } else if ((q = p.next()) == null) {
                    updateHead(h, p);
                    return null;
                } else if (p == q)
                    continue restartFromHead;
            }
        }
    }

    final void updateHead(Poolable h, Poolable p) {
        if (h != p && casHead(h, p))
            h.lazySetNext(h);
    }

    private boolean casHead(Poolable cmp, Poolable val) {
        return Unsafe.U.compareAndSwapObject(this, HEAD, cmp, val);
    }

    private boolean casTail(Poolable cmp, Poolable val) {
        return Unsafe.U.compareAndSwapObject(this, TAIL, cmp, val);
    }

    private static final long HEAD;
    private static final long TAIL;
    static {
        try {
            HEAD = Unsafe.U.objectFieldOffset(
                    ResourcePool.class.getDeclaredField("head"));
            TAIL = Unsafe.U.objectFieldOffset(
                    ResourcePool.class.getDeclaredField("tail"));
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<P> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends P> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(P e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public P remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public P element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public P peek() {
        throw new UnsupportedOperationException();
    }

}
