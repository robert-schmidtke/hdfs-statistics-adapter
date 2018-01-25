/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 * Originally Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * 
 * http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/jdk8/java/util/concurrent/ConcurrentSkipListMap.java?revision=1.12
 */
package de.zib.sfs.instrument.util;

import java.lang.reflect.Constructor;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

// greatly reduced and int-long-specialized version
@SuppressWarnings("restriction")
public class ConcurrentIntLongSkipListMap {

    public static interface LongBiFunction {
        public long apply(long a, long b);
    }

    static final class Node {
        final int key;
        long val;
        Node next;

        Node(int key, long value, Node next) {
            this.key = key;
            this.val = value;
            this.next = next;
        }
    }

    static final class Index {
        final Node node;
        final Index down;
        Index right;

        Index(Node node, Index down, Index right) {
            this.node = node;
            this.down = down;
            this.right = right;
        }
    }

    public final class ValueIterator {
        Node lastReturned;
        Node next;
        long nextValue;

        ValueIterator() {
            advance(baseHead());
        }

        public final boolean hasNext() {
            return this.next != null;
        }

        final void advance(Node b) {
            Node n = null;
            long v = Long.MIN_VALUE;
            if ((this.lastReturned = b) != null) {
                while ((n = b.next) != null && (v = n.val) == Long.MIN_VALUE)
                    b = n;
            }
            this.nextValue = v;
            this.next = n;
        }

        public long next() {
            long v;
            if ((v = this.nextValue) == Long.MIN_VALUE)
                throw new NoSuchElementException();
            advance(this.next);
            return v;
        }
    }

    private transient Index head;

    private static final AtomicLong seeder = new AtomicLong(
            mix64(System.currentTimeMillis()) ^ mix64(System.nanoTime()));

    public long merge(int key, long value, LongBiFunction remappingFunction) {
        if (key == Integer.MIN_VALUE || value == Long.MIN_VALUE
                || remappingFunction == null)
            throw new NullPointerException();
        for (;;) {
            Node n;
            long v, r;
            if ((n = findNode(key)) == null) {
                if (doPut(key, value, true) == Long.MIN_VALUE)
                    return value;
            } else if ((v = n.val) != Long.MIN_VALUE) {
                if ((r = remappingFunction.apply(v, value)) != Long.MIN_VALUE) {
                    if (U.compareAndSwapLong(n, VAL, v, r)) {
                        return r;
                    }
                } else if (doRemove(key, v) != Long.MIN_VALUE)
                    return Long.MIN_VALUE;
            }
        }
    }

    public ValueIterator values() {
        return new ValueIterator();
    }

    static boolean addIndices(Index q, int skips, Index x) {
        Node z;
        int key;
        if (x != null && (z = x.node) != null
                && (key = z.key) != Integer.MIN_VALUE && q != null) {
            boolean retrying = false;
            for (;;) {
                Index r, d;
                int c;
                if ((r = q.right) != null) {
                    Node p;
                    int k;
                    if ((p = r.node) == null || (k = p.key) == Integer.MIN_VALUE
                            || p.val == Long.MIN_VALUE) {
                        U.compareAndSwapObject(q, RIGHT, r, r.right);
                        c = 0;
                    } else if ((c = key > k ? 1 : (key < k ? -1 : 0)) > 0)
                        q = r;
                    else if (c == 0)
                        break;
                } else
                    c = -1;

                if (c < 0) {
                    if ((d = q.down) != null && skips > 0) {
                        --skips;
                        q = d;
                    } else if (d != null && !retrying
                            && !addIndices(d, 0, x.down))
                        break;
                    else {
                        x.right = r;
                        if (U.compareAndSwapObject(q, RIGHT, r, x))
                            return true;
                        retrying = true;
                    }
                }
            }
        }
        return false;
    }

    final Node baseHead() {
        Index h;
        U.loadFence();
        return ((h = this.head) == null) ? null : h.node;
    }

    private long doPut(int key, long value, boolean onlyIfAbsent) {
        if (key == Integer.MIN_VALUE)
            throw new NullPointerException();
        for (;;) {
            Index h;
            Node b;
            U.loadFence();
            int levels = 0;
            if ((h = this.head) == null) {
                Node base = new Node(Integer.MIN_VALUE, Long.MIN_VALUE, null);
                h = new Index(base, null, null);
                b = (U.compareAndSwapObject(this, HEAD, null, h)) ? base : null;
            } else {
                for (Index q = h, r, d;;) {
                    while ((r = q.right) != null) {
                        Node p;
                        int k;
                        if ((p = r.node) == null
                                || (k = p.key) == Integer.MIN_VALUE
                                || p.val == Long.MIN_VALUE) {
                            U.compareAndSwapObject(q, RIGHT, r, r.right);
                        } else if (key > k)
                            q = r;
                        else
                            break;
                    }
                    if ((d = q.down) != null) {
                        ++levels;
                        q = d;
                    } else {
                        b = q.node;
                        break;
                    }
                }
            }
            if (b != null) {
                Node z = null;
                for (;;) {
                    Node n, p;
                    int k, c;
                    long v;
                    if ((n = b.next) == null) {
                        c = -1;
                    } else if ((k = n.key) == Integer.MIN_VALUE)
                        break;
                    else if ((v = n.val) == Long.MIN_VALUE) {
                        unlinkNode(b, n);
                        c = 1;
                    } else if ((c = key > k ? 1 : (key < k ? -1 : 0)) > 0)
                        b = n;
                    else if (c == 0 && (onlyIfAbsent
                            || U.compareAndSwapLong(n, VAL, v, value)))
                        return v;

                    if (c < 0 && U.compareAndSwapObject(b, NEXT, n,
                            p = new Node(key, value, n))) {
                        z = p;
                        break;
                    }
                }

                if (z != null) {
                    int lr = nextSecondarySeed();
                    if ((lr & 0x3) == 0) {
                        int hr = nextSecondarySeed();
                        long rnd = ((long) hr << 32) | (lr & 0xffffffffL);
                        int skips = levels;
                        Index x = null;
                        for (;;) {
                            x = new Index(z, x, null);
                            if (rnd >= 0L || --skips < 0)
                                break;
                            rnd <<= 1;
                        }
                        if (addIndices(h, skips, x) && skips < 0
                                && this.head == h) {
                            Index hx = new Index(z, x, null);
                            Index nh = new Index(h.node, h, hx);
                            U.compareAndSwapObject(this, HEAD, h, nh);
                        }
                        if (z.val == Long.MIN_VALUE)
                            findPredecessor(key);
                    }
                    return Long.MIN_VALUE;
                }
            }
        }
    }

    final long doRemove(int key, long value) {
        if (key == Integer.MIN_VALUE)
            throw new NullPointerException();
        long result = Long.MIN_VALUE;
        Node b;
        outer: while ((b = findPredecessor(key)) != null
                && result == Long.MIN_VALUE) {
            for (;;) {
                Node n;
                int k, c;
                long v;
                if ((n = b.next) == null)
                    break outer;
                else if ((k = n.key) == Integer.MIN_VALUE)
                    break;
                else if ((v = n.val) == Long.MIN_VALUE)
                    unlinkNode(b, n);
                else if ((c = key > k ? 1 : (key < k ? -1 : 0)) > 0)
                    b = n;
                else if (c < 0)
                    break outer;
                else if (value != Long.MIN_VALUE && value != v)
                    break outer;
                else if (U.compareAndSwapLong(n, VAL, v, Long.MIN_VALUE)) {
                    result = v;
                    unlinkNode(b, n);
                    break;
                }
            }
        }
        if (result != Long.MIN_VALUE) {
            tryReduceLevel();
        }
        return result;
    }

    private Node findNode(int key) {
        if (key == Integer.MIN_VALUE)
            throw new NullPointerException();
        Node b;
        outer: while ((b = findPredecessor(key)) != null) {
            for (;;) {
                Node n;
                int k, c;
                if ((n = b.next) == null)
                    break outer;
                else if ((k = n.key) == Integer.MIN_VALUE)
                    break;
                else if (n.val == Long.MIN_VALUE)
                    unlinkNode(b, n);
                else if ((c = key > k ? 1 : (key < k ? -1 : 0)) > 0)
                    b = n;
                else if (c == 0)
                    return n;
                else
                    break outer;
            }
        }
        return null;
    }

    private Node findPredecessor(int key) {
        Index q;
        U.loadFence();
        if ((q = this.head) == null || key == Integer.MIN_VALUE)
            return null;
        for (Index r, d;;) {
            while ((r = q.right) != null) {
                Node p;
                int k;
                if ((p = r.node) == null || (k = p.key) == Integer.MIN_VALUE
                        || p.val == Long.MIN_VALUE) {
                    U.compareAndSwapObject(q, RIGHT, r, r.right);
                } else if (key > k)
                    q = r;
                else
                    break;
            }
            if ((d = q.down) != null)
                q = d;
            else
                return q.node;
        }
    }

    private void tryReduceLevel() {
        Index h, d, e;
        if ((h = this.head) != null && h.right == null && (d = h.down) != null
                && d.right == null && (e = d.down) != null && e.right == null
                && U.compareAndSwapObject(this, HEAD, h, d)
                && h.right != null) {
            U.compareAndSwapObject(this, HEAD, d, h);
        }
    }

    static void unlinkNode(Node b, Node n) {
        if (b != null && n != null) {
            Node f, p;
            for (;;) {
                if ((f = n.next) != null && f.key == Integer.MIN_VALUE) {
                    p = f.next;
                    break;
                } else if (U.compareAndSwapObject(n, NEXT, f,
                        new Node(Integer.MIN_VALUE, Long.MIN_VALUE, f))) {
                    p = f;
                    break;
                }
            }
            U.compareAndSwapObject(b, NEXT, n, p);
        }
    }

    // ThreadLocalRandom
    // http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/jdk8/java/util/concurrent/ThreadLocalRandom.java?revision=1.3

    private static int mix32(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        return (int) (((z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L) >>> 32);
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    static final int nextSecondarySeed() {
        int r;
        Thread t = Thread.currentThread();
        if ((r = U.getInt(t, SECONDARY)) != 0) {
            r ^= r << 13;
            r ^= r >>> 17;
            r ^= r << 5;
        } else if ((r = mix32(seeder.getAndAdd(0xbb67ae8584caa73bL))) == 0)
            r = 1;
        U.putInt(t, SECONDARY, r);
        return r;
    }

    private static final sun.misc.Unsafe U;
    private static final long HEAD;
    private static final long NEXT;
    private static final long VAL;
    private static final long RIGHT;
    private static final long SECONDARY;
    static {
        try {
            Constructor<sun.misc.Unsafe> unsafeConstructor = sun.misc.Unsafe.class
                    .getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            U = unsafeConstructor.newInstance();
            HEAD = U.objectFieldOffset(ConcurrentIntLongSkipListMap.class
                    .getDeclaredField("head"));
            NEXT = U.objectFieldOffset(Node.class.getDeclaredField("next"));
            VAL = U.objectFieldOffset(Node.class.getDeclaredField("val"));
            RIGHT = U.objectFieldOffset(Index.class.getDeclaredField("right"));
            SECONDARY = U.objectFieldOffset(Thread.class
                    .getDeclaredField("threadLocalRandomSecondarySeed"));
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }
}
