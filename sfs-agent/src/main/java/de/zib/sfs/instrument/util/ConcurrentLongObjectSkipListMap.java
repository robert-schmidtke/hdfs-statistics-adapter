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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

// greatly reduced and long-Object-specialized version
@SuppressWarnings("restriction")
public class ConcurrentLongObjectSkipListMap<V> {

    public static interface LongObjectFunction<V> {
        public V apply(long key);
    }

    static final class Node<V> {
        final long key;
        V val;
        Node<V> next;

        Node(long key, V value, Node<V> next) {
            this.key = key;
            this.val = value;
            this.next = next;
        }
    }

    static final class Index<V> {
        final Node<V> node;
        final Index<V> down;
        Index<V> right;

        Index(Node<V> node, Index<V> down, Index<V> right) {
            this.node = node;
            this.down = down;
            this.right = right;
        }
    }

    private transient Index<V> head;

    private transient LongAdder adder;

    private static final AtomicLong seeder = new AtomicLong(
            mix64(System.currentTimeMillis()) ^ mix64(System.nanoTime()));

    public V computeIfAbsent(long key, LongObjectFunction<V> mappingFunction) {
        if (key == Long.MIN_VALUE || mappingFunction == null)
            throw new NullPointerException();
        V v, p, r;
        if ((v = doGet(key)) == null
                && (r = mappingFunction.apply(key)) != null)
            v = (p = doPut(key, r, true)) == null ? r : p;
        return v;
    }

    public V poll() {
        Node<V> b, n;
        V v;
        if ((b = baseHead()) != null) {
            while ((n = b.next) != null) {
                if ((v = n.val) == null
                        || U.compareAndSwapObject(n, VAL, v, null)) {
                    long k = n.key;
                    unlinkNode(b, n);
                    if (v != null) {
                        tryReduceLevel();
                        findPredecessor(k);
                        addCount(-1L);
                        return v;
                    }
                }
            }
        }
        return null;
    }

    public int size() {
        long c;
        return ((baseHead() == null) ? 0
                : ((c = getAdderCount()) >= Integer.MAX_VALUE)
                        ? Integer.MAX_VALUE : (int) c);
    }

    private void addCount(long c) {
        LongAdder a;
        do {
            // retry
        } while ((a = this.adder) == null && !U.compareAndSwapObject(this,
                ADDER, null, a = new LongAdder()));
        a.add(c);
    }

    static <V> boolean addIndices(Index<V> q, int skips, Index<V> x) {
        Node<V> z;
        long key;
        if (x != null && (z = x.node) != null && (key = z.key) != Long.MIN_VALUE
                && q != null) {
            boolean retrying = false;
            for (;;) {
                Index<V> r, d;
                int c;
                if ((r = q.right) != null) {
                    Node<V> p;
                    long k;
                    if ((p = r.node) == null || (k = p.key) == Long.MIN_VALUE
                            || p.val == null) {
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

    final Node<V> baseHead() {
        Index<V> h;
        U.loadFence();
        return ((h = this.head) == null) ? null : h.node;
    }

    private V doGet(long key) {
        Index<V> q;
        U.loadFence();
        if (key == Long.MIN_VALUE)
            throw new NullPointerException();
        V result = null;
        if ((q = this.head) != null) {
            outer: for (Index<V> r, d;;) {
                while ((r = q.right) != null) {
                    Node<V> p;
                    long k;
                    V v;
                    int c;
                    if ((p = r.node) == null || (k = p.key) == Long.MIN_VALUE
                            || (v = p.val) == null) {
                        U.compareAndSwapObject(q, RIGHT, r, r.right);
                    } else if ((c = key > k ? 1 : (key < k ? -1 : 0)) > 0)
                        q = r;
                    else if (c == 0) {
                        result = v;
                        break outer;
                    } else
                        break;
                }
                if ((d = q.down) != null)
                    q = d;
                else {
                    Node<V> b, n;
                    if ((b = q.node) != null) {
                        while ((n = b.next) != null) {
                            V v;
                            int c;
                            long k = n.key;
                            if ((v = n.val) == null || k == Long.MIN_VALUE
                                    || (c = key > k ? 1
                                            : (key < k ? -1 : 0)) > 0)
                                b = n;
                            else {
                                if (c == 0)
                                    result = v;
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
        return result;
    }

    private V doPut(long key, V value, boolean onlyIfAbsent) {
        if (key == Long.MIN_VALUE)
            throw new NullPointerException();
        for (;;) {
            Index<V> h;
            Node<V> b;
            U.loadFence();
            int levels = 0;
            if ((h = this.head) == null) {
                Node<V> base = new Node<>(Long.MIN_VALUE, null, null);
                h = new Index<>(base, null, null);
                b = (U.compareAndSwapObject(this, HEAD, null, h)) ? base : null;
            } else {
                for (Index<V> q = h, r, d;;) {
                    while ((r = q.right) != null) {
                        Node<V> p;
                        long k;
                        if ((p = r.node) == null
                                || (k = p.key) == Long.MIN_VALUE
                                || p.val == null) {
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
                Node<V> z = null;
                for (;;) {
                    Node<V> n, p;
                    long k;
                    V v;
                    int c;
                    if ((n = b.next) == null) {
                        c = -1;
                    } else if ((k = n.key) == Long.MIN_VALUE)
                        break;
                    else if ((v = n.val) == null) {
                        unlinkNode(b, n);
                        c = 1;
                    } else if ((c = key > k ? 1 : (key < k ? -1 : 0)) > 0)
                        b = n;
                    else if (c == 0 && (onlyIfAbsent
                            || U.compareAndSwapObject(n, VAL, v, value)))
                        return v;

                    if (c < 0 && U.compareAndSwapObject(b, NEXT, n,
                            p = new Node<>(key, value, n))) {
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
                        Index<V> x = null;
                        for (;;) {
                            x = new Index<>(z, x, null);
                            if (rnd >= 0L || --skips < 0)
                                break;
                            rnd <<= 1;
                        }
                        if (addIndices(h, skips, x) && skips < 0
                                && this.head == h) {
                            Index<V> hx = new Index<>(z, x, null);
                            Index<V> nh = new Index<>(h.node, h, hx);
                            U.compareAndSwapObject(this, HEAD, h, nh);
                        }
                        if (z.val == null)
                            findPredecessor(key);
                    }
                    addCount(1L);
                    return null;
                }
            }
        }
    }

    private Node<V> findPredecessor(long key) {
        Index<V> q;
        U.loadFence();
        if ((q = this.head) == null || key == Long.MIN_VALUE)
            return null;
        for (Index<V> r, d;;) {
            while ((r = q.right) != null) {
                Node<V> p;
                long k;
                if ((p = r.node) == null || (k = p.key) == Long.MIN_VALUE
                        || p.val == null) {
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

    final long getAdderCount() {
        LongAdder a;
        long c;
        do {
            // retry
        } while ((a = this.adder) == null && !U.compareAndSwapObject(this,
                ADDER, null, a = new LongAdder()));
        return ((c = a.sum()) <= 0L) ? 0L : c;
    }

    private void tryReduceLevel() {
        Index<V> h, d, e;
        if ((h = this.head) != null && h.right == null && (d = h.down) != null
                && d.right == null && (e = d.down) != null && e.right == null
                && U.compareAndSwapObject(this, HEAD, h, d)
                && h.right != null) {
            U.compareAndSwapObject(this, HEAD, d, h);
        }
    }

    static <V> void unlinkNode(Node<V> b, Node<V> n) {
        if (b != null && n != null) {
            Node<V> f, p;
            for (;;) {
                if ((f = n.next) != null && f.key == Long.MIN_VALUE) {
                    p = f.next; // already marked
                    break;
                } else if (U.compareAndSwapObject(n, NEXT, f,
                        new Node<>(Long.MIN_VALUE, null, f))) {
                    p = f; // add marker
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
    private static final long ADDER;
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
            HEAD = U.objectFieldOffset(ConcurrentLongObjectSkipListMap.class
                    .getDeclaredField("head"));
            ADDER = U.objectFieldOffset(ConcurrentLongObjectSkipListMap.class
                    .getDeclaredField("adder"));
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
