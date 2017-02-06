package SparkPinkMST;
/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
/**
 * Created by cjin on 4/20/14.
 */
public final class UnionFind {
    private int[] id;
    int count;
    public UnionFind(int N) {
        if (N < 0) throw new IllegalArgumentException();
        id = new int[N];
        for (int i = 0; i < N; i++) {
            id[i] = i;
        }
        count = N;
    }

    public int find(int p) {
        if (p < 0 || p >= id.length) {
            System.err.println(" p = " + p);
            System.exit(-1);
        }
        while (p != id[p]) {
            p = id[p];
        }
        return p;
    }

    public int getComponents() {
        return count;
    }

    public boolean isConnected(int left, int right) {
        int i = find(left);
        int j = find(right);
        return (i == j);
    }

    public boolean unify(int left, int right) {
        int i = find(left);
        int j = find(right);
        int big = (i > j) ? i : j;
        compressPath(left, big);
        compressPath(right, big);
        if (i == j) {
            return false;
        } else {
            count--;
            return true;
        }
    }

    void compressPath(int element, int big) {
        int p = element, parent;
        while (id[p] != p) {
            parent = id[p];
            id[p] = big;
            p = parent;
        }
        id[p] = big;
    }
}
