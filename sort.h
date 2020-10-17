#ifndef _SORT_H
#define _SORT_H

#include <thread>
#include <vector>
#include <cstring>

template<class T>
T* quicksort(T* arr, T* dest, int n, int* st) {
    T* Q;
    if (dest == NULL) {
        Q = arr;
    }
    else {
        memcpy(dest, arr, n*sizeof(T));
        Q = dest;
    }

    int* stack;
    int sp = 0;

    if (st == NULL)
        stack = new int[2*n];
    else
        stack = st;

    stack[sp++] = 0;
    stack[sp++] = n;

    while (sp > 0) {
        int nelems = stack[--sp];
        T* P = Q + stack[--sp];

        // If the number of elements is at most 16 use insertion sort
        if (nelems <= 16) {
            for (int i = nelems - 1; i > 0; i--) {
                T* max = &P[i];
                for (int j = i; j >= 0; j--) {
                    if (P[j] > *max) {
                        max = &P[j];
                    }
                }
                std::swap(P[i], *max);
            }
        
            continue;
        }

        // Use the median of the first, middle, and last element as pivot
        T* pivot = &P[nelems/2];
        if (P[nelems - 1] <= *pivot && P[0] <= *pivot) {
            pivot = (P[nelems - 1] <= P[0]) ? &P[0] : &P[nelems - 1];
        }
        else if (P[nelems - 1] >= *pivot && P[0] >= *pivot) {
            pivot = (P[nelems - 1] >= P[0]) ? &P[0] : &P[nelems - 1];
        }

        // Put elements > pivot on right and < pivot on left
        T* left = &P[0];
        T* right = &P[nelems - 1];
        while (left < right) {
            if (*left > *pivot) {
                while (left < right) {
                    if (*right < *pivot) {
                        std::swap(*left, *right);
                        right--;
                        break;
                    }
                    right--;
                }
            }
            left++;
        }

        // Put pivot in the correct position
        if (*right < *pivot && pivot > right)
            right++;
        else if (*right > *pivot && pivot < right)
            right--;

        std::swap(*right, *pivot);

        stack[sp++] = P - Q;
        stack[sp++] = right - P;
        stack[sp++] = right - Q + 1;
        stack[sp++] = &P[nelems - 1] - right;
    }

    if (st == NULL)
        delete[] stack;

    return Q;
}

// Anonymous namespace containing PSRS internal functions and types
namespace {
    template<class T>
    void psrs_local_sort(T* arr, T* dest, int n, int p, int* offset, int* len) {
        std::vector<std::thread> threads;
        int* mem = new int[2*n];

        if (dest == NULL) {
            for (int i=0; i < p; i++) {
                threads.push_back(std::thread(
                    quicksort<T>, 
                    &arr[offset[i]], 
                    nullptr, 
                    len[i],
                    &mem[2*offset[i]]
                ));
            }
        }
        else {
            for (int i = 0; i < p; i++) {
                threads.push_back(std::thread(
                    quicksort<T>,
                    &arr[offset[i]],
                    &dest[offset[i]],
                    len[i],
                    &mem[2*offset[i]]
                ));
            }
        }

        for (auto &th : threads)
            th.join();

        delete[] mem;
    }

    template<class T>
    void psrs_regular_sample(T* arr, int n, int p, T* pivots) {
        int w = n/p;
        T* regular_sample = new T[p*(p-1)];

        for (int j = 0; j < p; j++) {
            for (int i = 1; i < p; i++) {
                regular_sample[(i-1)*p + j] = arr[i*w/p + j*w];
            }
        }

        quicksort<T>(regular_sample, NULL, p*(p-1), NULL);

        for (int i = 0; i < p - 1; i++) {
            pivots[i] = regular_sample[i*p + p/2];
        }

        delete[] regular_sample;
    }

    template<class T>
    void partition_search(
        T* arr, 
        int p, 
        int* offset, 
        int* len, 
        T pivot, 
        int* merges, 
        int* sum
    ) {
        for (int i = 0; i < p; i++) {
            int left = offset[i];
            int k = offset[i] + len[i];
            int right = k - 1;
            while (right >= left) {
                int m = (left + right)/2;
                if (arr[m] >= pivot) {
                    k = m;
                    right = m - 1;
                } 
                else {
                    left = m + 1;
                }
            }

            merges[i] = k;
            *sum += k;
        }
    }

    template<class T>
    void psrs_partition(
        T* arr, 
        int p, 
        int* offset, 
        int* len, 
        T* pivots, 
        int* merges, 
        int* sizes
    ) {
        std::vector<std::thread> threads;

        for (int i = 1; i < p; i++) {
            threads.push_back(std::thread(
                partition_search<T>,
                arr,
                p,
                offset,
                len,
                pivots[i-1],
                &merges[i*p],
                &sizes[i]
            ));
        }

        for (int i = 0; i < p; i++) {
            merges[i] = offset[i];
            sizes[0] += offset[i];
        }

        for (int i = 0; i < p; i++) {
            merges[p*p + i] = offset[i] + len[i];
            sizes[p] += offset[i] + len[i];
        }

        for (auto &th : threads)
            th.join();
    }

    template<class T>
    union U {
        struct {
            int h;
            int l;
        } lf;
        struct {
            T val;
            int i;
        } in;
    };

    template<class T>
    void pway_merge(
        T* arr, 
        T* dest, 
        int m, 
        int p, 
        int* start, 
        int* end, 
        U<T>* tree, 
        U<T>* winners
    ) {
        int q = 0;
        for (int i = 0; i < p; i++) {
            if (start[i] < end[i])
                q++;
        }

        for (int i = 0, j = 0; i < q; j++) {
            if (start[j] < end[j]) {
                tree[q+i].lf.l = start[j];
                tree[q+i].lf.h = end[j];
                i++;
            }
        }

        if (q == 1) {
            for (int i = 0; i < m; i++) {
                dest[i] = arr[tree[1].lf.l++];
            }
            return;
        }

        for (int i = q - 1; i > 0; i--) {
            if (2*i >= q) {
                int win, lose;
                if (arr[tree[2*i].lf.l] < arr[tree[2*i+1].lf.l]) {
                    win = 2*i;
                    lose = 2*i + 1;
                }
                else {
                    win = 2*i + 1;
                    lose = 2*i;
                }
                winners[i].in.val = arr[tree[win].lf.l];
                winners[i].in.i = win;
                tree[i].in.val = arr[tree[lose].lf.l];
                tree[i].in.i = lose;
            }
            else if (2*i + 1 >= q) {
                if (winners[2*i].in.val < arr[tree[2*i+1].lf.l]) {
                    winners[i] = winners[2*i];
                    tree[i].in.val = arr[tree[2*i+1].lf.l];
                    tree[i].in.i = 2*i+1;
                }
                else {
                    winners[i].in.val = arr[tree[2*i+1].lf.l];
                    winners[i].in.i = 2*i+1;
                    tree[i] = winners[2*i];
                }
            }
            else {
                if (winners[2*i].in.val < winners[2*i+1].in.val) {
                    winners[i] = winners[2*i];
                    tree[i] = winners[2*i+1];
                }
                else {
                    winners[i] = winners[2*i+1];
                    tree[i] = winners[2*i];
                }
            }
        }
        tree[0] = winners[1];

        for (int i = 0; i < m; i++) {
            dest[i] = tree[0].in.val;
            int j = tree[0].in.i;

            if (++tree[j].lf.l >= tree[j].lf.h) {
                for (j /= 2; tree[j].in.i < 0 && j > 0; j /= 2);
                tree[0] = tree[j];
                tree[j].in.i = -1;
            }
            else {
                tree[0].in.val = arr[tree[j].lf.l];
            }

            for (j /= 2; j > 0; j /= 2) {
                if (tree[j].in.val < tree[0].in.val && tree[j].in.i >= 0) {
                    std::swap(tree[j], tree[0]);
                }
            }
        }
    }

    template<class T>
    void psrs_merge(T* arr, T* dest, int n, int p, int* merges, int* sizes) {
        std::vector<std::thread> threads;
        U<T>* mem = new U<T>[3*p*p];

        int offset = 0;
        for (int i = 0; i < p; i++) {
            int m = sizes[i+1] - sizes[i];
            threads.push_back(std::thread(
                pway_merge<T>,
                arr,
                &dest[offset],
                m,
                p,
                &merges[i*p],
                &merges[(i+1)*p],
                &mem[i*3*p],
                &mem[i*3*p + 2*p]
            ));
            offset += m;
        }

        for (auto &th : threads)
            th.join();

        delete[] mem;
    }
}

template<class T>
T* sort(T* arr, T* dest, int n, int p, bool in_place) {
    if (p <= 1 || n < p*p*p) {
        if (!in_place && dest == NULL) {
            T* A = new T[n];
            return quicksort<T>(arr, A, n, NULL);
        }
        else if (!in_place && dest != NULL) {
            return quicksort<T>(arr, dest, n, NULL);
        }
        else {
            return quicksort<T>(arr, NULL, n, NULL);
        }
    }

    int* offset = new int[p];
    int* len = new int[p];

    int w = n/p;
    for (int i = 0; i < p; i++) {
        offset[i] = i*w;
    }

    for (int i = 0; i < p - 1; i++) {
        len[i] = offset[i+1] - offset[i];
    }
    len[p-1] = n - offset[p-1];

    T* A;
    if (in_place) {
        A = arr;
        psrs_local_sort<T>(A, NULL, n, p, offset, len);
    }
    else {
        A = new T[n];
        psrs_local_sort<T>(arr, A, n, p, offset, len);
    }

    T* pivots = new T[p-1];
    psrs_regular_sample<T>(A, n, p, pivots);

    int* merges = new int[p*(p+1)];
    int* sizes = new int[p+1] {0};
    psrs_partition<T>(A, p, offset, len, pivots, merges, sizes);

    T* sorted;
    if (dest == NULL)
        sorted = new T[n];
    else
        sorted = dest;

    psrs_merge<T>(A, sorted, n, p, merges, sizes);

    delete[] sizes;
    delete[] merges;
    delete[] pivots;
    delete[] offset;
    delete[] len;

    if (!in_place)
        delete[] A;
    
    return sorted;
}

#endif
