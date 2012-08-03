/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.util;

import java.lang.reflect.Array;
import java.util.Comparator;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ReferenceMinHeap<T> {

    private final Comparator<T> comparator;

    private final T[] references;

    private int itemCount;

    @SuppressWarnings("unchecked")
    public ReferenceMinHeap(Class<T> tClass, Comparator<T> comparator, int maxSize) {
        this.comparator = comparator;
        this.references = (T[]) Array.newInstance(tClass, maxSize);
        this.itemCount = 0;
    }

    public void insert(T t) throws HyracksDataException {
        if (itemCount == references.length) {
            throw new HyracksDataException("Failed to insert a reference to a full heap!");
        }
        references[itemCount] = t;
        itemCount++;
        if (itemCount > 1) {
            increaseKey(itemCount - 1);
        }
    }

    private void increaseKey(int index) {
        int parentIndex = getParent(index);
        while (index > 0 && comparator.compare(references[parentIndex], references[index]) > 0) {
            swap(parentIndex, index);
            index = parentIndex;
            parentIndex = getParent(index);
        }
    }

    private void heapfy(int index) {
        int l = getLeftChild(index);
        int r = getRightChild(index);
        int topIndex = index;
        if (l < itemCount) {
            if (comparator.compare(references[l], references[topIndex]) < 0) {
                topIndex = l;
            }
        }
        if (r < itemCount) {
            if (comparator.compare(references[r], references[topIndex]) < 0) {
                topIndex = r;
            }
        }
        if (topIndex != index) {
            swap(topIndex, index);
            heapfy(topIndex);
        }
    }

    public T pop() {
        T top = references[0];
        swap(0, itemCount - 1);
        itemCount--;
        heapfy(0);
        return top;
    }

    public T popAndReplace(T newT) {
        T top = references[0];
        references[0] = newT;
        heapfy(0);
        return top;
    }

    public void heapfyTop() {
        heapfy(0);
    }

    public T top() {
        return references[0];
    }

    public boolean isEmpty() {
        return itemCount <= 0;
    }

    public void reset() {
        for (int i = 0; i < references.length; i++) {
            references[i] = null;
        }
        itemCount = 0;
    }

    public int getItemCount() {
        return itemCount;
    }

    private int getParent(int index) {
        return ((index + 1) >> 1) - 1;
    }

    private int getLeftChild(int index) {
        return (index << 1) + 1;
    }

    private int getRightChild(int index) {
        return (index << 1) + 2;
    }

    private void swap(int index1, int index2) {
        T t = references[index1];
        references[index1] = references[index2];
        references[index2] = t;
    }

}
