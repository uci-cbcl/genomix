package edu.uci.ics.genomix.experiment;

public class VertexIDList {

    private int[] readIDList;
    private byte[] posInReadList;
    private static final int[] EMPTY_INTS = {};
    private static final byte[] EMPTY_BYTES = {};

    private int usedSize;
    private int arraySize;

    public VertexIDList() {
        this(0, 0, EMPTY_INTS, EMPTY_BYTES);
    }

    public VertexIDList(int usedSize, int arraySize, int[] rList, byte[] pList) {
        this.usedSize = usedSize;
        this.arraySize = arraySize;
        if (arraySize > 0) {
            this.readIDList = rList;
            this.posInReadList = pList;
            if (this.readIDList.length != arraySize | this.posInReadList.length != arraySize) {
                throw new ArrayIndexOutOfBoundsException("the arraySize doesn't corespond to the array");
            }
            if (this.readIDList.length < usedSize | this.posInReadList.length < usedSize) {
                throw new ArrayIndexOutOfBoundsException("the usedSize doesn't corespond to the array");
            }
        } else {
            this.readIDList = rList;
            this.posInReadList = pList;
            this.arraySize = 0;
            this.usedSize = 0;
        }
    }

    public VertexIDList(int arraySize) {
        this.arraySize = arraySize;
        this.usedSize = 0;
        if (arraySize > 0) {
            this.readIDList = new int[this.arraySize];
            this.posInReadList = new byte[this.arraySize];
        } else {
            this.readIDList = EMPTY_INTS;
            this.posInReadList = EMPTY_BYTES;
        }
    }

    public VertexIDList(VertexIDList right) {
        if (right != null) {
            this.usedSize = right.usedSize;
            this.arraySize = right.arraySize;
            this.readIDList = new int[right.arraySize];
            this.posInReadList = new byte[right.arraySize];
            if (this.readIDList.length != arraySize | this.posInReadList.length != arraySize) {
                throw new ArrayIndexOutOfBoundsException("the arraySize doesn't corespond to the array");
            }
            if (this.readIDList.length < usedSize | this.posInReadList.length < usedSize) {
                throw new ArrayIndexOutOfBoundsException("the usedSize doesn't corespond to the array");
            }
            set(right);
        } else {
            this.arraySize = 0;
            this.usedSize = 0;
            this.readIDList = EMPTY_INTS;
            this.posInReadList = EMPTY_BYTES;
        }
    }

    public void set(VertexIDList newData) {
        set(newData.readIDList, 0, newData.posInReadList, 0, newData.usedSize);
    }

    public void set(int[] rIDList, int rOffset, byte[] pReadList, int pOffset, int copySize) {
        setArraySize(0);
        setArraySize(copySize);
        System.arraycopy(rIDList, rOffset, this.readIDList, 0, copySize);
        System.arraycopy(pReadList, pOffset, this.posInReadList, 0, copySize);
        this.usedSize = copySize;
    }

    public void setArraySize(int arraySize) {
        if (arraySize > getCapacity()) {
            setCapacity((arraySize * 2));
        }
        this.arraySize = arraySize;
    }

    public int getCapacity() {
        return this.arraySize;
    }

    public void setCapacity(int new_cap) {
        if (new_cap != getCapacity()) {
            int[] newRList = new int[new_cap];
            byte[] newPList = new byte[new_cap];
            if (new_cap < this.arraySize) {
                this.arraySize = new_cap;
            }
            if (this.arraySize != 0) {
                System.arraycopy(this.readIDList, 0, newRList, 0, this.usedSize);
                System.arraycopy(this.posInReadList, 0, newPList, 0, this.usedSize);
            }
            this.readIDList = newRList;
            this.posInReadList = newPList;
        }
    }

    public int getReadListElement(int position) {
        if (position < this.usedSize) {
            return this.readIDList[position];
        } else {
            throw new ArrayIndexOutOfBoundsException("position exceed for the usedSize");
        }
    }

    public byte getPosinReadListElement(int position) {
        if (position < this.usedSize) {
            return this.posInReadList[position];
        } else {
            throw new ArrayIndexOutOfBoundsException("position exceed for the usedSize");
        }
    }

    public int findReadListElement(int rContent) {
        for (int i = 0; i < this.usedSize; i++) {
            if (this.readIDList[i] == rContent)
                return i;
        }
        return -1;
    }

    public int findPosinReadListElement(int pContent) {
        for (int i = 0; i < this.usedSize; i++) {
            if (this.usedSize == pContent)
                return i;
        }
        return -1;
    }

    public void addELementToList(int rContent, byte pContent) {
        if (this.usedSize < this.arraySize) {
            this.readIDList[this.usedSize] = rContent;
            this.posInReadList[this.usedSize] = pContent;
            this.usedSize++;
        } else {
            setCapacity((this.arraySize * 2));
            this.readIDList[this.usedSize] = rContent;
            this.posInReadList[this.usedSize] = pContent;
            this.usedSize++;
        }
    }

    public void deleteElementFromTwoList(int position) {
        if (position < this.usedSize) {
            for (int i = position; i < this.usedSize; i++) {
                this.readIDList[i] = this.readIDList[i + 1];
                this.posInReadList[i] = this.posInReadList[i + 1];
            }
            this.readIDList[this.usedSize - 1] = -1;
            this.posInReadList[this.usedSize - 1] = (byte) -1;
            this.usedSize--;
        } else {
            throw new ArrayIndexOutOfBoundsException("position exceed for the usedSize");
        }
    }
    
    public int[] getReadIDList() {
        return this.readIDList;
    }
    
    public byte[] getPosInReadList() {
        return this.posInReadList;
    }
    
    public int getUsedSize() {
        return this.usedSize;
    }
    
    public int getArraySize() {
        return this.arraySize;
    }
}
