package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.data.Marshal;


/**
 * A list of fixed-length kmers.  The length of this list is stored internally 
 *
 */
public class KmerListWritable implements Writable, Iterable<KmerBytesWritable>,
		Serializable {
	private static final long serialVersionUID = 1L;
	protected static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 };
	protected static final int HEADER_SIZE = 4;

	protected byte[] storage;
	protected int offset;
	protected int valueCount;

	protected int bytesPerKmer = 0;
	protected int lettersPerKmer = 0;
	private KmerBytesWritable posIter = new KmerBytesWritable();

	public KmerListWritable() {
		this.storage = EMPTY_BYTES;
		this.valueCount = 0;
		this.offset = 0;
	}

	public KmerListWritable(int kmerlength) {
		this();
		this.lettersPerKmer = kmerlength;
		this.bytesPerKmer = KmerUtil.getByteNumFromK(kmerlength);
	}

	public KmerListWritable(int kmerlength, byte[] data, int offset) {
		this.lettersPerKmer = kmerlength;
		this.bytesPerKmer = KmerUtil.getByteNumFromK(kmerlength);
		setNewReference(data, offset);
	}

	public KmerListWritable(List<KmerBytesWritable> kmers) {
		this();
		setSize(kmers.size()); // reserve space for all elements
		for (KmerBytesWritable kmer : kmers) {
			if (kmer.getKmerLength() != lettersPerKmer)
				throw new IllegalArgumentException("Kmer " + kmer.toString()
						+ " is of incorrect length (l=" + kmer.getKmerLength()
						+ ") for this list (should be " + lettersPerKmer + ").");
			append(kmer);
		}
	}

	public void setNewReference(byte[] data, int offset) {
		this.valueCount = Marshal.getInt(data, offset);
		this.storage = data;
		this.offset = offset;
	}

	public void append(KmerBytesWritable kmer) {
		setSize((1 + valueCount) * bytesPerKmer);
		System.arraycopy(kmer.getBytes(), 0, storage, offset + valueCount
				* bytesPerKmer, bytesPerKmer);
		valueCount += 1;
	}

	/*
	 * Append the otherList to the end of myList
	 */
	public void appendList(KmerListWritable otherList) {
		if (otherList.valueCount > 0) {
			setSize((valueCount + otherList.valueCount) * bytesPerKmer);
			// copy contents of otherList into the end of my storage
			System.arraycopy(otherList.storage, otherList.offset, storage,
					offset + valueCount * bytesPerKmer, otherList.valueCount
							* bytesPerKmer);
			valueCount += otherList.valueCount;
		}
	}

	/**
	 * Save the union of my list and otherList. Uses a temporary HashSet for
	 * uniquefication
	 */
	public void unionUpdate(KmerListWritable otherList) {
		int newSize = valueCount + otherList.valueCount;
		HashSet<KmerBytesWritable> uniqueElements = new HashSet<KmerBytesWritable>(
				newSize);
		for (KmerBytesWritable kmer : this) {
			uniqueElements.add(kmer);
		}
		for (KmerBytesWritable kmer : otherList) {
			uniqueElements.add(kmer);
		}
		valueCount = 0;
		setSize(newSize);
		for (KmerBytesWritable kmer : uniqueElements) {
			append(kmer);
		}
	}

	protected void setSize(int size) {
		if (size > getCapacity()) {
			setCapacity((size * 3 / 2));
		}
	}

	protected int getCapacity() {
		return storage.length - offset;
	}

	protected void setCapacity(int new_cap) {
		if (new_cap > getCapacity()) {
			byte[] new_data = new byte[new_cap];
			if (storage.length - offset > 0) {
				System.arraycopy(storage, offset, new_data, 0, storage.length
						- offset);
			}
			storage = new_data;
			offset = 0;
		}
	}

	public void reset(int kmerSize) {
		lettersPerKmer = kmerSize;
		bytesPerKmer = KmerUtil.getByteNumFromK(lettersPerKmer);
		storage = EMPTY_BYTES;
		valueCount = 0;
		offset = 0;
	}

	public KmerBytesWritable getPosition(int i) {
		if (i >= valueCount) {
			throw new ArrayIndexOutOfBoundsException("No such positions");
		}
		posIter.setAsReference(lettersPerKmer, storage, offset + i
				* bytesPerKmer);
		return posIter;
	}

	public void set(KmerListWritable otherList) {
		this.lettersPerKmer = otherList.lettersPerKmer;
		this.bytesPerKmer = otherList.bytesPerKmer;
		set(otherList.valueCount, otherList.storage, otherList.offset);
	}

	public void set(int valueCount, byte[] newData, int offset) {
		this.valueCount = valueCount;
		setSize(valueCount * bytesPerKmer);
		if (valueCount > 0) {
			System.arraycopy(newData, offset, storage, this.offset, valueCount
					* bytesPerKmer);
		}
	}

	@Override
	public Iterator<KmerBytesWritable> iterator() {
		Iterator<KmerBytesWritable> it = new Iterator<KmerBytesWritable>() {

			private int currentIndex = 0;

			@Override
			public boolean hasNext() {
				return currentIndex < valueCount;
			}

			@Override
			public KmerBytesWritable next() {
				return getPosition(currentIndex++);
			}

			@Override
			public void remove() {
				if (currentIndex < valueCount)
					System.arraycopy(storage, offset + currentIndex
							* bytesPerKmer, storage, offset
							+ (currentIndex - 1) * bytesPerKmer,
							(valueCount - currentIndex) * bytesPerKmer);
				valueCount--;
				currentIndex--;
			}
		};
		return it;
	}

	/*
	 * remove the first instance of `toRemove`. Uses a linear scan. Throws an
	 * exception if not in this list.
	 */
	public void remove(KmerBytesWritable toRemove, boolean ignoreMissing) {
		Iterator<KmerBytesWritable> posIterator = this.iterator();
		while (posIterator.hasNext()) {
			if (toRemove.equals(posIterator.next())) {
				posIterator.remove();
				return;
			}
		}
		if (!ignoreMissing) {
			throw new ArrayIndexOutOfBoundsException("the KmerBytesWritable `"
					+ toRemove.toString() + "` was not found in this list.");
		}
	}

	public void remove(KmerBytesWritable toRemove) {
		remove(toRemove, false);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.valueCount = in.readInt();
		setSize(valueCount * bytesPerKmer);// kmerByteSize
		in.readFully(storage, offset, valueCount * bytesPerKmer);// kmerByteSize
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(valueCount);
		out.write(storage, offset, valueCount * bytesPerKmer);
	}

	public int getCountOfPosition() {
		return valueCount;
	}

	public byte[] getByteArray() {
		return storage;
	}

	public int getStartOffset() {
		return offset;
	}

	public int getLength() {
		return valueCount * bytesPerKmer;
	}

	@Override
	public String toString() {
		StringBuilder sbuilder = new StringBuilder();
		sbuilder.append('[');
		for (int i = 0; i < valueCount; i++) {
			sbuilder.append(getPosition(i).toString());
			sbuilder.append(',');
		}
		if (valueCount > 0) {
			sbuilder.setCharAt(sbuilder.length() - 1, ']');
		} else {
			sbuilder.append(']');
		}
		return sbuilder.toString();
	}

	@Override
	public int hashCode() {
		return Marshal.hashBytes(getByteArray(), getStartOffset(), getLength());
	}
}
