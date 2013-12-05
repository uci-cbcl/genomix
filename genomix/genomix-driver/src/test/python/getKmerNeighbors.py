__author__ = 'wbiesing'

import sys
import string
import pickle
from itertools import imap


def getneighbors(ids_to_search, kmerlength, readindex, kmerindex, skipids):
    """return a set of ids_to_search containing any of the kmers in the given readid
    :param dict ids_to_search:
    :param int kmerlength:
    :param dict readindex:
    :param dict kmerindex:
    :param set skipids:
    """
    neighbors = set()
    for rid in ids_to_search:
        print 'searching ', rid
        read = readindex[rid]
        for i in range(len(read) - kmerlength):
            kmer = read[i:i + kmerlength]
            neighbors.update(kmerindex.get(kmer, set()) - skipids)
            neighbors.update(kmerindex.get(revcomp(kmer), set()) - skipids)
    return neighbors


def revcomp(s, _revcomp_table=string.maketrans('ACGT', 'TGCA')):
    """return the reverse-complement of s
    """
    return string.translate(s, _revcomp_table)[::-1]


def main(argv):
    try:
        origkmer = argv[1]
        infile = argv[2]
        numhops = int(argv[3])
        outname = argv[4]
    except IndexError:
        print "usage: <prog> origkmer infile numhops outname"
        return -1

    kmerlength = len(origkmer)

    print 'reading in', infile
    readindex = {rid: read for rid, read in imap(lambda line: line.strip().split('\t'), open(infile))}

    kmerindex = {}
    print 'building index'
    for rid, read in readindex.items():
        for i in range(len(read) - kmerlength):
            kmerindex.setdefault(read[i:i + kmerlength], set()).add(rid)

    outreads = set()

    print 'getting matching readids'
    curkmer = origkmer
    outreads.update(kmerindex.get(curkmer, set()))
    outreads.update(kmerindex.get(revcomp(curkmer), set()))

    # get neighbors
    new_neighbors = outreads
    for i in range(numhops):
        print 'hop', i
        new_neighbors = getneighbors(new_neighbors, kmerlength, readindex, kmerindex, outreads)
        outreads.update(new_neighbors)

    print 'found', len(outreads), 'neighbors'

    with open(outname, 'w') as outfile:
        for rid in outreads:
            outfile.write(rid + '\t' + readindex[rid] + '\n')


if __name__ == '__main__':
    main(sys.argv)
