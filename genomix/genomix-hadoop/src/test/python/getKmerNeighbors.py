__author__ = 'wbiesing'

import sys
import string




def getneighbors(readids, kmerlength, reads):
    "return a set of readids containing any of the kmers in the given readid"
    neighbors = set()
    for origreadid in readids:
        print 'searching ', origreadid
        seq = reads[origreadid]
        for i in range(len(seq) - kmerlength):
            subseq = seq[i:i+kmerlength]
            revsubseq = revcomp(subseq)
            neighbors.update(readid for readid, otherseq in reads.iteritems() if subseq in otherseq or revsubseq in otherseq)
    return neighbors


_revcomp_table = string.maketrans('ACGT', 'TGCA')
def revcomp(s):
    return string.translate(s, _revcomp_table)[::-1]


def main(argv):
    origkmer = argv[1]
    infile = argv[2]
    numhops = int(argv[3])
    outname = argv[4]

    kmerlength = len(origkmer)

    print 'reading in', infile
    reads = {line.strip().split('\t')[0] : line.strip().split('\t')[1] for line in open(infile)}
    outreads = set()

    print 'getting matching readids'
    curkmer = origkmer
    for readid, seq in reads.iteritems():
        if curkmer in seq or revcomp(curkmer) in seq:
            outreads.add(readid)

    # get neighbors
    new_neighbors = outreads
    for i in range(numhops):
        print 'hop', i
        new_neighbors = getneighbors(new_neighbors, kmerlength, reads)
        outreads.update(new_neighbors)

    print 'found', len(outreads), 'neighbors'

    with open(outname, 'w') as outfile:
        for readid in outreads:
            outfile.write(readid + '\t' + reads[readid] + '\n')



if __name__ == '__main__':
    main(sys.argv)