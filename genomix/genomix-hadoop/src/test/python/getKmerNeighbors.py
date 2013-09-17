__author__ = 'wbiesing'

import sys
import string
import pickle



def getneighbors(readids, kmerlength, reads, index, skipreads):
    "return a set of readids containing any of the kmers in the given readid"
    neighbors = set()
    for origreadid in readids:
        print 'searching ', origreadid
        seq = reads[origreadid]
        for i in range(len(seq) - kmerlength):
            subseq = seq[i:i+kmerlength]
            neighbors.update(index.get(subseq, set()) - skipreads)
            neighbors.update(index.get(revcomp(subseq), set()) - skipreads)
    return neighbors


_revcomp_table = string.maketrans('ACGT', 'TGCA')
def revcomp(s):
    return string.translate(s, _revcomp_table)[::-1]


def main(argv):
    try:
        origkmer = argv[1]
        infile = argv[2]
        numhops = int(argv[3])
        outname = argv[4]
    except:
        print "usage: <prog> origkmer infile numhops outname"
        return -1

    kmerlength = len(origkmer)

    try:
        print 'reading in', infile + ".reads.pkl"
        reads = pickle.load(open(infile + ".reads.pkl"))
    except:
        print 'reading in', infile
        reads = {line.strip().split('\t')[0] : line.strip().split('\t')[1] for line in open(infile)}
        pickle.dump(reads, open(infile + ".reads.pkl", 'w'), -1)

    #try:
    #    print 'reading', infile + '.index.pkl'
    #    index = pickle.load(open(infile + ".index.pkl"))
    #except:
    index = {}
    print 'building index'
    for readid, line in reads.items():
        for i in range(len(line) - kmerlength):
            index.setdefault(line[i:i+kmerlength], set()).add(readid)
    #pickle.dump(index, open(infile + ".index.pkl", 'w'), -1)

    outreads = set()

    print 'getting matching readids'
    curkmer = origkmer
    outreads.update(index.get(curkmer, set()))
    outreads.update(index.get(revcomp(curkmer), set()))

    # get neighbors
    new_neighbors = outreads
    for i in range(numhops):
        print 'hop', i
        new_neighbors = getneighbors(new_neighbors, kmerlength, reads, index, outreads)
        outreads.update(new_neighbors)

    print 'found', len(outreads), 'neighbors'

    with open(outname, 'w') as outfile:
        for readid in outreads:
            outfile.write(readid + '\t' + reads[readid] + '\n')



if __name__ == '__main__':
    main(sys.argv)
