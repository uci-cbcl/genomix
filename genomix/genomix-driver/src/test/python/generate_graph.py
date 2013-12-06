#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate a random smattering of reads
"""

import sys
import argparse
import random
import itertools
import string


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--walk', '-w', action='store_true', help="walk the genome in regular intervals rather than placing the reads randomly")
    parser.add_argument('--coverage', '-c', type=float, required=True)
    parser.add_argument('--genome-length', '-g', type=int,
                        required=True)
    parser.add_argument('--read-length', '-l', type=int, required=True)
    parser.add_argument('--no-rc', action='store_true')
    parser.add_argument('--error-rate', type=float, default=.01)
    parser.add_argument('--outreads', '-r', type=argparse.FileType('w'),
                        default='reads.txt')
    parser.add_argument('--outgenome', '-o', type=argparse.FileType('w'),
                        default='genome.txt')
    return parser


def reverse_complement(kmer, _table=string.maketrans('ACGT', 'TGCA')):
    return string.translate(kmer, _table)[::-1]


def make_genome(length):
    return ''.join(random.choice('ACGT') for i in xrange(length))


def make_reads(genome, read_length, coverage, walk=False, no_rc=False,
               error_rate=0.):
    num_reads = int(coverage * len(genome)) / read_length
    if walk:
        step_size = max(1, int(len(genome) / num_reads))
        next_starts = itertools.cycle(xrange(0, len(genome) - read_length + 1,
                                            step_size))
    else:
        next_starts = (random.randrange(len(genome) - read_length) for i in itertools.cycle([None]))
    num_errors = 0
    for i in range(1, num_reads + 1):
        start = next_starts.next()
        seq = genome[start:start + read_length]
        if not no_rc and random.choice([True, False]):
            seq = reverse_complement(seq)
        final_seq = []
        for l in seq:
            if random.random() < error_rate:
                num_errors += 1
                final_seq.append(random.choice(list(set('ATGC') - set(l))))
            else:
                final_seq.append(l)

        yield '%s\t%s\n' % (i, ''.join(final_seq))
    print >> sys.stderr, 'introduced', num_errors, 'errors'


def main(args):
    parser = get_parser()
    args = parser.parse_args(args)
    genome = make_genome(args.genome_length)
    args.outgenome.write(genome)
    args.outgenome.write('\n')
    args.outreads.writelines(make_reads(genome, args.read_length,
                                        args.coverage, args.walk, args.no_rc,
                                        args.error_rate))


if __name__ == '__main__':
    main(sys.argv[1:])
