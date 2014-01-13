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
    parser.add_argument('--insert-size', type=int)
    parser.add_argument('--in-genome', '-i', type=str, required=False)
    parser.add_argument('--genome-length', '-g', type=int, required=False)
    parser.add_argument('--read-length', '-l', type=int, required=True)
    parser.add_argument('--no-rc', action='store_true')
    parser.add_argument('--error-rate', type=float, default=.01)
    parser.add_argument('--out-reads', '-r', type=argparse.FileType('w'),
                        default='reads.txt')
    parser.add_argument('--out-genome', '-o', type=argparse.FileType('w'),
                        default='genome.txt')
    return parser


def reverse_complement(kmer, _table=string.maketrans('ACGT', 'TGCA')):
    return string.translate(kmer, _table)[::-1]


def make_genome(length):
    return ''.join(random.choice('ACGT') for i in xrange(length))


def make_reads(genome, read_length, coverage, walk=False, no_rc=False,
               error_rate=0., insert_size=None):
    num_reads = int(coverage * len(genome)) / read_length
    max_size = read_length if insert_size is None else read_length + insert_size
    if walk:
        step_size = max(1, int(len(genome) / num_reads))
        next_starts = itertools.cycle(xrange(0, len(genome) - max_size + 1,
                                            step_size))
    else:
        next_starts = (random.randrange(len(genome) - max_size) for i in itertools.cycle([None]))
    num_errors = 0
    for i in range(1, num_reads + 1):
        start = next_starts.next()
        seqs = [genome[start:start + read_length]]
        if not no_rc and random.choice([True, False]):
            seqs[0] = reverse_complement(seqs[0])
            if insert_size is not None:
                start2 = start + insert_size
                seq2 = genome[start2:start2 + read_length]
                seqs.append(seq2)
                seqs[0], seqs[1] = reverse_complement(seqs[1]), reverse_complement(seqs[0])
        elif insert_size is not None:
            start2 = start + insert_size
            seq2 = reverse_complement(genome[start2:start2 + read_length])
            seqs.append(seq2)

        final_seqs = [str(i)]
        for s in seqs:
            f = []
            for l in s:
                if random.random() < error_rate:
                    num_errors += 1
                    f.append(random.choice(list(set('ATGC') - set(l))))
                else:
                    f.append(l)
            final_seqs.append(''.join(f))

        yield '\t'.join(final_seqs) + '\n'
    print >> sys.stderr, 'introduced', num_errors, 'errors'


def main(args):
    parser = get_parser()
    args = parser.parse_args(args)
    if args.in_genome is not None:
        genome = ''.join(l.strip() for l in open(args.in_genome) if not l.startswith('>'))
    elif args.genome_length is not None:
        genome = make_genome(args.genome_length)
    else:
        parser.error("Please specify either --in-genome or --genome-length!")
    args.out_genome.write('>synthetic_genome\n%s\n>reverse_complement\n%s\n' % (genome, reverse_complement(genome)))
    args.out_reads.writelines(make_reads(genome, args.read_length,
                                        args.coverage, args.walk, args.no_rc,
                                        args.error_rate, args.insert_size))


if __name__ == '__main__':
    main(sys.argv[1:])
