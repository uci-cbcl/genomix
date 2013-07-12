#!/usr/bin/env python
"""
Convert a graph to graphviz format and run `dot` on it.

Kmer sequences are included
"""

__author__ = "Jacob Biesinger"
__copyright__ = "Copyright 2009-2013, The Regents of the University of California"
__license__ = "Apache"


import sys
import os
import re
import string
import argparse

import pydot


element_re = re.compile(r"\d+,\d+|\w+")
#edge_colors = dict(FF='black', FR='red', RF='blue', RR='gray')
edge_colors = dict(FF='#DD1E2F', FR='#EBB035', RF='#06A2CB', RR='#218559')


def reverse_complement(kmer, _table=string.maketrans('ACGT', 'TGCA')):
    return string.translate(kmer, _table)[::-1]


def add_legend(graph):
    legend = pydot.Subgraph('cluster_legend', splines='line', rankdir='LR', label='legend', rank='min')
    for i, (edgetype, edgecolor) in enumerate(sorted(edge_colors.items())):
        legend.add_node(pydot.Node('legend_0_' + str(i), label='', shape='point'))
        legend.add_node(pydot.Node('legend_1_' + str(i), label='', shape='point'))
        legend.add_edge(pydot.Edge('legend_0_' + str(i), 'legend_1_' + str(i), label=edgetype, color=edgecolor))
    graph.add_subgraph(legend)
    return graph


def graph_from_file(filename, legend=True, kmers=True, flag=True):
    graph_name = os.path.split(filename)[1].replace('.', '_')
    graph = pydot.Dot(graph_name, graph_type='digraph', rankdir='LR', splines='ortho', weight='2')
    if legend:
        add_legend(graph)

    # annoyingly, order matters. add nodes before any edges or else properties aren't set right
    nodes = {}
    edges = []
    for line in open(filename):
        nodeid, ff, fr, rf, rr, kmer, flag = map(element_re.findall, line.strip().split('\t'))
        nodeid, kmer, flag = nodeid[0], kmer[0], flag[0]
        readid = nodeid.split(',')[0]
        flag = '--%s' % flag if flag else ''
        FF_kmer = '<TR><TD BGCOLOR="%s">%s</TD></TR>' % (edge_colors['FF'], kmer) if kmers else ''
        RR_kmer = '<TR><TD BGCOLOR="%s">%s</TD></TR>' % (edge_colors['RR'], reverse_complement(kmer)) if kmers else ''
        node_label = r'''<<FONT POINT-SIZE="10"><TABLE ALIGN="CENTER" BORDER="0" CELLBORDER="0" CELLSPACING="0">
        <TR><TD>{nodeid}{flag}</TD></TR>
        {FF_kmer}
        {RR_kmer}
        </TABLE></FONT>>'''.format(**locals())
        node = pydot.Node(nodeid, rank=readid, group=readid, label=node_label)
        nodes.setdefault(readid, []).append(node)
        for edgename, edgelist in [('FF', ff), ('FR', fr), ('RF', rf), ('RR', rr)]:
            for e in edgelist:
                edges.append(pydot.Edge(nodeid, e, color=edge_colors[edgename]))

    for readid, subnodes in nodes.items():
        subg = pydot.Subgraph('cluster_' + readid, fillcolor='lightgray')
        for node in subnodes:
            subg.add_node(node)
        graph.add_subgraph(subg)

    for e in edges:
        graph.add_edge(e)

    return graph

def recursive_plot(topdir, suffix='.txt', **kwargs):
    "Recursively plot any files matching `suffix`"
    out_type = kwargs.get('out_type', 'svg')
    for root, dirnames, filenames in os.walk(topdir):
        for filename in filenames:
            f = os.path.join(root, filename)
            if not os.path.isfile(f) or not f.endswith(suffix):
                continue
            try:
                graph = graph_from_file(f, **kwargs)
            except Exception:
                pass
            else:
                print 'plotting', f +'.' + out_type
                graph.write(f + '.' + out_type, format=out_type)


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-legend', action='store_true')
    parser.add_argument('--no-kmers', action='store_true')
    parser.add_argument('--no-flag', action='store_true')

    parser.add_argument('txt_graphs', nargs='*')
    parser.add_argument('--directory', '-d', help='Recurse here and plot all '
                        'graphs that are found.', action='append', default=[])
    parser.add_argument('--out-type', type=str, default='svg')
    return parser


def main(args):
    parser = get_parser()
    args = parser.parse_args(args)
    kwargs = dict(legend=not args.no_legend, kmers=not args.no_kmers,
                  flag=not args.no_flag)
    for filename in args.txt_graphs:
        graph = graph_from_file(filename, **kwargs)
        print 'plotting', filename + args.out_type
        graph.write(filename + args.out_type, format=args.out_type)

    for d in args.directory:
        recursive_plot(d, **kwargs)

if __name__ == '__main__':
    main(sys.argv[1:])
