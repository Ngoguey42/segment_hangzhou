import pandas as pd
import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell(f"""\
# Hanghzou Irmin Pack File Analysis

This document presents statistics computed on a pack file which results from a Tezos bootstrap on the first 2 month of the Hanghzou protocol.

This work is splited on several documents. Look for the "💡" for insights.

The raw data used for this document can be found at `csv/*csv` in the same repository.

### Contents of the Pack File

This bootstrap was realised using Irmin 3.0 and its new structured keys, using the "minimal" indexing strategy, which results in less sharing in the pack file.

The first commit of the pack file belongs to block 1,916,930 (2nd of Hangzhou, 2nd of cycle 429, 4 Dec 2021).

The last commit annalised belongs to block 2,056,194 (2nd of cycle 445, 23 Jan 2022).

### The Tree of the Last Commit
In Irmin, a commit points to a root node (i.e. "/"), which itself references contents and sub-directories. The root node and all the objects reachable from it constitute the "tree of a commit".

Each vertex in that tree is called an object. A new commit is based on an old one, it shares all its objects with the old one except the
objects in the paths between the root and the new (or modified) contents. Because of this, the objects in a tree are scattered all around the pack file.

The stats in this section focus on the tree at the beginning of cycle 445. Many more objects were added during cycle 444 and all the previous cycles, but they were not kept.

In practice, the tree of a commit is a DAG. A subtree can appear multiple times, at different paths. The stats showcased here correspond to what's stored on disk. If a subtree appears multiple times in the tree, it is counted only once in the stats.

[tree_of_cycle_445.ipynb](./tree_of_cycle_445.ipynb) details the contents of the tree.

The notebooks zoom on the heaviest paths of the tree:
- `/data/contracts/index`, the largest node in the tree [(url)](./tree_of_cycle_445_contracts-index.ipynb);
- `/data/contracts/index/*`, the children of the largest node [(url)](./tree_of_cycle_445_contracts-index-star.ipynb);
- `/data/contracts/index/*/manager`, a contents present in almost all contracts [(url)](./tree_of_cycle_445_contracts-index-star-manager.ipynb);
- `/data/big_maps/index/*/contents` [(url)](./tree_of_cycle_445_big_maps-index-star-contents.ipynb);
- `/data/big_maps/index/*/contents/*` [(url)](./tree_of_cycle_445_big_maps-index-star-contents-star.ipynb);
- `/data/big_maps/index/*/contents/*/data` [(url)](./tree_of_cycle_445_big_maps-index-star-contents-star-data.ipynb).


"""))



nb['cells'] = cells
with open('index.ipynb', 'w') as f:
    nbf.write(nb, f)
