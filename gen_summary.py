
import pandas as pd
import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell(f"""\
# Hanghzou Irmin Pack File Analysis

This document presents statistics computed on a pack file which results from a Tezos bootstrap on the first 2 month of the Hanghzou protocol.

This bootstrap was realised using Irmin 3.0 and its new structured keys, using the "minimal" indexing strategy, which results in less sharing in the pack file.

The first commit of the pack file is the one belonging to the block 1,916,930 (2nd of Hangzhou, 2nd of cycle 429, 4 Dec 2021).

The last commit of the pack file is the one belonging to the block 2,056,194 (2nd of cycle 445, 23 Jan 2022).

In Irmin, a commit points to a root node (i.e. "/"), which itself reference contents and sub-directories. All these objects constitue the tree of the commit.

This work is splited on several documents. Look for the "💡" for insights of the data.

The tree of the last commit of the pack file is analysed in depth in these notebooks:
- [tree_of_cycle_445.ipynb](./tree_of_cycle_445.ipynb) details the contents of the block,
- zooms on the heaviest paths of the tree:
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
