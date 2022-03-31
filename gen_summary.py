import pandas as pd
import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell(f"""\
# Hanghzou Irmin Pack File Analysis

This document presents statistics computed on a pack file which results from a Tezos bootstrapping on the first 2 month of the Hanghzou protocol.

The goal of this document is to gather insights for future improvements of irmin-pack, notably for the layered store design.

This work is splited on several documents.

The light bulbs 💡 accompanying the plots are insights on what can be seen.

The raw data used for this document can be found at `csv/*csv` in the same repository.

These `.ipynb` files are generated using the `gen_*.py` scripts in the same repository.

### Contents of the Pack File

This bootstrap was realised using Irmin 3.0 and its new structured keys, using the _minimal_ indexing strategy, which results in a bit less sharing in the pack file.

The first commit of the pack file belongs to block 1,916,930 (2nd of Hangzhou, 2nd of cycle 429, 4 Dec 2021).

The last commit analysed belongs to block 2,056,194 (2nd of cycle 445, 23 Jan 2022). This particular commit is analysed in depth in the __Data Distribution in the Tree of the Last Commit__ section.


### What's the _Tree of a Commit_

In Irmin, a commit points to a root directory (i.e. "/"), which itself references files and/or sub-directories. Directories are called _nodes_ and files are called _contents_. Both are _objects_ in Irmin. The root node and all the objects reachable from it constitute the _tree of a commit_.

When the tree of a commit is fully loaded from disk to memory to a `Tree.t` value, it really is a _tree_ in the graph sense. However, when looking at the tree of a commit on-disk in irmin-pack, it is stored as a DAG.

The subtrees that correspond to the same objects in the backend have the same _key_. For irmin-pack, it means having the same _offset_ in the pack file.

Subtrees that have the same key also have the same hash, but the opposite is not true. When irmin-pack's indexing strategy is _always_, there is a bijection between the keys and hashes of persisted objects. When the strategy is _minimal_, there is no such bijection because the hash-consing using _Index_ is disabled, resulting in duplication in the pack file.

Since this document focuses on what's stored on disk in irmin-pack, key-equality is used over hash-equality to reason about object equality. "an object" can then be interpreted as "an offset on disk". Concretely:
- if an object appears at 2 paths, it means that it's key is referenced twice;
- if a subtree appears multiple time at different paths, it is counted only once in the stats;
- the on-disk size of a tree counts the bytes only once without attempting any kind of unrolling.

Regardless of the indexing strategy, in Tezos, the tree of a commit shares more than 99.9% of its disk space with the tree of the preceeding commit. This is happening because in lib_context, the tree of the newer commit was formed by modifying the in-memory tree of the older commit, and that Irmin's `Tree` module is conservative when persisting a tree to backend.

Regardless of the indexing strategy, in Tezos, many contents and nodes appear several time in the tree. This is happening because Tezos tend to copy nodes and contents around, instead of re-creating them from scratch.

### Data Distribution in the Tree of the Last Commit

The stats in this section focus on the tree at the beginning of cycle 445. Many more objects were added during cycle 444 and all the previous cycles, but only a fraction is still referenced by the tree at the beginning of cycle 445.

[tree_of_cycle_445.ipynb](./tree_of_cycle_445.ipynb) details the contents of the tree.

These notebooks zoom on the heaviest paths of the tree:
- `/data/contracts/index`, the largest node in the tree [(url)](./tree_of_cycle_445_contracts-index.ipynb);
- `/data/contracts/index/*`, the children of the largest node [(url)](./tree_of_cycle_445_contracts-index-star.ipynb);
- `/data/contracts/index/*/manager`, a contents present in almost all contracts [(url)](./tree_of_cycle_445_contracts-index-star-manager.ipynb);
- `/data/big_maps/index/*/contents` is where 43% of the bytes of the tree are located [(url)](./tree_of_cycle_445_big_maps-index-star-contents.ipynb);
- `/data/big_maps/index/*/contents/*` are 5 million small nodes [(url)](./tree_of_cycle_445_big_maps-index-star-contents-star.ipynb);
- `/data/big_maps/index/*/contents/*/data` are contents that take up a lot of space [(url)](./tree_of_cycle_445_big_maps-index-star-contents-star-data.ipynb).


"""))



nb['cells'] = cells
with open('index.ipynb', 'w') as f:
    nbf.write(nb, f)
