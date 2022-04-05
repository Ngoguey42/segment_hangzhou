"""
To regenerate:

python gen_summary.py && python gen_tree_analysis.py && python gen_areas_and_trees.py && ls -1 *ipynb | xargs -L1 -P8 jupyter nbconvert --to notebook --inplace --execute

"""

import pandas as pd
import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []
markdown = lambda cell: cells.append(nbf.v4.new_markdown_cell((cell)))
code = lambda cell: cells.append(nbf.v4.new_code_cell((cell)))

df = pd.read_csv('csv/areas.csv')
df['bytes'] = df['byte_count']
df['count'] = df['entry_count']

del df['byte_count']
del df['entry_count']

# indicators = 'count node_count inner_count blob_count step_count indirect_count direct_count bytes direct_bytes header_bytes other_bytes'.split(' ')
indicators = 'count node_count inner_count blob_count commit_count bytes node_bytes inner_bytes blob_bytes commit_bytes header_bytes other_bytes'.split(' ')

# Artificially patching [df] to add the last commit. It simplifies explanation
df = df.append(dict(area=445, kind="Commit", contents_size="Na", count=1, bytes=110), ignore_index=True)

df['ekind'] =  df.apply(lambda row: row.contents_size if row.contents_size != "Na" else row.kind, axis=1)
df['node_count'] = df.apply(lambda row: row['count'] if '_root_' in row.kind else 0, axis=1)
df['inner_count'] = df.apply(lambda row: row['count'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_count'] = df.apply(lambda row: row['count'] if 'Contents' == row.kind else 0, axis=1)
df['commit_count'] = df.apply(lambda row: row['count'] if 'Commit' == row.kind else 0, axis=1)

df['node_bytes'] = df.apply(lambda row: row['bytes'] if '_root_' in row.kind else 0, axis=1)
df['inner_bytes'] = df.apply(lambda row: row['bytes'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_bytes'] = df.apply(lambda row: row['bytes'] if 'Contents' == row.kind else 0, axis=1)
df['commit_bytes'] = df.apply(lambda row: row['bytes'] if 'Commit' == row.kind else 0, axis=1)

df['header_bytes'] = df['count'] * 32
df['other_bytes'] = df.bytes - df.header_bytes

d0 = df[indicators].sum()

dtmp = df.groupby(['area', 'kind', 'contents_size', 'ekind']).sum().reset_index()
rows = []
for i in range(df.area.min(), df.area.max() + 1):
    if i == df.area.min():
        lvl = "ø"
    else:
        lvl = '{:,d}'.format(1916930 + 8192 * (i - 428))
    rows.append(dict(
        area=i,
        first_level=lvl,
        start_offset=int(df[df.area < i].sum().bytes),
        end_offset=int(df[df.area <= i].sum().bytes),
        **{
            k: dtmp[dtmp.area == i].sum()[k]
            for k in indicators
        },
    ))
df1 = pd.DataFrame(rows)
df.to_csv('/tmp/summary_df.csv', index=False)
df1.to_csv('/tmp/summary_df1.csv', index=False)
print(df1)

markdown(f"""\
# Hanghzou Irmin Pack File Analysis""")

code(f"""\
%matplotlib inline
%load_ext autoreload
%autoreload 2
import custom_plot_tools""")

markdown(f"""\
This document presents statistics computed on a pack file which results from a Tezos bootstrapping on the first 2 month of the Hanghzou protocol.

The goal of this document is to gather insights for future improvements of irmin-pack, notably for the layered store design.

## Contents of this Work

```
pack file -> OCaml program -> csv files -> notebook generator programs -> notebooks
```

An OCaml program scans the pack file, gathering data that are saved to csv files that can be found in the `csv` directory in the same repository.

For the occasion, was written a pack file tree traversal algorithm ([traverse.ml](./traverse.ml)) that reads the disk pages only once while traversing from high offsets to low offsets. This algorithm can be used to fold over the entries and also to fold over the chunks of consecutive entries (for optimised copies).

The csv results are analysed throughout ipython notebooks (like this one) which are generated using the `gen_*.py` scripts in the same repository.

The results are splitted on several pages 📄.

The light bulbs 💡 accompanying the data are insights on what can be seen.

## Definition: _Tree of a Commit_

In Irmin, a commit points to a root directory (i.e. the "/" path), which itself references files and/or sub-directories. Directories are called _nodes_ and files are called _contents_ or _blobs_. Both are _objects_ in Irmin. The root node and all the objects reachable from it constitute the _tree of a commit_.

When the tree of a commit is fully loaded from disk to memory to a `Tree.t` value, it really is a _tree_ in the graph sense. However, when looking at the tree of a commit on-disk in irmin-pack, it is stored as a DAG.

In memory objects that correspond to a common backend object have the same _key_. For irmin-pack, it means having the same _offset_ in the pack file, since the key stores the offset.

Subtrees that have the same key also have the same hash, but the opposite is not true. When irmin-pack's indexing strategy is _always,_ there is a bijection between the keys and hashes of persisted objects. When the strategy is _minimal,_ there is no such bijection because the hash-consing using _Index_ is disabled, resulting in duplication in the pack file.

Since this document focuses on what's stored on disk in irmin-pack, key-equality is used over hash-equality to reason about object equality. "an object" can then be interpreted as "an offset on disk". Concretely:
- if an object appears at 2 paths, it means that it's key is referenced twice;
- if a subtree appears multiple time at different paths, it is counted only once in the stats;
- the on-disk size of a tree counts the bytes only once without attempting any kind of unrolling.

Regardless of the indexing strategy, in Tezos, the tree of a commit shares more than 99.9% of its disk space with the tree of the preceeding commit. This is happening because in lib_context, the tree of the newer commit was formed by modifying the in-memory tree of the older commit, and that Irmin's `Tree` module is conservative when persisting a tree to backend.

Regardless of the indexing strategy, in Tezos, many contents and nodes appear several time in the tree. This is happening because Tezos tend to copy nodes and contents around, instead of re-creating them from scratch.


## Contents of the Pack File

This bootstraping was realised using Irmin 3.0 and its new structured keys using the _minimal_ indexing strategy, which results in a bit less sharing in the pack file.

Ignoring the genesis commit which is present in all Tezos pack files, the first commit of the pack file belongs to block 1,916,930 (2nd of Hangzhou, 2nd of cycle 428, 4 Dec 2021, https://tzstats.com/1916930). This commit was created by a Tezos snapshot import.

The last commit analysed belongs to block 2,056,194 (2nd of cycle 445, 23 Jan 2022, https://tzstats.com/2056194). This commit is analysed in depth in the __The Tree of Commit 445__ section.


#### Summary
```
Number of objects: {int(d0.loc['count']):,d} (a.k.a. pack file entries). Breakdown:
- {float(d0.loc['blob_count'] / max(1, d0.loc['count'])):4.0%} {int(d0.loc['blob_count']):>11,d} contents (a.k.a. blobs);
- {float(d0.loc['node_count'] / max(1, d0.loc['count'])):4.0%} {int(d0.loc['node_count']):>11,d} nodes (a.k.a. root inodes, directories);
- {float(d0.loc['inner_count'] / max(1, d0.loc['count'])):4.0%} {int(d0.loc['inner_count']):>11,d} hidden nodes (a.k.a. non-root inodes);
- {float(d0.loc['commit_count'] / max(1, d0.loc['count'])):4.0%} {int(d0.loc['commit_count']):>11,d} commits.

Number of bytes: {int(d0.loc['bytes']):,d}. First breakdown:
- {float(d0.loc['blob_bytes'] / max(1, d0.loc['bytes'])):4.0%} {int(d0.loc['blob_bytes']):>14,d}B in contents;
- {float(d0.loc['node_bytes'] / max(1, d0.loc['bytes'])):4.0%} {int(d0.loc['node_bytes']):>14,d}B in nodes;
- {float(d0.loc['inner_bytes'] / max(1, d0.loc['bytes'])):4.0%} {int(d0.loc['inner_bytes']):>14,d}B in hidden nodes;
- {float(d0.loc['commit_bytes'] / max(1, d0.loc['bytes'])):4.0%} {int(d0.loc['commit_bytes']):>14,d}B in commits.

Second breakdown:
- {float(d0.loc['header_bytes'] / max(1, d0.loc['bytes'])):4.0%} {int(d0.loc['header_bytes']):>14,d}B used by the 32 byte hash that prefixes all objects;
- {float(d0.loc['other_bytes'] / max(1, d0.loc['bytes'])):4.0%} {int(d0.loc['other_bytes']):>14,d}B elsewhere.
```

💡 The hidden nodes are responsible for 70% of the size of the pack file.

💡 `139,431` commits are accounted above, but when looking at the block levels we would expect `139,265` (`2,056,194 - 1,916,930 + 1`). This difference is due to orphan blocks.

## Pack File Areas

This document focuses on 18 commits, which are the 2nd of the Tezos cycles 428 to 445. "Commit 428" is the first commit of the pack file (ignoring the genesis commit) and "commit 445" is the last commit analysed in this document.

This document considers 19 areas delimited by the 18 commits. An area X contains the data that was pushed to the pack file during the cycle X.

""")

l = []
for i, row in df1.iterrows():
    x = 'area first_level blob_count node_count inner_count commit_count count bytes'.split(' ')
    x = [row[x] for x in x]
    x = [
        (f'{x:,d}' if isinstance(x, int) else str(x))
        for x in x
    ]
    x = ' | '.join(x)
    x = f'| {i} | {x} | `[{row.start_offset:,d}; {row.end_offset:,d}[` |'
    l.append(x)
newline = '\n'
markdown(f"""\
|idx|area|first commit level|blob count|node count|hidden node count|commit count|object count|bytes|offset interval|
|-|-|-|-|-|-|-|-|-|-|
{newline.join(l)}

💡 The first area starts at the beginning of the pack file and ends right before commit 428.

💡 The second area's first object is commit 428. This area ends right before commit 429.

💡 The first area contains the genesis commit. The last area only contains commit 445. All other areas have at least 8192 commits, which is the number of block level in a cycle, the other commits being orphan ones.

💡 The first area is solely made of objects from the snapshot import.

💡 Aside for ~7 objects that belong to the genesis commit, the objects of the first area are the objects of the tree of commit 428.


#### Areas Evolution
It can be seen in the above table that the areas get bigger over time. This is because the Tezos blockchain is growing. Recent cycles tend to host more transactions than the older ones.
""")

code(f"""custom_plot_tools.plot_area_curve_object_count('/tmp/summary_df1.csv', 'area', "Evolution of Areas' Object Count", xbounds=(428, 444))""")

markdown(f"""\
💡 Cycles 430, 434, 437, 442 and 444 are significantly below the average curve. They correspond to week-ends and holidays!
""")

code(f"""custom_plot_tools.plot_area_curve_byte_count('/tmp/summary_df1.csv', 'area', "Evolution of Areas' Disk Footprint", xbounds=(428, 444))""")

markdown(f"""\
💡 Each area weigh around 3GB, which corresponds the growth of the pack file at every cycle (every 3 days).

💡 Every cycle the pack file write rate increases by ~1% (35MB over 3000MB).

💡 Cycle 428 grew the pack file by ~2.9GB and cycle 443 by ~3.5GB. Most of this acceleration is due to the hidden nodes.
""")

del df, dtmp, df1, rows

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

discriminators = 'area_distance_from_origin path path2 path3 kind node_length contents_size'.split(' ')
indicators = 'count node_count inner_count blob_count step_count indirect_count direct_count bytes direct_bytes header_bytes other_bytes node_bytes inner_bytes blob_bytes'.split(' ')

df = pd.read_csv('csv/entries.csv')

# Fix the broken paths
df['path'] = df.path.fillna('/')

# Add the derived discriminators
df['ekind'] = df.apply(lambda row: row.contents_size if row.contents_size != "Na" else row.node_length, axis=1)

# Add the derived indicators
df['header_bytes'] = df['count'] * 32
df['other_bytes'] = df.bytes - df.header_bytes - df.direct_bytes
df['node_count'] = df.apply(lambda row: row['count'] if '_root_' in row.kind else 0, axis=1)
df['inner_count'] = df.apply(lambda row: row['count'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_count'] = df.apply(lambda row: row['count'] if 'Contents' == row.kind else 0, axis=1)
df['step_count'] = df['direct_count'] + df['indirect_count']
df['node_bytes'] = df.apply(lambda row: row['bytes'] if '_root_' in row.kind else 0, axis=1)
df['inner_bytes'] = df.apply(lambda row: row['bytes'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_bytes'] = df.apply(lambda row: row['bytes'] if 'Contents' == row.kind else 0, axis=1)

# Switch indicators to float
for c in indicators:
    df[c] = df[c].astype(float)

# Switch from 'entry_area' to 'area_distance_from_origin'
df['area_distance_from_origin'] = (df.parent_cycle_start - df.entry_area).clip(0, 5)
df = df.drop(columns=['entry_area'])

df1 = df.groupby('parent_cycle_start')[indicators].sum().reset_index()
df1.to_csv(f'/tmp/all_trees_df1.csv', index=False)

# ******************************************************************************
markdown(f"""\
## Tree of Commits

The previous section focused on the everything that was pushed to the pack file during cycles. This new section focuses on how the trees evolve. The pack file getting big is a short term problem for rolling Tezos nodes, but the commit tree getting big is a long term problem. Fortunately, the commit tree grows much slower than the pack file.
""")

code(f"""custom_plot_tools.plot_area_curve_object_count('/tmp/all_trees_df1.csv', 'parent_cycle_start', "Evolution of Commit Trees' Object Count")""")

markdown(f"""\
💡 While the rate at which the pack file areas grows is unstable, the rate at which the commit trees grows is very stable.

💡 The previous section shows that ~6 million hidden nodes are pushed per cycle. The above plot shows that the commit tree gains 37K hidden nodes every cycle. This means that ~0.5% of the hidden nodes stick around in the tree durably.

💡 The previous section shows that ~3 million blobs are pushed per cycle. The above plot shows that the commit tree gains 141K blobs every cycle. This means that ~5% of the blobs stick around in the tree durably.
""")

code(f"""custom_plot_tools.plot_area_curve_byte_count('/tmp/all_trees_df1.csv', 'parent_cycle_start', "Evolution of Commit Trees' Disk Footprint")""")
markdown(f"""\
💡 The commit tree weighs ~2.5GB at the end of Jan 2022. The commit tree growth rate is 39MB per cycle, which extrapolates to 5GB per year.

💡 The commit tree grows 39MB per cycle while the pack file grows 3GB per cycle. These numbers don't reflect the fact that a cycle replaces objects from previous cycles. The data in __The Tree of Commit 445__ shows that commit 445 references 267MB of data from cycle 444 and 154MB from cycle 443.
""")

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

markdown(f"""\
## Areas and Trees Cross-Analysis

[📄areas_and_trees.ipynb](./areas_and_trees.ipynb) details where the data of each tree is located in the pack file.
""")


markdown(f"""\

## The Tree of Commit 445

The stats in this section focus on the tree at the beginning of cycle 445. Many more objects were added during cycle 444 and all the previous cycles, but only a fraction is still referenced by the tree at the beginning of cycle 445.

[📄tree_of_cycle_445.ipynb](./tree_of_cycle_445.ipynb) details the contents of the tree.

These notebooks zoom on the heaviest paths of the tree:
- `/data/contracts/index`, the largest node in the tree [📄url](./tree_of_cycle_445_contracts-index.ipynb);
- `/data/contracts/index/*`, the children of the largest node [📄url](./tree_of_cycle_445_contracts-index-star.ipynb);
- `/data/contracts/index/*/manager`, a contents present in almost all contracts [📄url](./tree_of_cycle_445_contracts-index-star-manager.ipynb);
- `/data/big_maps/index/*/contents` is where 43% of the bytes of the tree are located [📄url](./tree_of_cycle_445_big_maps-index-star-contents.ipynb);
- `/data/big_maps/index/*/contents/*` are 5 million small nodes [📄url](./tree_of_cycle_445_big_maps-index-star-contents-star.ipynb);
- `/data/big_maps/index/*/contents/*/data` are contents that take up a lot of space [📄url](./tree_of_cycle_445_big_maps-index-star-contents-star-data.ipynb).

""")


markdown(f"""\
## Conclusion

There is room for improvements with the on-disk encoding of irmin-pack and the layered store garbage collection will solve the pack file growth. However, the extrapolated commit tree growth is massive: x3 over a year. At best we can lower the on-disk size by constant factors. The layered store is helpless regarding growths in the size of the tree. Tripling the size of the tree implies tripling the size of the "sparse pack file" of the layered store. This might also mean tripling the length of the freezes.

Nothing suggests that Irmin trees, irmin-pack and the layered store will not support that projected load, but we need to think about the implications of this and proactively ensure that things will be OK.

It is unexpected to see that so much bytes are occupied by "direct" steps (40% in the commit tree). The way the dict behaves is obviously sub-optimal (i.e. when full it stops ingesting new data). 3 ideas for improvements:
- Manually push the ~150 recurring steps (i.e. the english words like "delegated") to the dict and see if it has a positive impact. This might improve the situation for `/data/contracts/index/*`.
- Introduce a "step_key", analoguous to the structured keys, which would allow to have steps not just being "indirect" and "direct" but also offsets that point to "direct" steps earlier in the pack file.
- Some steps are known to be human-readable ascii integers (i.e. 35 bytes in `/data/contracts/index` and 65 bytes in `/data/big_maps/index/*/contents`). Tezos could migrate the tree to use a binary form for these steps, or a feature could be added to Irmin that would allow that optimisation at serialisation time.

""")



nb['cells'] = cells
with open('index.ipynb', 'w') as f:
    nbf.write(nb, f)
print('Made index.ipynb')
