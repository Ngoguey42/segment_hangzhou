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
df.to_csv('/tmp/summary_df1.csv', index=False)
print(df1)



markdown(f"""\
# Hanghzou Irmin Pack File Analysis

This document presents statistics computed on a pack file which results from a Tezos bootstrapping on the first 2 month of the Hanghzou protocol.

The goal of this document is to gather insights for future improvements of irmin-pack, notably for the layered store design.

This work is splited on several documents.

The light bulbs 💡 accompanying the data are insights on what can be seen.

The raw data used for this document can be found at `csv/*csv` in the same repository.

These `.ipynb` files are generated with the `gen_*.py` scripts in the same repository.


### What's the _Tree of a Commit_

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


### Contents of the Pack File

This bootstrap was realised using Irmin 3.0 and its new structured keys, using the _minimal_ indexing strategy, which results in a bit less sharing in the pack file.

Ignoring the genesis commit which is present in all Tezos pack files, the first commit of the pack file belongs to block 1,916,930 (2nd of Hangzhou, 2nd of cycle 428, 4 Dec 2021, https://tzstats.com/1916930). This commit was created by a Tezos snapshot import.

The last commit analysed belongs to block 2,056,194 (2nd of cycle 445, 23 Jan 2022, https://tzstats.com/2056194). This commit is analysed in depth in the __Data Distribution in the Tree of Commit 445__ section.

##### Summary
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

💡 `139,431` commits are accounted above, but when looking at the block levels we would expect `139,265` (`2,056,194 - 1,916,930 + 1`). This difference is due to orphan blocks.

##### Summary per pack file Area

This document focuses on 18 commits, which are the 2nd of the Tezos cycles 428 to 445. "Commit 428" is the first commit of the pack file (ignoring the genesis commit) and "commit 445" is the last commit analysed in this document.

This document considers 19 areas delimited by the 18 commits:
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

💡 The second area's first object is commit 428. It ends right before commit 429.

💡 The first area contains the genesis commit. The last area only contains commit 445. All other areas have at least 8192 commits, which is the number of block level in a cycle, the other commits being orphan ones.

💡 The first area is solely made of objects from the snapshot import.

💡 Aside for 4 objects that belong to the genesis commit, the objects of the first area are the objects of the tree of commit 428.

💡 Commit 429 is preceded by ~8200 commits, which makes it a typical _freeze commit_ for the layered store

[📄areas.ipynb](./areas.ipynb) details the contents of these areas.

""")

markdown(f"""\
### Data Distribution in the Tree of Commit 445

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

nb['cells'] = cells
with open('index.ipynb', 'w') as f:
    nbf.write(nb, f)
print('Made index.ipynb')
