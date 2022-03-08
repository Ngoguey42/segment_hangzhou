
import pandas as pd
import nbformat as nbf

from path_patterns import path_pats

pd.set_option('display.max_rows', 120)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


def on_averaged_tree(df, fname, block_desc, block_subdesc,
                     filter_name=None,
                     summary_desc=None,
                     kind_desc=None, distance_desc=None, path2_desc=None, path3_desc=None):
    print('Generating', fname)
    nb = nbf.v4.new_notebook()
    cells = []
    df.to_csv(f'/tmp/{fname}.csv', index=False)

    # **************************************************************************
    if filter_name is None:
        a = ""
    else:
        a = f'##### {filter_name}'
    cell = f"""\
# Analysis of the Irmin tree of {block_desc}
### which is {block_subdesc}

{a}
"""
    cells.append(nbf.v4.new_markdown_cell(cell))

    # **************************************************************************
    cell = f"""\
%matplotlib inline
%load_ext autoreload
%autoreload 2
from custom_plot_tools import plot_4_vertical_bubble_histo, plot_grid_bubble_histo"""
    cells.append(nbf.v4.new_code_cell(cell))

    # **************************************************************************
    d = df[indicators].sum()
    if filter_name is None:
        a = " (i.e. number of directories)"
        b = "Side note: The difference between `directories` and `objects` is due to sharing of objects, i.e. may objects are referenced by several paths."
    else:
        a = " (i.e. number of children in the nodes)"
        b = ""

    cell = f"""\
### Summary
```
Number of bytes: {int(d.loc['bytes']):,d}. Breakdown:
  - {float(d.loc['header_bytes'] / max(1, d.loc['bytes'])):4.0%} {int(d.loc['header_bytes']):>11,d}B in 32 byte hash of objects,
  - {float(d.loc['direct_bytes'] / max(1, d.loc['bytes'])):4.0%} {int(d.loc['direct_bytes']):>11,d}B in hard coded steps (a.k.a. direct step),
  - {float(d.loc['other_bytes'] / max(1, d.loc['bytes'])):4.0%} {int(d.loc['other_bytes']):>11,d}B elsewhere (i.e. length segment + value segment).

Number of objects: {int(d.loc['count']):,d} (a.k.a. pack file entries). Breakdown:
  - {float(d.loc['blob_count'] / max(1, d.loc['count'])):4.0%} {int(d.loc['blob_count']):>10,d} contents (a.k.a. blobs),
  - {float(d.loc['node_count'] / max(1, d.loc['count'])):4.0%} {int(d.loc['node_count']):>10,d} nodes (a.k.a. root inodes, directory),
  - {float(d.loc['inner_count'] / max(1, d.loc['count'])):4.0%} {int(d.loc['inner_count']):>10,d} hidden nodes (a.k.a. non-root inodes).

Number of steps: {int(d.loc['step_count']):,d}{a}. Breakdown:
  - {float(d.loc['direct_count'] / max(1, d.loc['step_count'])):4.0%} {int(d.loc['direct_count']):>10,d} by direct references (i.e. parent records hard coded step),
  - {float(d.loc['indirect_count'] / max(1, d.loc['step_count'])):4.0%} {int(d.loc['indirect_count']):>10,d} by indirect references (i.e. parent records "dict" id),
```
{b}

"""
    cells.append(nbf.v4.new_markdown_cell(cell))

    # **************************************************************************
    if summary_desc is not None:
        cell = summary_desc
        cells.append(nbf.v4.new_markdown_cell(cell))


    # **************************************************************************
    # **************************************************************************
    cell = f"""\
### Objects Kind

The following plot groups the objects into 10 categories:
- 4 categories for contents, depending on their size,
- 5 categories for inodes, depending on the size of the node they belong to,
- 1 extra category for inodes that appear in several categories at once.
"""
    cells.append(nbf.v4.new_markdown_cell(cell))

    # **************************************************************************
    cell = f"""\
plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                           'ekind')"""
    cells.append(nbf.v4.new_code_cell(cell))


    # **************************************************************************
    if kind_desc is not None:
        cell = kind_desc
        cells.append(nbf.v4.new_markdown_cell(cell))


    # **************************************************************************
    # **************************************************************************
    cell = f"""\
### Objects Distance to Commit

The following plot groups the objects into 5 categories, depending on their distance to the commit of the tree.
For instance, `<1 cycle` implies that the objects in that row are less than 1 cycle away from the commit being analysed (i.e. less than 8200 blocks away, less than 3 days away).

"""
    cells.append(nbf.v4.new_markdown_cell(cell))

    # **************************************************************************
    cell = f"""\
plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                           'area_distance_from_origin')"""
    cells.append(nbf.v4.new_code_cell(cell))


    # **************************************************************************
    if distance_desc is not None:
        cell = distance_desc
        cells.append(nbf.v4.new_markdown_cell(cell))

    if filter_name is None:
        # **************************************************************************
        # **************************************************************************
        cell = f"""\
### Objects Path

The following plot groups the objects into 4 categories, depending on their ancestor directory.
    """
        cells.append(nbf.v4.new_markdown_cell(cell))

        # **************************************************************************
        cell = f"""\
plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                           'path2')"""
        cells.append(nbf.v4.new_code_cell(cell))

        # **************************************************************************
        if path2_desc is not None:
            cell = path2_desc
            cells.append(nbf.v4.new_markdown_cell(cell))

        # **************************************************************************
        # **************************************************************************
        cell = f"""\
<br/>

The following plot groups the objects on 8 interesting locations.
"""
        cells.append(nbf.v4.new_markdown_cell(cell))

        cell = f"""\
plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                           'path3')"""
        cells.append(nbf.v4.new_code_cell(cell))

        if path3_desc is not None:
            cell = path3_desc
            cells.append(nbf.v4.new_markdown_cell(cell))

        # **************************************************************************
        # **************************************************************************
        cell = f"""\
<br/>

The following plot groups the objects on their precise location.
"""
        cells.append(nbf.v4.new_markdown_cell(cell))


        cells.append(nbf.v4.new_code_cell(f"""\
plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                             'path')"""))

        # **************************************************************************
        # if path3_desc is not None:
            # cell = path3_desc
            # cells.append(nbf.v4.new_markdown_cell(cell))


    # **************************************************************************
    # **************************************************************************
    cells.append(nbf.v4.new_markdown_cell(f"""\
### toto

toto
"""))
    cells.append(nbf.v4.new_code_cell(f"""\
plot_grid_bubble_histo('/tmp/{fname}.csv',
                       'bytes', 'area_distance_from_origin', 'ekind')"""))
    cells.append(nbf.v4.new_code_cell(f"""\
plot_grid_bubble_histo('/tmp/{fname}.csv',
                       'count', 'area_distance_from_origin', 'ekind')"""))

    # **************************************************************************
    # **************************************************************************
    cells.append(nbf.v4.new_markdown_cell(f"""\
### toto

toto
"""))
    cells.append(nbf.v4.new_code_cell(f"""\
plot_grid_bubble_histo('/tmp/{fname}.csv',
                       'bytes', 'area_distance_from_origin', 'path3')"""))
    cells.append(nbf.v4.new_code_cell(f"""\
plot_grid_bubble_histo('/tmp/{fname}.csv',
                       'count', 'area_distance_from_origin', 'path3')"""))

    # **************************************************************************
    # **************************************************************************
    cells.append(nbf.v4.new_markdown_cell(f"""\
### toto

toto
"""))
    cells.append(nbf.v4.new_code_cell(f"""\
plot_grid_bubble_histo('/tmp/{fname}.csv',
                       'bytes', 'ekind', 'path3')"""))
    cells.append(nbf.v4.new_code_cell(f"""\
plot_grid_bubble_histo('/tmp/{fname}.csv',
                       'count', 'ekind', 'path3')"""))


    # **************************************************************************
    # **************************************************************************
    nb['cells'] = cells
    with open(fname, 'w') as f:
        nbf.write(nb, f)

discriminators = 'area_distance_from_origin path path2 path3 kind node_length contents_size'.split(' ')
indicators = 'count node_count inner_count blob_count step_count indirect_count direct_count bytes direct_bytes header_bytes other_bytes'.split(' ')

df = pd.read_csv('csv/entries.csv')

df1 = df.copy()

# Fix the broken paths
df['path'] = df.path.fillna('/')

# Add the derived discriminators
df['path2'] = df.path.apply(lambda x: path_pats[x][0])
df['path3'] = df.path.apply(lambda x: path_pats[x][1])
df['ekind'] = df.apply(lambda row: row.contents_size if row.contents_size != "Na" else row.node_length, axis=1)

# Add the derived indicators
df['header_bytes'] = df['count'] * 32
df['other_bytes'] = df.bytes - df.header_bytes - df.direct_bytes
df['node_count'] = df.apply(lambda row: row['count'] if '_root_' in row.kind else 0, axis=1)
df['inner_count'] = df.apply(lambda row: row['count'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_count'] = df.apply(lambda row: row['count'] if 'Contents' == row.kind else 0, axis=1)
df['step_count'] = df['direct_count'] + df['indirect_count']
# Drop the first ~5 cycle stats as they are very close to the snapshot
df = df[df.parent_cycle_start >= 434]

# Switch indicators to float
for c in indicators:
    df[c] = df[c].astype(float)

# Switch from 'entry_area' to 'area_distance_from_origin'
df['area_distance_from_origin'] = (df.parent_cycle_start - df.entry_area).clip(0, 5)
df = df.drop(columns=['entry_area'])

# Average together all the remaning cycle_starts
# df = df[df.parent_cycle_start == 442]
# tree_count = len(set(df.parent_cycle_start))
# df = df.groupby(discriminators)[indicators].sum() / tree_count
# df = df.reset_index()

d = df.set_index('parent_cycle_start').loc[445].reset_index(drop=True)
on_averaged_tree(
    d,
    block_desc="block level 2,056,194",
    block_subdesc="the second block of cycle 445 (created on Jan 23, 2022)",
    fname='tree_of_cycle_445.ipynb',
    kind_desc = f"""\
💡 94 nodes have a length greater than 16k, they make up 46% of the bytes of the tree.

💡 Almost all nodes are small (i.e. with a length of 32 or less)
""",
    distance_desc = f"""\
💡 A single cycle doesn't modifies a lot the Irmin tree.
""",
    path2_desc= f"""\
💡 Almost all the data is contained in `big_maps` and `contracts`.
""",
    path3_desc= f"""\
💡 The `/data/contracts/index` directory is made of nearly 1 million inodes. It points to nearly 2 million nodes (i.e. `/data/contracts/index/*`). (Technically it points to `2,003,307` nodes but only `1,988,785` objects due to sharing)

These paths are also individually analysed in separate files.

""",
)

d = df.set_index('parent_cycle_start').loc[445].query('path3 == "/data/contracts/index"').reset_index(drop=True)
on_averaged_tree(
    d,
    block_desc="block level 2,056,194",
    block_subdesc="the second block of cycle 445 (created on Jan 23, 2022)",
    fname='tree_of_cycle_445_contracts-index.ipynb',
    filter_name='zoom on this node: `/data/contracts/index`',
)

descs = {
    '/data/contracts/index/*/manager': dict(
#         distance_desc = f"""\
# 💡 Very few new "manager" contents enter the pack file on the last cycles. However, many new contracts enter the pack file on these cycles (see `/data/contracts/index/*` [(url)](./tree_of_cycle_445_contracts-index-star.ipynb)), it implies that the new contracts share the "manager" files with the older contracts.
# """,
    ),
    '/data/contracts/index': dict(
        distance_desc = f"""\
💡 This directory is modified all the time (most likely at every block). A new root inode has to be re-commited every time it is modified, this is why the bubble for "node count" shows on the first row.
""",
    ),
    '/data/contracts/index/*': dict(
        distance_desc = f"""\
💡 124.4k contracts were modified (or added) during the last cycle.

💡 14.5k contracts were modified (or added) 2 cycles ago and were not modified since.
""",
    ),
    '/data/big_maps/index/*/contents': dict(
    kind_desc = f"""\
💡 The 62 very large nodes weigh almost 1GB, while the full tree weighs 2.5GB.
""",
    summary_desc = f"""\
💡 The steps all have length 65 here. They represent 818MB out of the 2.5GB that the full tree weighs.
""",

    )
}

for p in [
        '/data/big_maps/index/*/contents',
        '/data/big_maps/index/*/contents/*',
        '/data/big_maps/index/*/contents/*/data',
        '/data/contracts/index',
        '/data/contracts/index/*',
        '/data/contracts/index/*/manager',
]:
    q = p.replace('/data/', '').replace('/', '-').replace('*', 'star')
    d = df.set_index('parent_cycle_start').loc[445].query(f'path3 == "{p}"').reset_index(drop=True)
    on_averaged_tree(
        d,
        block_desc="block level 2,056,194",
        block_subdesc="the second block of cycle 445 (created on Jan 23, 2022)",
        fname=f'tree_of_cycle_445_{q}.ipynb',
        filter_name=f'zoom on this node: `{p}`',
        **(descs.get(p, {})),
    )



#
