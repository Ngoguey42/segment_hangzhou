
import pandas as pd
import nbformat as nbf

from path_patterns import path_pats

pd.set_option('display.max_rows', 120)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


def on_averaged_tree(df, fname, block_desc, block_subdesc, filter_name=None):
    nb = nbf.v4.new_notebook()
    cells = []

    # **************************************************************************
    if filter_name is None:
        a = ""
    else:
        a = f'##### {filter_name}'
    print(filter_name, a)
    text = f"""\
# Analysis of the Irmin tree of {block_desc}
### which is {block_subdesc}

{a}
"""
    cells.append(nbf.v4.new_markdown_cell(text))

    # **************************************************************************
    d = df[indicators].sum()
    if filter_name is None:
        a = " (i.e. number of directories)"
        b = "Side note: The difference between `directories` and `objects` is due to sharing of objects, i.e. may objects are referenced by several paths."
    else:
        a = " (i.e. number of children in the nodes)"
        b = ""

    text = f"""\
##### Summary
```
Number of bytes: {int(d.loc['bytes']):,d}. Breakdown:
  - {float(d.loc['header_bytes'] / d.loc['bytes']):4.0%} {int(d.loc['header_bytes']):>11,d}B in 32 byte hash of objects,
  - {float(d.loc['direct_bytes'] / d.loc['bytes']):4.0%} {int(d.loc['direct_bytes']):>11,d}B in hard coded steps (a.k.a. direct step),
  - {float(d.loc['other_bytes'] / d.loc['bytes']):4.0%} {int(d.loc['other_bytes']):>11,d}B elsewhere (i.e. length segment + value segment).

Number of objects: {int(d.loc['count']):,d} (a.k.a. pack file entries). Breakdown:
  - {float(d.loc['blob_count'] / d.loc['count']):4.0%} {int(d.loc['blob_count']):>10,d} contents (a.k.a. blobs),
  - {float(d.loc['node_count'] / d.loc['count']):4.0%} {int(d.loc['node_count']):>10,d} nodes (a.k.a. root inodes),
  - {float(d.loc['inner_count'] / d.loc['count']):4.0%} {int(d.loc['inner_count']):>10,d} hidden nodes (a.k.a. non-root inodes).

Number of steps: {int(d.loc['step_count']):,d}{a}. Breakdown:
  - {float(d.loc['direct_count'] / d.loc['step_count']):4.0%} {int(d.loc['direct_count']):>10,d} by direct references (i.e. parent records hard coded step),
  - {float(d.loc['indirect_count'] / d.loc['step_count']):4.0%} {int(d.loc['indirect_count']):>10,d} by indirect references (i.e. parent records "dict" id),
```
{b}

"""
    print(text)
    cells.append(nbf.v4.new_markdown_cell(text))

    # **************************************************************************



    # **************************************************************************
    nb['cells'] = cells
    with open(fname, 'w') as f:
        nbf.write(nb, f)

discriminators = 'area_distance_from_origin path path2 path3 kind node_length contents_size'.split(' ')
indicators = 'count node_count inner_count blob_count step_count indirect_count direct_count bytes direct_bytes header_bytes other_bytes'.split(' ')

df = pd.read_csv('csv/entries.csv')

# Fix the broken paths
df['path'] = df.path.fillna('/')

# Add the derived discriminators
df['path2'] = df.path.apply(lambda x: path_pats[x][0])
df['path3'] = df.path.apply(lambda x: path_pats[x][1])

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

# on_averaged_tree(
#     df.set_index('parent_cycle_start').loc[445],
#     block_desc="block level 2_002_944 which is the second block of cycle 438",
#     fname='average_tree.ipynb',
# )
d = df.set_index('parent_cycle_start').loc[445].query('path3 == "/data/contracts/index/*"')
on_averaged_tree(
    d,
    block_desc="block level 2_002_944",
    block_subdesc="the second block of cycle 438 (created on Jan 4, 2022)",
    fname='average_tree.ipynb',
    filter_name='zoom on these entries: "/data/contracts/index/*"',
)



#
