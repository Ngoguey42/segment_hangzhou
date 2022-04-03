import pandas as pd
import nbformat as nbf
import numpy as np

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

df = pd.read_csv('csv/areas.csv')
df['bytes'] = df['byte_count']
df['count'] = df['entry_count']

del df['byte_count']
del df['entry_count']

# Artificially patching [df] to add the last commit. It simplifies explanation
# df = df.append(dict(area=445, kind="Commit", contents_size="Na", count=1, bytes=110), ignore_index=True)

df['ekind'] =  df.apply(lambda row: row.contents_size if row.contents_size != "Na" else row.kind, axis=1)
df['node_count'] = df.apply(lambda row: row['count'] if '_root_' in row.kind else 0, axis=1)
df['inner_count'] = df.apply(lambda row: row['count'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_count'] = df.apply(lambda row: row['count'] if 'Contents' == row.kind else 0, axis=1)

df['node_bytes'] = df.apply(lambda row: row['bytes'] if '_root_' in row.kind else 0, axis=1)
df['inner_bytes'] = df.apply(lambda row: row['bytes'] if '_nonroot_' in row.kind else 0, axis=1)
df['blob_bytes'] = df.apply(lambda row: row['bytes'] if 'Contents' == row.kind else 0, axis=1)

df['header_bytes'] = df['count'] * 32
df['other_bytes'] = df.bytes - df.header_bytes

indicators = 'count node_count inner_count blob_count bytes node_bytes inner_bytes blob_bytes header_bytes other_bytes'.split(' ')

df = df.groupby('area')[indicators].sum().reset_index()

df['pages'] = np.ceil((df.bytes / 4096)).astype(int)
df1 = df

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

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

df['area'] = df.entry_area
del df['entry_area']
df = df.groupby(['parent_cycle_start', 'area'])[indicators].sum().reset_index()

# Add memory layout
d = pd.read_csv('csv/memory_layout.csv')
d['area'] = d.entry_area
d['pages'] = d.pages_touched
del d['entry_area']
del d['pages_touched']
del d['algo_chunk_count']

df = d.merge(df, 'inner', ['parent_cycle_start', 'area'])
df = df.sort_values(['parent_cycle_start', 'area'])

df2 = df

# ******************************************************************************
# ******************************************************************************
# ******************************************************************************

print(df1)
print(df2)
df1.to_csv(f'/tmp/areas.csv', index=False)
df2.to_csv(f'/tmp/trees_in_areas.csv', index=False)

nb = nbf.v4.new_notebook()
cells = []
markdown = lambda cell: cells.append(nbf.v4.new_markdown_cell((cell)))
code = lambda cell: cells.append(nbf.v4.new_code_cell((cell)))

markdown(f"""\
# Analysis of Tree Locations Within the Pack File
""")

code(f"""\
%matplotlib inline
%load_ext autoreload
%autoreload 2
import custom_plot_tools""")

# ********************************************************************
markdown(f"""\
### Objects used by each commit in each area

The first line informs on the number of objects in each area. The following lines inform on the percentage of these objects that is used by the commits.
""")
code(f"""\
custom_plot_tools.plot_areas_and_trees('/tmp/areas.csv', '/tmp/trees_in_areas.csv', 'count')""")
markdown(f"""\
💡 All the numbers in the first column are greater or equal to 85%. This indicates that most of the objects of the snapshot stay referenced by new commits for a long time.

💡 All the number in the other columns are below 10%. This indicates that most of the new objects are short-lived garbage for a user that doesn't care about history.

💡 Commit 428 (the first commit) always uses everything in area 427 (the snapshot import area).

💡 Commit 429 (which resembles to a typical freeze commit) uses 95% of the objects of area 427 and 8% of area 428.
""")

# ********************************************************************
markdown(f"""\
### Inodes used by each commit in each area
""")
code(f"""\
custom_plot_tools.plot_areas_and_trees('/tmp/areas.csv', '/tmp/trees_in_areas.csv', 'inner_count')""")
markdown(f"""\
💡 All the percentages here are significantly lower than in the previous plot. This indicates that there is a larger turn-over with inodes than with other object kind.
""")

# ********************************************************************
markdown(f"""\
### Bytes used by each commit in each area
""")
code(f"""\
custom_plot_tools.plot_areas_and_trees('/tmp/areas.csv', '/tmp/trees_in_areas.csv', 'bytes')""")
markdown(f"""\
💡 Commit 429 uses 90% of the bytes of area 427 and 8% of area 428.
""")

# ********************************************************************
markdown(f"""\
### Pages used by each commit in each area
""")
code(f"""\
custom_plot_tools.plot_areas_and_trees('/tmp/areas.csv', '/tmp/trees_in_areas.csv', 'pages')""")
markdown(f"""\
💡 Commit 429 spans on 99% of the pages of area 427 and 35% of the pages of area 429.
""")

# ********************************************************************

# d = df2.pivot('parent_cycle_start', 'area', 'chunk_count')
bytes = df2.pivot('parent_cycle_start', 'area', 'bytes')
chunks = df2.pivot('parent_cycle_start', 'area', 'chunk_count')
bytes.index.name = 'commit'
chunks.index.name = 'commit'

chunks_1d = chunks.sum(axis=1).astype(int).apply(lambda x: x if x < 1000 else f'{x / 1e6:.1f}M')
bpc_2d = (bytes / chunks).applymap(lambda x: "" if not np.isfinite(x) else f'{x:.0f}' if x < 1e6 else f'{x / 1000000:.0f}M')
chunks_2d = chunks.fillna(0).astype(int).applymap(lambda x: x if x < 1000 else f'{x / 1000:.0f}k')

markdown(f"""\
### Chunks of Consecutive Bytes of Commit Trees

The tree of commit 428 is fully contained in area 427, which (almost) contains nothing else. This makes the file to file copy of commit 428 lightning fast.

However, all the other commit trees suffer fragmentation.

The following table shows the number of chunks for each commit.

```
{chunks_1d}
```

💡 This list can be seen as the number of offset intervals that a "sparse pack file" would be made of. It grows freeze after freeze, as the fragmentation of offset increases.

💡 Commit 428 might be split in two because of the genesis commit.

<br/>

The following table indicates the number of chunks that each commit tree span on in each area. This corresponds to the number of small copies that must be performed to fully copy a tree form file to file.

```
{chunks_2d}
```

💡 The column for area 427 grows downward, indicating the increase in fragmentation as commits get older. The other columns shrink downward, which is a bit strange.

<br/>

That last table shows the average length in bytes of each of these chunks.

```
{bpc_2d}
```

""")



nb['cells'] = cells
with open('areas_and_trees.ipynb', 'w') as f:
    nbf.write(nb, f)
print('Made areas_and_trees.ipynb')
