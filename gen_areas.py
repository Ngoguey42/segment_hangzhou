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
# Analysis of the {len(set(df.area))} Areas

The areas get bigger over time. This is directly because the Tezos blockchain is growing. A cycle tend to host more transactions than the previous one.
""")

code(f"""\
%matplotlib inline
%load_ext autoreload
%autoreload 2
import custom_plot_tools""")

markdown(f"""\
### Areas Evolution
""")

code(f"""\
custom_plot_tools.plot_area_curve_object_count('/tmp/summary_df1.csv')""")

markdown(f"""\
""")

code(f"""custom_plot_tools.plot_area_curve_byte_count('/tmp/summary_df1.csv')""")

markdown(f"""\
💡 Cycle 428 grew the pack file by ~2.9GB and cycle 443 by ~3.5GB. Most of this acceleration is due to the hidden nodes.
""")



nb['cells'] = cells
with open('areas.ipynb', 'w') as f:
    nbf.write(nb, f)
print('Made areas.ipynb')
