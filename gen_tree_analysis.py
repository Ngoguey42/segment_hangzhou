import pandas as pd
import nbformat as nbf

from path_patterns import path_pats

pd.set_option('display.max_rows', 120)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

def trimleft(txt):
    lines = txt.split('\n')
    indentation = min(
        len(l) - len(l.lstrip(' '))
        for l in lines
        if l != ''
    )
    lines = [
        l[indentation:]
        for l in lines
    ]
    return '\n'.join(lines)

def on_averaged_tree(df, fname, block_desc, block_subdesc, path_zoom=None):
    print('Generating', fname)
    nb = nbf.v4.new_notebook()
    cells = []
    df.to_csv(f'/tmp/{fname}.csv', index=False)

    markdown = lambda cell: cells.append(nbf.v4.new_markdown_cell(trimleft(cell)))
    code = lambda cell: cells.append(nbf.v4.new_code_cell(trimleft(cell)))

    # **************************************************************************
    # **************************************************************************
    if path_zoom is None:
        a = ""
    elif '*' not in path_zoom:
        a = f'##### zoom on this node: `{path_zoom}`'
    else:
        a = f'##### zoom on this set of nodes: `{path_zoom}`'
    markdown(f"""\
    # Analysis of the tree of the commit belonging to {block_desc}
    ### which is {block_subdesc}

    {a}
    """)
    code(f"""\
    %matplotlib inline
    %load_ext autoreload
    %autoreload 2
    from custom_plot_tools import plot_4_vertical_bubble_histo, plot_grid_bubble_histo""")

    # **************************************************************************
    # **************************************************************************
    d = df[indicators].sum()
    markdown(f"""\
    ### Summary
    ```
    Number of objects: {int(d.loc['count']):,d} (a.k.a. pack file entries). Breakdown:
    - {float(d.loc['blob_count'] / max(1, d.loc['count'])):4.0%} {int(d.loc['blob_count']):>10,d} contents (a.k.a. blobs);
    - {float(d.loc['node_count'] / max(1, d.loc['count'])):4.0%} {int(d.loc['node_count']):>10,d} nodes (a.k.a. root inodes, directories);
    - {float(d.loc['inner_count'] / max(1, d.loc['count'])):4.0%} {int(d.loc['inner_count']):>10,d} hidden nodes (a.k.a. non-root inodes).

    Number of bytes: {int(d.loc['bytes']):,d}. Breakdown:
    - {float(d.loc['header_bytes'] / max(1, d.loc['bytes'])):4.0%} {int(d.loc['header_bytes']):>11,d}B used by the 32 byte hash that prefixes all objects;
    - {float(d.loc['direct_bytes'] / max(1, d.loc['bytes'])):4.0%} {int(d.loc['direct_bytes']):>11,d}B used in hard coded steps (a.k.a. direct step) (a step is a file name or a directory name, it is a step in a path);
    - {float(d.loc['other_bytes'] / max(1, d.loc['bytes'])):4.0%} {int(d.loc['other_bytes']):>11,d}B elsewhere (i.e. length segment + value segment).

    Number of steps: {int(d.loc['step_count']):,d} (i.e. number of node children). Breakdown:
    - {float(d.loc['direct_count'] / max(1, d.loc['step_count'])):4.0%} {int(d.loc['direct_count']):>10,d} by direct references (i.e. parent records hard coded step);
    - {float(d.loc['indirect_count'] / max(1, d.loc['step_count'])):4.0%} {int(d.loc['indirect_count']):>10,d} by indirect references (i.e. parent records "dict" id),
    ```

    """)
    cell = {
        'block level 2,056,194': {
            None: """
            💡 The difference between the number of steps and the number of contents+nodes is due to sharing of objects,
            i.e. an object may may be referenced by several nodes.

            💡 The dict stores steps so that the pack file can store integers instead of repeatedly hard coding strings.
            The dict has a maximum size of 100_000.
            When the dict is full, all the new steps are stored "direct"ly in the pack file.

            💡 40% of the bytes of the tree occupied by hard-coded steps is a lot, but maybe it's legit. An investigation is in order.
            """,
            # contracts
            '/data/contracts/index': f"""
            💡 The steps here are hashes. The fact that they are all "direct" is OK. The dict is made for storings strings that occur often.
            """,
            '/data/contracts/index/*': f"""
            💡 There are only 13 unique steps that can be found here (e.g. "manager", "delegate"), the fact that 99%
            of them are "direct" suggests a problem with the "dict".
            """,
            '/data/contracts/index/*/manager': """
            💡 The 32 byte hash which live at the beginning of every entries represent 50% of the total of bytes here.
            """,
            # big_maps
            '/data/big_maps/index/*/contents': f"""
            💡 The steps all have length 65 here. They represent 818MB out of the 2.5GB that the full tree weighs.
            If there are many duplicates, something could maybe be done.

            💡 The indirect steps might be cluttering the dict.
            """,
            '/data/big_maps/index/*/contents/*': """
            💡 The "steps" breakdown is a bit strange.
            """,
            '/data/big_maps/index/*/contents/*/data': None,
        },
    }.get(block_desc, {}).get(path_zoom)
    if cell is not None: markdown(cell)

    # **************************************************************************
    # **************************************************************************
    markdown(f"""\
    ### Objects Kind

    The following plot groups the objects into 10 categories:
    - 4 categories for contents, depending on their size,
    - 5 categories for inodes, depending on the size of the node they belong to,
    - 1 extra category for inodes that appear in several categories at once. (No need to overthink that category)

    __Each column in such a graph sum to 100%__.
    """)
    code(f"""\
    plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                                 'ekind')""")
    cell = {
        'block level 2,056,194': {
            None: f"""\
            💡 The 3.7 million contents of length 0-31 take 46.7 bytes each on average. This result
            is correct, it reflects the fact that all objects in irmin-pack are prefixed by a 32 byte
            hash, which represents a huge overhead for these small contents.

            💡 94 nodes have a length greater than 16k, they make up 46% of the bytes of the tree.

            💡 Almost all nodes are small (i.e. with a length of 32 or less), but they only represent 20% of the bytes of the tree.
            """,
            # contracts
            '/data/contracts/index': None,
            '/data/contracts/index/*': f"""
            💡 All the contracts have less than 32 entries. They are individually encoded as single "root" inodes.
            """,
            '/data/contracts/index/*/manager': None,
            # big_maps
            '/data/big_maps/index/*/contents': f"""
            💡 The 62 very large nodes weigh almost 1GB, while the full tree weighs 2.5GB.
            """,
            '/data/big_maps/index/*/contents/*': None,
            '/data/big_maps/index/*/contents/*/data': None,
        },
    }.get(block_desc, {}).get(path_zoom)
    if cell is not None: markdown(cell)

    # **************************************************************************
    # **************************************************************************
    patch = ""
    if block_desc == 'block level 2,056,194':
        path = "In other words, the objects in the first row are added during cycle 444 and the objects in the 4th row during cycle 441."

    markdown(f"""\
    ### Objects Distance to Commit

    The following plot groups the objects into 5 categories, depending on how old they are.

    For instance, `<1 cycle away` implies that the objects in that row are less than 1 cycle away from
    the commit in the pack file (i.e. less than 8200 blocks away, less than 3 days away). {patch}

    The `4 or more cycles away` row is the largest because most objects of a tree are at the beginning of the file, in the "snapshot import" section.

    """)
    code(f"""\
    plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                                 'area_distance_from_origin')""")
    cell = {
        'block level 2,056,194': {
            None: f"""\
            💡 5% of the objects of the tree were created during cycle 444. Reminder: we are looking at cycle 445.

            💡 While cycle 444 pushed 3149MB to the pack file (not visible here), only 267MB (sum of the 1st row) are still useful for cycle 445 (8%).

            💡 While cycle 441 pushed 3474MB to the pack file (not visible here), only 95MB (sum of the 4th row) are still useful for cycle 445 (3%).
            """,
            # contracts
            '/data/contracts/index': f"""
            💡 This directory is modified all the time (most likely at every block). A new root inode has to be re-commited
            every time it is modified, this is why the bubble for "node count" shows on the first row.

            💡 Modifying a contract implies modifying all the objects from the root to it. 151k hidden nodes were modified (or added) during the last cycle while 124k contracts were actualy modified (or added) (see [tree_of_cycle_445_contracts-index-star.ipynb](tree_of_cycle_445_contracts-index-star.ipynb)).
            """,
            '/data/contracts/index/*': f"""\
            💡 124.4k contracts were modified (or added) during the last cycle.

            💡 14.5k contracts were modified (or added) 2 cycles ago and were not modified since.
            """,
            '/data/contracts/index/*/manager': f"""
            💡 While 124k contracts were modified (or added) during the previous cycle, only 5805 "manager" contents were added during the last cycle. Is this the number of new contracts?
            """,
            # big_maps
            '/data/big_maps/index/*/contents': None,
            '/data/big_maps/index/*/contents/*': """
            💡 Each cycle adds (or modifies) around 130k new unique entries in `data/big_maps/index/*/contents`.
            The fact that there is no peak at `<1 cycle` suggests that these directories are never modified when they are pushed to the tree.
            """,
            '/data/big_maps/index/*/contents/*/data': None,
        },
    }.get(block_desc, {}).get(path_zoom)
    if cell is not None: markdown(cell)

    # **************************************************************************
    # **************************************************************************
    if path_zoom is None:
        markdown(f"""\
        ### Objects Path
        ##### 4 categories

        The following plot groups the objects into 4 categories, depending on their ancestor directory.
        """)
        code(f"""\
        plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                                     'path2')""")

        cell = {
            'block level 2,056,194': {
                None: f"""\
                💡 Almost all the data is contained in `big_maps` and `contracts`.
                """,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    if path_zoom is None:
        markdown(f"""\
        ##### 8 categories
        The following plot is very similar to the previous one, but it groups the objects on 8 interesting locations.

        These paths are also individually analysed in separate files that you may find by going back to the index.
        """)
        code(f"""\
        plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                                     'path3')""")

        cell = {
            'block level 2,056,194': {
                None: f"""\
                💡 The `/data/contracts/index` directory is made of nearly 1 million inodes.
                It points to nearly 2 million nodes (i.e. `/data/contracts/index/*`).
                (In practice, it points to `2,003,307` nodes (not visible here) but only `1,988,785` objects due to sharing).
                See [tree_of_cycle_445_contracts-index.ipynb](./tree_of_cycle_445_contracts-index.ipynb).

                💡 The `/data/big_maps/index/*/contents` directories have `13,890,632` sub-directories (not visible here), but they
                are only `5,067,232` unique sub-directories, thanks to sharing.
                """,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    if path_zoom is None:
        markdown(f"""\
        ##### 100+ categories
        The following plot groups the objects given their precise location.
        """)
        code(f"""\
        plot_4_vertical_bubble_histo('/tmp/{fname}.csv',
                                     'path')""")
        cell = {
            'block level 2,056,194': {
                None: None,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    # **************************************************************************
    # **************************************************************************

    if path_zoom is None:
        markdown(f"""\
        ### Distance to Commit x Kind
        The following plots groups the objects in 2 grids.

        ##### Nodes
        This plot groups the nodes in 25 categories in order to highlight when the nodes were modified (or added), depending on their size.

        __All the points in such a grid grid sum to 100%.__
        """)
        code(f"""\
        plot_grid_bubble_histo('/tmp/{fname}.csv',
                               'node_count', 'area_distance_from_origin', 'ekind', elide_empty_rows=True)""")
        cell = {
            'block level 2,056,194': {
                None: """
                💡 90% of the nodes are small and were not modified during the last 4 cycles.

                💡 Almost all the massive nodes were modified during the last cycle (73 out of 94).
                """
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    if path_zoom is None:
        markdown(f"""\
        ##### Bytes
        This plot groups the objects in 50 categories in order to highlight where are located the bytes of the tree.
        """)
        code(f"""\
        plot_grid_bubble_histo('/tmp/{fname}.csv',
                               'bytes', 'area_distance_from_origin', 'ekind', elide_empty_rows=True)""")
        cell = {
            'block level 2,056,194': {
                None: """
                💡 The massive nodes weigh 46% of the tree (sum of the penultimate row), but they completely dominate in the last columns, suggesting that their modification is costing a lot.
                """,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    # **************************************************************************
    # **************************************************************************

    if path_zoom is None:
        markdown(f"""\
        ### Distance to Commit x Path
        ##### Bytes

        This plot groups the objects in 40 categories in order to highlight where are located the bytes of the tree.
        """)
        code(f"""\
        plot_grid_bubble_histo('/tmp/{fname}.csv',
                               'bytes', 'area_distance_from_origin', 'path3')""")
        cell = {
            'block level 2,056,194': {
                None: """
                💡 The nodes at `/data/big_maps/index/*/contents` weigh 43% of the tree, but they dominate the last columns, suggesting that their modification is costing a lot.
                """,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    # **************************************************************************
    # **************************************************************************
    if path_zoom is None:
        markdown(f"""\
        ### Kind x Path
        ##### Nodes
        This plot groups the nodes in 40 categories in order to highlight where can be found the nodes of each size.
        """)
        code(f"""\
        plot_grid_bubble_histo('/tmp/{fname}.csv',
                               'node_count', 'ekind', 'path3', elide_empty_cols=True)""")
        cell = {
            'block level 2,056,194': {
                None: """
                💡 There are 2 million contracts but there are only 70k directories in all contracts. On of these directories has a size of >16k children.
                """,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)

    if path_zoom is None:
        markdown(f"""\
        ##### Bytes

        This plot groups the objects in 80 categories in order to highlight what exactly is heavy for each of these weightful paths.
        """)
        code(f"""\
        plot_grid_bubble_histo('/tmp/{fname}.csv',
                               'bytes', 'ekind', 'path3')""")
        cell = {
            'block level 2,056,194': {
                None: """
                💡 40% of the bytes taken by the tree are located within the 62 nodes of length >16k children.

                💡 Most of the space occupied by contents is in the `/data/big_maps/index/*/contents/*/data` files.
                """,
            },
        }.get(block_desc, {}).get(path_zoom)
        if cell is not None: markdown(cell)


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
)

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
        path_zoom=p,
    )



#
