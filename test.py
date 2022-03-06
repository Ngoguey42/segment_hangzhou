# To name cycle dist: "distance from commit (cycles). <1, 1-2, 2-3, 3-4, 4+"
# To name contents:
# To name inodes:
#  - "xsmall" (Inode_root_values)
#  - "small" (root len 33-256)
#  - "medium" (root len 256-2048)
#  - "large" (root len 2049-16384)
#  - "xlarge" (root len 16385+)
#
# What should I plot?
#
# E/B: Most (all?) stats should be plot twice, once with #entries, once with #bytes.
#
# - In an "average tree", which stuff takes up E/B, 1d correlation
#   - A pie for each indicator.
#     - 6+2kind, 3+Kpath, 3part, 5dist
#   - It is a simplified version of the "grid" one that follows
#   - The "path" one justifies never talking about the other dirs in the future
#     Maybe print a table for that one?
#
#
# - In an "average tree", which stuff takes up E/B, 2d correlation
#   - A grid with a circle in all cells with area equal to importance
#     - 6 kind, 3path, 3part, 5dist
#     - left=kind,   top=dist
#     - left=path,   top=dist
#     - left=part,   top=dist
#     - left=kind,   top=path
#     - left=kind,   top=part
#     - left=path,   top=part
#
# - Over all trees, how evolves the E/B
#  - A simplified version of the following one
#
# - Over all trees, how evolves the amount of E/B
#  - One plot for each discriminator,
#  - In a plot are stacked curve that show the variations of the categories of an indicator
#
# - In an "average tree", over distance to commit, what is touched in each area?
#   - % of pages, % of bytes, % of entries, % of each kind
#   - This doesn't really make sense for the furthest area :(
#
# - Over all trees, how many pages of data are needed to be read
#
# - three 2d grids, y=pack file area, x=which tree
#   - cells:
#     1. txt:how many entries, txt+shape:% of area entries
#     1. txt:how many bytes, txt+shape:% of area bytes
#     1. txt:how many pages, txt+shape:% of area pages
#
#
#
#



big_dirs = ['/data/big_maps', '/data/contracts', '/data/rolls']

import pandas as pd
pd.set_option('display.max_rows', 57)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
# pd.describe_option('display')

discriminators = 'area_distance_from_origin path kind node_length contents_size'.split(' ')
indicators = 'count indirect_count direct_count bytes direct_bytes header_bytes other_bytes'.split(' ')

df = pd.read_csv('csv/entries.csv')
print(df.shape)
print()


# Add the derived indicators
df['header_bytes'] = df['count'] * 32
df['other_bytes'] = df.bytes - df.header_bytes - df.direct_bytes
print(df.shape)
print()


df2 = df.copy()

# Drop the first ~5 cycle stats as they are very close to the snapshot
df = df[df.parent_cycle_start >= 434]
print(df.shape)
print()

# Switch indicators to float
for c in indicators:
    df[c] = df[c].astype(float)

# Switch from 'entry_area' to 'area_distance_from_origin'
df['area_distance_from_origin'] = (df.parent_cycle_start - df.entry_area).clip(0, 5)
df = df.drop(columns=['entry_area'])
print(df.shape)
print()

# Average together all the remaning cycle_starts
tree_count = len(set(df.parent_cycle_start))
print('tree_count', tree_count)
df = df.groupby(discriminators)[indicators].sum() / tree_count
df = df.reset_index()
print(df.shape)

d = df[indicators].sum().to_frame().T
l = []
for c in indicators:
    if 'byte' in c:
        e = c.replace('byte', 'Mbyte')
        d[e] = d[c] / 1e6
        del d[c]
        c = e
    l.append(c)
d = d[l].T
print(d)

for c in discriminators:
    # print(c, len(set(df[c])), set(df[c]))
    d = df.groupby(c)[indicators].sum().sort_values('bytes')
    l = []
    for c in indicators:
        d[c + '%'] = d[c] / d[c].sum() * 100
        l += [c + '%']
        if 'byte' in c:
            e = c.replace('byte', 'Mbyte')
            d[e] = d[c] / 1e6
            del d[c]
            c = e
        l.insert(-1, c)
    d = d[l]
    print(d)
    # if len(d) > 40:
        # raise "super"


df3 = df.copy()






df = pd.read_csv('csv/areas.csv')
print(df.shape)
print()
df4 = df.copy()
e = df.groupby('area')[['entry_count', 'byte_count']].sum() / 1_000_000
e['page_count'] =  e.byte_count * 1e6 / 4096
print('per area')
print(e)


d = df2.groupby(['parent_cycle_start', 'entry_area'])[indicators].sum().reset_index().pivot('parent_cycle_start', 'entry_area', 'bytes') / 1_000_000
print('bytes touched')
print(d)
print('% bytes touched')
print(d / e.byte_count.to_frame().T.values * 100)
print()
print()


f = df2.groupby(['parent_cycle_start', 'entry_area'])[indicators].sum().reset_index().pivot('parent_cycle_start', 'entry_area', 'count') / 1_000_000
print('entries touched')
print(f)
print('% entries touched')
print(f / e.entry_count.to_frame().T.values * 100)
print()
print()



df = pd.read_csv('csv/memory_layout.csv')
print(df.shape)
print()


g = df.pivot('parent_cycle_start', 'entry_area', 'pages_touched').fillna(0).astype(int)
print('pages touched')
print(g)
print('% pages touched')
print(g / e.page_count.to_frame().T.values * 100)


#
