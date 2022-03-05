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
#  - One plot for each indicator,
#  - In a plot are stacked curve that show the variations of the categories of an indicator
#
# - In an "average tree", over distance to commit, what is touched in each area?
#   - % of pages, % of bytes, % of entries, % of each kind
#   - This doesn't really make sense for the furthest area :(
#
# - Over all trees, how many pages of data are needed to be read
#
#
#
#
# - Maybe section on data that I dropped
#   - the root stuff
#   - indirect count vs direct count
#   - count

big_dirs = ['/data/big_maps', '/data/contracts', '/data/rolls']

import pandas as pd
pd.set_option('display.max_rows', 45)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
# pd.describe_option('display')



df = pd.read_csv('csv/entries.csv')

df2 = df.copy()

# Drop the first ~5 cycle stats as they are very close to the snapshot
df = df[df.parent_cycle_start >= 434]

# Switch from 'entry_area' to 'area_distance_from_origin'
df['area_distance_from_origin'] = (df.parent_cycle_start - df.entry_area).clip(0, 5)
df = df.drop(columns=['entry_area'])
df = df.groupby(['area_distance_from_origin', 'parent_cycle_start', 'path_prefix', 'kind']).sum().reset_index()

# Average together all the remaning cycle_starts
df = df.set_index(['parent_cycle_start', 'path_prefix', 'kind', 'area_distance_from_origin'], verify_integrity=True).groupby(level=[1, 2, 3]).mean().reset_index()

# Make the "root" information disappear
mapping = {'Contents_0_31': 'Contents_0_31',
 'Contents_128_511': 'Contents_128_511',
 'Contents_32_127': 'Contents_32_127',
 'Contents_512_plus': 'Contents_512_plus',
 'Inode_nonroot_tree': 'Inode_tree',
 'Inode_nonroot_values': 'Inode_values',
 'Inode_root_tree': 'Inode_tree',
 'Inode_root_values': 'Inode_values'}
df['kind'] = df['kind'].apply(lambda x: mapping.get(x))
df = df.groupby(['area_distance_from_origin', 'path_prefix', 'kind']).sum().reset_index()


df0 = df
# print(df)
# print()
# print()

# exit()


df = pd.read_csv('csv/steps.csv')

# Drop the first ~5 cycle stats as they are very close to the snapshot
df = df[df.parent_cycle_start >= 434]

# Switch from 'entry_area' to 'area_distance_from_origin'
df['area_distance_from_origin'] = (df.parent_cycle_start - df.entry_area).clip(0, 5)
df = df.drop(columns=['entry_area'])
df = df.groupby(['area_distance_from_origin', 'parent_cycle_start', 'path_prefix']).sum().reset_index()

# Average together all the remaning cycle_starts
df = df.set_index(['parent_cycle_start', 'path_prefix', 'area_distance_from_origin'], verify_integrity=True).groupby(level=[1, 2]).mean().reset_index()

# Merge with [df1]
df['kind'] = 'Inode_values'
df = df.merge(right=df0, how='outer', on=['path_prefix', 'area_distance_from_origin', 'kind'])

for col in 'indirect_count,direct_count,direct_bytes'.split(','):
    df[col] = df[col].fillna(0.)
del df0

# drop all the small path_prefix
df = df[df.path_prefix.isin(big_dirs)]

# Make "part" a new distriminator
df['direct_steps'] = df.direct_bytes
df['hash_headers'] = df['count'] * 32
df['other'] = df['bytes'] - df.direct_bytes - df.hash_headers
df = df.drop(columns=['indirect_count', 'direct_count', 'count', 'bytes', 'direct_bytes'])
df = df.melt(id_vars=['path_prefix', 'kind', 'area_distance_from_origin'], value_vars=['direct_steps', 'hash_headers', 'other'], value_name='bytes', var_name='part')

# df.pivot(index=None, columns=None, values=None)

# pd.set_option('display.max_rows', 100)
# df1 = df
print(df.head())
print(df.tail())













# pd.set_option('display.max_columns', None)  # or 1000
# pd.set_option('display.max_rows', None)  # or 1000
# pd.set_option('display.max_colwidth', None)  # or 199


# df = df.query('parent_cycle_start == 445')
# df = df.query('parent_cycle_start == 8')
# df =  df[df.parent_cycle_start >= 5]

# for k in ["entry_area", "parent_cycle_start", "path_prefix", "kind"]:
#     d = df.groupby([k])[['count', 'bytes']].sum()
#     print(d)
#     print()


# for k in ["entry_area", "path_prefix", "kind"]:
#     d = df.groupby([k, 'parent_cycle_start'])[['count', 'bytes']].sum().groupby(level=[0]).mean()
#     print(d)
#     print()
#     break



#
