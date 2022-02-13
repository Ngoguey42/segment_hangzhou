
import pandas as pd
pd.set_option('display.max_rows', 225)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# pd.describe_option('display')

df = pd.read_csv('comanche_d0.csv')
# df = pd.read_csv('d0.csv')


# Drop the first ~5 cycle stats as they are very close to the snapshot
df = df[df.parent_cycle_start >= 434]

# Switch from 'entry_area' to 'area_distance_from_origin'
df['area_distance_from_origin'] = (df.parent_cycle_start - df.entry_area).clip(0, 5)
df = df.drop(columns=['entry_area'])
df = df.groupby(['area_distance_from_origin', 'parent_cycle_start', 'path_prefix', 'kind']).sum().reset_index()

# Average together all the remaning cycle_starts
df = df.set_index(['parent_cycle_start', 'path_prefix', 'kind', 'area_distance_from_origin'], verify_integrity=True).groupby(level=[1, 2, 3]).mean().reset_index()


print(df)
print()
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
