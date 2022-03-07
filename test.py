
# To name cycle dist: "distance from commit (cycles). <1, 1-2, 2-3, 3-4, 4+"
# To name contents:
# To name inodes:
#  - "xsmall" (Inode_root_values)
#  - "small" (root len 33-256)
#  - "medium" (root len 256-2048)
#  - "large" (root len 2049-16384)
#  - "xlarge" (root len 16385+)
#
# * PRESENTATION ***************************************************************
#
# - Let's pretend the pack file stops at the last commit analysed
# - A horizontal histogram that shows the pack file and where the areas and the analysed commits are
#   - It should also explain what is the average tree
#
# * AVERAGE TREE  **************************************************************
#
# - In an "average tree", 0d break down
#   - For each indicator, its total value
#
# - In an "average tree", 1d break down
#   - #roots/#entries/#bytes (3 pies or 1 vertical bubble list with 3 cols)
#   - For each indicator.
#     - Kind2
#     - Contents_size4
#     - Node_length5
#     - Kind9
#     - Distance from commit5
#     - Path4
#     - Path8
#       - The 6 large dirs are individually detailed in another section
#     - Path120
#   - It is a simplified version of the "grid" one that follows
#
# - In an "average tree", 2d break down
#   - 9 grids with a circle in all cells with area equal to importance
#     - #roots/#entries/#bytes
#     - 9kinds, 8paths, 5dists
#       - left=kind,   top=dist
#       - left=path,   top=dist
#       - left=path,   top=kind
#
# * LARGE DIRECTORIES IN AVERAGE TREE  *****************************************
#
# - For each weighful directories, a detailed analysis of why they are large
#   - We should see the breakdown as well as how it relates to the full dataset
#   - Direct/Indirect/Header
#
# * OVER ALL TREES *************************************************************
#
# - Over all trees, how evolves #roots/#entries/#bytes
#  - A simplified version of the following one
#
# - Over all trees, how evolves  #roots/#entries/#bytes
#  - One plot for each discriminator,
#  - In a plot are stacked the categories of an indicator
#
# - Over all trees, how many pages are touched
#
#
# * TRAVERSAL  *****************************************************************
#
# - three 2d grids, y=pack file area, x=which tree
#   - cells:
#     1. txt:how many entries, txt+shape:% of area entries
#     1. txt:how many bytes, txt+shape:% of area bytes
#     1. txt:how many pages, txt+shape:% of area pages
#
# * MISSING  *******************************************************************
#
# - step stuff
# - hash header stuff
# - 0d break down of the full file
#
#
#
#



big_dirs = ['/data/big_maps', '/data/contracts', '/data/rolls']

import pandas as pd
pd.set_option('display.max_rows', 120)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
# pd.describe_option('display')

discriminators = 'area_distance_from_origin path path2 path3 kind node_length contents_size'.split(' ')
indicators = 'count roots indirect_count direct_count bytes direct_bytes header_bytes other_bytes'.split(' ')

path_pats = []



path_pats.append(('/'                                                  , 'the rest',          'the rest',         ))
path_pats.append(('multiple'                                           , 'the rest',          'the rest',         ))
path_pats.append(('/predecessor_block_metadata_hash'                   , 'the rest',          'the rest',         ))
path_pats.append(('/predecessor_ops_metadata_hash'                     , 'the rest',          'the rest',         ))
path_pats.append(('/protocol'                                          , 'the rest',          'the rest',         ))
path_pats.append(('/data'                                              , 'the rest',          'the rest',         ))

path_pats.append(('/data/big_maps'                                     , 'the rest',           'the rest', ))
path_pats.append(('/data/big_maps/index'                               , '/data/big_maps/**/*',  'the rest', ))
path_pats.append(('/data/big_maps/index/*'                             , '/data/big_maps/**/*',  'the rest', ))
path_pats.append(('/data/big_maps/index/*/contents'                    , '/data/big_maps/**/*',  '/data/big_maps/index/*/contents'        , ))
path_pats.append(('/data/big_maps/index/*/contents/*'                  , '/data/big_maps/**/*',  '/data/big_maps/index/*/contents/*'      , ))
path_pats.append(('/data/big_maps/index/*/contents/*/data'             , '/data/big_maps/**/*',  '/data/big_maps/index/*/contents/*/data' , ))
path_pats.append(('/data/big_maps/index/*/key_type'                    , '/data/big_maps/**/*',  'the rest', ))
path_pats.append(('/data/big_maps/index/*/total_bytes'                 , '/data/big_maps/**/*',  'the rest', ))
path_pats.append(('/data/big_maps/index/*/value_type'                  , '/data/big_maps/**/*',  'the rest', ))
path_pats.append(('/data/big_maps/next'                                , '/data/big_maps/**/*',  'the rest', ))

path_pats.append(('/data/contracts'                                    , 'the rest',           'the rest' ))
path_pats.append(('/data/contracts/global_counter'                     , '/data/contracts/**/*', 'the rest' ))
path_pats.append(('/data/contracts/index'                              , '/data/contracts/**/*', '/data/contracts/index'           ))
path_pats.append(('/data/contracts/index/*'                            , '/data/contracts/**/*', '/data/contracts/index/*'         ))
path_pats.append(('/data/contracts/index/*/balance'                    , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/change'                     , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/counter'                    , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/data'                       , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/data/code'                  , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/data/storage'               , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/delegate'                   , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/delegated'                  , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/frozen_balance'             , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/frozen_balance/*'           , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/frozen_balance/*/deposits'  , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/frozen_balance/*/fees'      , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/frozen_balance/*/rewards'   , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/len'                        , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/manager'                    , '/data/contracts/**/*', '/data/contracts/index/*/manager' ))
path_pats.append(('/data/contracts/index/*/paid_bytes'                 , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/roll_list'                  , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))
path_pats.append(('/data/contracts/index/*/used_bytes'                 , '/data/contracts/**/*', '/data/contracts/index/*/<the rest>' ))

path_pats.append(('/data/rolls'                                        , 'the rest',           'the rest',    ))
path_pats.append(('/data/rolls/index'                                  , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/index/*'                                , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/index/*/successor'                      , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/limbo'                                  , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner'                                  , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner/current'                          , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner/current/*'                        , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner/snapshot'                         , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner/snapshot/*'                       , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner/snapshot/*/*'                     , '/data/rolls/**/*',     'the rest',    ))
path_pats.append(('/data/rolls/owner/snapshot/*/*/*'                   , '/data/rolls/**/*',     'the rest',    ))

path_pats.append(('/data/active_delegates_with_rolls'                  , 'the rest',          'the rest',         ))
path_pats.append(('/data/active_delegates_with_rolls/ed25519'          , 'the rest',          'the rest',         ))
path_pats.append(('/data/active_delegates_with_rolls/p256'             , 'the rest',          'the rest',         ))
path_pats.append(('/data/active_delegates_with_rolls/secp256k1'        , 'the rest',          'the rest',         ))
path_pats.append(('/data/cache'                                        , 'the rest',          'the rest',         ))
path_pats.append(('/data/cache/*'                                      , 'the rest',          'the rest',         ))
path_pats.append(('/data/cache/*/limit'                                , 'the rest',          'the rest',         ))
path_pats.append(('/data/commitments'                                  , 'the rest',          'the rest',         ))
path_pats.append(('/data/commitments/*'                                , 'the rest',          'the rest',         ))
path_pats.append(('/data/cycle'                                        , 'the rest',          'the rest',         ))
path_pats.append(('/data/cycle/*'                                      , 'the rest',          'the rest',         ))
path_pats.append(('/data/cycle/*/last_roll'                            , 'the rest',          'the rest',         ))
path_pats.append(('/data/cycle/*/nonces'                               , 'the rest',          'the rest',         ))
path_pats.append(('/data/cycle/*/nonces/*'                             , 'the rest',          'the rest',         ))
path_pats.append(('/data/cycle/*/random_seed'                          , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates'                                    , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates/ed25519'                            , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates/p256'                               , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates/secp256k1'                          , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates_with_frozen_balance'                , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates_with_frozen_balance/*'              , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates_with_frozen_balance/*/ed25519'      , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates_with_frozen_balance/*/p256'         , 'the rest',          'the rest',         ))
path_pats.append(('/data/delegates_with_frozen_balance/*/secp256k1'    , 'the rest',          'the rest',         ))
path_pats.append(('/data/domain'                                       , 'the rest',          'the rest',         ))
path_pats.append(('/data/liquidity_baking_cpmm_address'                , 'the rest',          'the rest',         ))
path_pats.append(('/data/liquidity_baking_escape_ema'                  , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index'                                , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*'                              , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+'                         , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/ciphertexts'             , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/ciphertexts/*'           , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/ciphertexts/*/data'      , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/commitments'             , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/commitments/*'           , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/commitments/*/data'      , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/commitments_size'        , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/nullifiers_hashed'       , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/nullifiers_hashed/*'     , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/nullifiers_hashed/*/data', 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/nullifiers_ordered'      , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/nullifiers_ordered/*'    , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/nullifiers_size'         , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/roots'                   , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/roots/*'                 , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/roots_level'             , 'the rest',          'the rest',         ))
path_pats.append(('/data/sapling/index/*/\\d+/total_bytes'             , 'the rest',          'the rest',         ))
path_pats.append(('/data/v1'                                           , 'the rest',          'the rest',         ))
path_pats.append(('/data/v1/constants'                                 , 'the rest',          'the rest',         ))
path_pats.append(('/data/v1/cycle_eras'                                , 'the rest',          'the rest',         ))
path_pats.append(('/data/version'                                      , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes'                                        , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/ballots'                                , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/ballots/ed25519'                        , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/ballots/p256'                           , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/current_period'                         , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/current_proposal'                       , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/listings'                               , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/listings/ed25519'                       , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/listings/p256'                          , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/listings/secp256k1'                     , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals'                              , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals/*'                            , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals/*/ed25519'                    , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals/*/p256'                       , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals/*/secp256k1'                  , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals_count'                        , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals_count/ed25519'                , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals_count/p256'                   , 'the rest',          'the rest',         ))
path_pats.append(('/data/votes/proposals_count/secp256k1'              , 'the rest',          'the rest',         ))




path_pats = {
    k: (v,w)
    for (k, v, w) in path_pats
}


df = pd.read_csv('csv/entries.csv')
print(df.shape)
print()

# Fix the broken paths
df['path'] = df.path.fillna('/')

# Add the derived discriminators
df['path2'] = df.path.apply(lambda x: path_pats[x][0])
df['path3'] = df.path.apply(lambda x: path_pats[x][1])

# Add the derived indicators
df['header_bytes'] = df['count'] * 32
df['other_bytes'] = df.bytes - df.header_bytes - df.direct_bytes
df['roots'] = df.apply(lambda row: row['count'] if '_root_' in row.kind or 'Contents' == row.kind else 0, axis=1)
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
    d = df.groupby(c)[indicators].sum().sort_values('bytes', ascending=False)
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


h = df3.query('path == "/data/big_maps/index/*/contents"').groupby(['kind', 'node_length'])[indicators].sum().reset_index().sort_values(['node_length', 'kind'])
h['step_count'] = h.indirect_count + h.direct_count
print(h)

d = df2.query('kind == "Contents"').groupby('path')[indicators].sum().sort_values('bytes', ascending=False)
d['avg_strlen'] = d['other_bytes'] / d['count']
d['avg_entrylen'] = d['bytes'] / d['count']
d['% in str'] = d['other_bytes'] / d['bytes'] * 100
print(d)


#
