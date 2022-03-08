
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

indicators = 'count node_count inner_count blob_count step_count indirect_count direct_count bytes direct_bytes header_bytes other_bytes'.split(' ')

def fmt_count(samples):
    m = samples.max()
    # if m >= 10e6:
    #     def fmt(x):
    #         return f'{x / 1e9:.3f} G'
    # elif m >= 10e3:
    #     def fmt(x):
    #         return f'{x / 1e6:.3f} M'
    # else:
    def fmt(x):
        return f'{int(x):,d} '
    return fmt

def fmt_megabyte(_samples):
    def fmt(x):
        x = x / 1e6
        if x > 40:
            return f'{x:.0f} MB'
        elif x > 10:
            return f'{x:.1f} MB'
        elif x > 1:
            return f'{x:.2f} MB'
        else:
            return f'{x:.3f} MB'
    return fmt

rows = {
    "ekind": {
        "Cs_0_31": "contents of length 0-31",
        "Cs_32_127": "contents of length 32-127",
        "Cs_128_511": "contents of length 128-511",
        "Cs_512_plus": "contents of length 512+",
        "Nl_0_32": "inodes of nodes of length 0-32",
        "Nl_33_256": "inodes of nodes of length 33-256",
        "Nl_257_2048": "inodes of nodes of length 257-2048",
        "Nl_2049_16384": "inodes of nodes of length 2049-16384",
        "Nl_16385_plus": "inodes of nodes of length 16385",
        "Multiple": "inodes that appear in 2 or more contexts",
    },
    "area_distance_from_origin": {
        1: '<1 cycle',
        2: '1-2 cycles',
        3: '2-3 cycles',
        4: '3-4 cycles',
        5: '4 or more cycles',
    },
    'path2': {
        '/data/big_maps/**/*': '/data/big_maps/**/*',
        '/data/contracts/**/*': '/data/contracts/**/*',
        '/data/rolls/**/*': '/data/rolls/**/*',
        'the rest': 'the rest',
    },
    'path3': {
        '/data/big_maps/index/*/contents': '/data/big_maps/index/*/contents',
        '/data/big_maps/index/*/contents/*': '/data/big_maps/index/*/contents/*',
        '/data/big_maps/index/*/contents/*/data': '/data/big_maps/index/*/contents/*/data',
        '/data/contracts/index': '/data/contracts/index',
        '/data/contracts/index/*': '/data/contracts/index/*',
        '/data/contracts/index/*/manager': '/data/contracts/index/*/manager',
        '/data/contracts/index/*/<the rest>': '/data/contracts/index/*/<the rest>',
        'the rest': 'the rest',
    },
}

# plt.rcParams['font.family'] = 'serif'
# plt.rcParams['font.serif'] = 'Ubuntu'
# plt.rcParams['font.monospace'] = 'Ubuntu Mono'

discriminator = 'ekind'

# csv_path = '/tmp/average_tree.ipynb.csv'
# discriminator = 'path2'
# if True:
def plot_vertical_bubble_histo(csv_path, discriminator):
    df = pd.read_csv(csv_path)
    df = df.groupby(discriminator)[indicators].sum()

    if discriminator != "path":
        row_names = list(rows[discriminator].keys())
        ylabs = [rows[discriminator][x] for x in row_names]
    else:
        row_names = sorted(set(df.index))
        maxlen = max(map(len, row_names))
        ylabs = row_names

    cols = ['count', 'node_count', 'bytes']
    xlabs = [
        'object count',
        'node count',
        'byte count'
    ]
    yborder = 0.2
    xborder = 0.4
    xshift = 1.1
    xs = [
        xborder + 0.5,
        xborder + 0.5 + xshift * 1,
        xborder + 0.5 + xshift * 2,
    ]
    fmts = [
        fmt_count,
        fmt_count,
        fmt_megabyte,
    ]

    if discriminator != 'path':
        figsize = np.asarray([5.5, 5.5])
    else:
        figsize = np.asarray([5.5, 50])
    fontsize_out=8.5
    fontsize_small=6
    fontsize=6.5
    fontstuff = dict(
        # fontweight="ultralight",
        # fontstyle='italic',
        # fontfamily = 'sans-serif',
        fontweight='semibold',
        # fontfamily = 'serif',
        # fontvariant='small-caps',
        # fontfamily = 'monospace',
        # fontproperties='Ubuntu',
        # fontproperties='Ubuntu Mono',
        # fontserif = 'Ubuntu',
        # fontmonospace = 'Ubuntu Mono',
    )
    if 'path' in discriminator:
        xaxis_fontstuff = dict(
            fontfamily = 'monospace',
        )
    else:
        xaxis_fontstuff = {}
    # print(fontstuff)

    plt.close('all')
    # figsize = np.asarray([len(cols), len(L)]) * 2

    # print('figsize', figsize)
    fig, ax = plt.subplots(
        # nrows=3,
        subplot_kw=dict(aspect="equal"),
        dpi=100,
        figsize=figsize,
        # gridspec_kw = {'wspace':0, 'hspace':0},
        # constrained_layout=True,
    )


    max_area = np.pi * 0.5 ** 2
    min_area = 0.15 / 100
    max_pct = (df[cols] / df[cols].sum()).max().max()
    for (col, fmt, x) in (zip(cols, fmts, xs)):
        fmt = fmt(df[col])

        total = df[col].sum()
        d = df[col].to_frame()
        d['pct'] = d[col] / total

        d['area'] = d.pct / max_pct * max_area
        d['area'] = np.where(
            d.area == 0,
            0,
            np.where(d.area < min_area, min_area, d.area)
        )
        d['rad'] = (d.area / np.pi) ** 0.5
        d['x'] = x
        # print(d)

        ys = []
        for y, ekind in enumerate(row_names):
            ys.append(-y)

            if ekind not in d.index: continue
            row = d.loc[ekind]
            if row.rad == 0: continue

            rec = plt.Circle(
                xy=[row.x, -y],
                radius=row.rad,
            )
            ax.add_patch(rec)

            text0 = fmt(row[col])

            if row.pct > 0.04:
                text = f'{row.pct:.0%}'
            elif row.pct > 0.003:
                text = f'{row.pct:.1%}'
            elif row.pct > 0.0003:
                text = f'{row.pct:.2%}'
            else:
                text = f'{row.pct:.3%}'

            ax.text(
                row.x, -y + row.rad, text0,
                horizontalalignment='center', verticalalignment='bottom',
                fontsize=fontsize,
                **fontstuff,
            )
            if row.pct > 0.25:
                ax.text(
                    row.x, -y + 0.012, text,
                    horizontalalignment='center', verticalalignment='center',
                    fontsize=fontsize,
                    # **fontstuff,
                )
            else:
                ax.text(
                    row.x, -y - row.rad - 0.037, text,
                    horizontalalignment='center', verticalalignment='top',
                    fontsize=fontsize_small,
                    # **fontstuff,
                )


    # for axis in ['bottom','right']:
        # ax.spines[axis].set_linewidth(0)
    for axis in ['left', 'top','bottom','right']:
        ax.spines[axis].set_edgecolor('lightgrey')

    # if 'path' in discriminator:
    if True:
        ax.tick_params(
            labelbottom=False,labeltop=True,
            labelleft=False,labelright=True,
        )
        ax.xaxis.tick_top()
        ax.yaxis.tick_right()
    # else:
    #     ax.tick_params(
    #         labelbottom=False,labeltop=True,
    #     )
    #     ax.xaxis.tick_top()

    ax.set_xlim(0, max(xs) + 0.5 + xborder)
    ax.set_xticks(xs)
    ax.set_xticklabels(xlabs, rotation=45, ha='left', fontsize=fontsize_out)

    ax.set_ylim(-len(row_names) +0.5 - yborder, +0.5 + yborder)


    ax.set_yticks(ys)
    ax.set_yticklabels(ylabs, fontsize=fontsize_out, **xaxis_fontstuff)

    plt.tight_layout()
    plt.show()
