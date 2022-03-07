
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

L = {
    "Cs_0_31": "contents of length 0-31",
    "Cs_32_127": "contents of length 32-127",
    "Cs_128_511": "contents of length 128-511",
    "Cs_512_plus": "contents of length 512+",
    "Nl_0_32": "inodes of nodes of length 0-32",
    "Nl_33_256": "inodes of nodes of length 33-256",
    "Nl_257_2048": "inodes of nodes of length 257-2048",
    "Nl_2049_16384": "inodes of nodes of length 2049-16384",
    "Nl_16385_plus": "inodes of nodes of length 16385",
    "Multiple": "inodes that have 2 or more inode parents",
}

# csv_path = '/tmp/average_tree.ipynb.csv'
# discriminator = 'ekind'
# if True:
def plot_vertical_bubble_histo(csv_path, discriminator):
    df = pd.read_csv(csv_path)
    df = df.groupby(discriminator)[indicators].sum()
    # df['cat'] = pd.Categorical(df.index, L.keys())
    # df = df.sort_values('cat')
    # print(df)
    cols = ['count', 'node_count', 'bytes']
    xlabs = [
        'object count',
        'node count',
        'megabyte count'
    ]
    fmts = [
        fmt_count,
        fmt_count,
        fmt_megabyte,
    ]

    fontsize=9

    plt.close('all')
    figsize = np.asarray([len(cols), len(L)]) * 2
    # print('figsize', figsize)
    fig, ax = plt.subplots(
        # nrows=3,
        subplot_kw=dict(aspect="equal"),
        # dpi=200,
        figsize=figsize,
        # gridspec_kw = {'wspace':0, 'hspace':0},
        # constrained_layout=True,
    )


    xs = []
    max_area = np.pi * 0.5 ** 2
    min_area = 0.15 / 100
    max_pct = (df[cols] / df[cols].sum()).max().max()
    for x, (col, fmt) in enumerate(zip(cols, fmts)):
        fmt = fmt(df[col])
        xs.append(x + 0.5)

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
        d['x'] = x + 0.5
        # print(d)

        ys = []
        ylabs = []
        for y, ekind in enumerate(L.keys()):
            ys.append(-y)
            ylabs.append(L.get(ekind, ekind))

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
            )
            if row.pct > 0.25:
                ax.text(
                    row.x, -y + 0.012, text,
                    horizontalalignment='center', verticalalignment='center',
                    fontsize=fontsize,
                )
            else:
                ax.text(
                    row.x, -y - row.rad - 0.037, text,
                    horizontalalignment='center', verticalalignment='top',
                    fontsize=fontsize - 1,
                )


    # for axis in ['bottom','right']:
        # ax.spines[axis].set_linewidth(0)
    for axis in ['left', 'top','bottom','right']:
        ax.spines[axis].set_edgecolor('lightgrey')

    ax.tick_params(labelbottom=False,labeltop=True)
    ax.set_xlim(0, len(cols))
    ax.set_xticks(xs)
    ax.set_xticklabels(xlabs, rotation=45, ha='left', fontsize=fontsize)
    ax.xaxis.tick_top()

    ax.set_ylim(-len(L) +0.5, +0.5)


    ax.set_yticks(ys)
    ax.set_yticklabels(ylabs, fontsize=fontsize + 3)

    plt.tight_layout()
    plt.show()
