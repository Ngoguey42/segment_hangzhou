
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import itertools
import scipy.optimize as spo

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

def fmt_count_short(x):
    data = [
        0,
        f"{x:.0f} ",
        10_000,
        f"{x / 1e3:.0f} K",
        1_000_000,
        f"{x / 1e6:.1f} M",
        10_000_000,
        f"{x / 1e6:.0f} M",
        1_000_000_000,
        f"{x / 1e9:.1f} G",
        10_000_000_000,
        f"{x / 1e9:.0f} G",
    ]
    while True:
        low_bound = data.pop(0)
        s = data.pop(0)
        if len(data) == 0:
            return s
        if x < data[0]:
            return s

def fmt_byte_short(x):
    data = [
        0,
        f"{x:.0f} B",
        10_000,
        f"{x / 1e3:.0f} KB",
        1_000_000,
        f"{x / 1e6:.1f} MB",
        10_000_000,
        f"{x / 1e6:.0f} MB",
        1_000_000_000,
        f"{x / 1e9:.1f} GB",
        10_000_000_000,
        f"{x / 1e9:.0f} GB",
    ]
    while True:
        low_bound = data.pop(0)
        s = data.pop(0)
        if len(data) == 0:
            return s
        if x < data[0]:
            return s

def fmt_megabyte(_samples):
    def fmt(x):
        x = x / 1e6
        if x > 40:
            return f'{x:.0f} MB'
        elif x > 10:
            return f'{x:.1f} MB'
        elif x > 1:
            return f'{x:.2f} MB'
        elif x > 0.005:
            return f'{x:.3f} MB'
        else:
            x = int(x * 1e6)
            return f'{x:d} B'
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
        1: '<1 cycle away from commit',
        2: '1 to 2 cycles away',
        3: '2 to 3 cycles away',
        4: '3 to 4 cycles away',
        5: '4 or more cycles away',
    },
    'path2': {
        '/data/big_maps/**/*': '/data/big_maps/**/*',
        '/data/contracts/**/*': '/data/contracts/**/*',
        '/data/rolls/**/*': '/data/rolls/**/*',
        'the rest': 'the rest',
    },
    'path3': {
        '/data/contracts/index': '/data/contracts/index',
        '/data/contracts/index/*': '/data/contracts/index/*',
        '/data/contracts/index/*/manager': '/data/contracts/index/*/manager',
        '/data/contracts/index/*/<the rest>': '/data/contracts/index/*/<the rest>',
        '/data/big_maps/index/*/contents': '/data/big_maps/index/*/contents',
        '/data/big_maps/index/*/contents/*': '/data/big_maps/index/*/contents/*',
        '/data/big_maps/index/*/contents/*/data': '/data/big_maps/index/*/contents/*/data',
        'the rest': 'the rest',
    },
}

csv_path = '/tmp/tree_of_cycle_445.ipynb.csv'
csv_path = '/tmp/tree_of_cycle_445_contracts-index.ipynb.csv'
top_discriminator = 'area_distance_from_origin'
right_discriminator = 'ekind'
# top_discriminator = 'area_distance_from_origin'
# right_discriminator = 'ekind'
# if True:
def plot_grid_bubble_histo(csv_path, indicator, top_discriminator, right_discriminator,
                           elide_empty_rows=False, elide_empty_cols=False):
    df = pd.read_csv(csv_path)
    df = df.groupby([right_discriminator, top_discriminator])[indicator].sum().reset_index()
    df = df.pivot(right_discriminator, top_discriminator, indicator).fillna(0)

    row_names = list(rows[right_discriminator].keys())
    if elide_empty_rows:
        row_names = [
            x
            for x in row_names
            if df.loc[x].sum() > 0
        ]
    ylabs = [rows[right_discriminator][x] for x in row_names]

    col_names = list(rows[top_discriminator].keys())
    if elide_empty_cols:
        col_names = [
            x
            for x in col_names
            if df[x].sum() > 0
        ]
    if top_discriminator == 'area_distance_from_origin':
        col_names = col_names[::-1]
    xlabs = [rows[top_discriminator][x] for x in col_names]

    min_area = 0.15 / 100
    max_area = np.pi * 0.5 ** 2
    yborder = 0.2
    xborder = 0.4
    xshift = 1.3
    yshift = 1.

    xs = xborder + 0.5 + np.arange(len(xlabs)) * xshift
    ys = (yborder + 0.5 + np.arange(len(ylabs)) * yshift)[::-1]
    if indicator == 'bytes':
        fmt = fmt_megabyte(df.values.flatten())
    else:
        fmt = fmt_count(df.values.flatten())
    figsize = np.asarray([10, 8])

    fontsize_out=8.5
    fontsize_small=6
    fontsize=6.5
    fontstuff = dict(
        fontweight='semibold',
    )

    plt.close('all')
    fig, ax = plt.subplots(
        subplot_kw=dict(aspect="equal"),
        dpi=100,
        figsize=figsize,
    )

    total = df.sum().sum()
    max_pct = df.max().max() / total

    for i, j in itertools.product(range(len(row_names)), range(len(col_names))):
        row, col = row_names[i], col_names[j]
        x, y = xs[j], ys[i]

        if row in df.index and col in df.columns:
            v = df.loc[row, col]
        else:
            v = 0
        pct = v / total
        area = pct / max_pct * max_area
        if area == 0: continue
        if area < min_area:
            area = min_area
        rad = (area / np.pi) ** 0.5

        if pct > 0.04:
            text = f'{pct:.0%}'
        elif pct > 0.003:
            text = f'{pct:.1%}'
        elif pct > 0.0003:
            text = f'{pct:.2%}'
        else:
            text = f'{pct:.3%}'

        ax.add_patch(plt.Circle(xy=[x, y], radius=rad, zorder=10))
        ax.text(
            x, y + rad + 0.01, fmt(v),
            horizontalalignment='center', verticalalignment='bottom',
            fontsize=fontsize,
            fontweight='semibold',
            zorder=15,
        )
        if pct > 0.04:
            ax.text(
                x, y, text,
                horizontalalignment='center', verticalalignment='center',
                fontsize=fontsize,
                zorder=15,
            )
        elif pct > 0.001:
            ax.text(
                x, y - rad - 0.037, text,
                horizontalalignment='center', verticalalignment='top',
                fontsize=fontsize,
                zorder=15,
            )



    for axis in ['left', 'top','bottom','right']:
        ax.spines[axis].set_visible(False)

    plt.tick_params(
        top=False, bottom=False, left=False, right=False,
        labelbottom=False,labeltop=True,
        labelleft=False,labelright=True,
    )
    ax.grid(True, axis='both', zorder=6, color='lightgrey')

    ax.set_xlim(min(xs) - 0.5 - xborder, max(xs) + 0.5 + xborder)
    ax.set_xticks(xs)
    ax.set_xticklabels(xlabs, rotation=45, ha='left', fontsize=fontsize_out)

    ax.set_ylim(min(ys) - 0.5 - yborder, max(ys) + 0.5 + yborder)
    ax.set_yticks(ys)
    ax.set_yticklabels(ylabs, fontsize=fontsize_out)

    plt.tight_layout()
    plt.show()



csv_path = '/tmp/tree_of_cycle_445_contracts-index-star-manager.ipynb.csv'
csv_path = '/tmp/tree_of_cycle_445.ipynb.csv'
discriminator = 'ekind'
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

    min_area = 0.15 / 100
    max_area = np.pi * 0.5 ** 2
    yborder = 0.2
    xborder = 0.4
    xshift = 1.3
    cols = [
        'blob_count',
        'node_count',
        'inner_count',
        'count',
        'bytes',
    ]
    xlabs = [
        'contents count',
        'node count',
        'hidden node count',
        'object count',
        'byte count',
    ]
    xs = [
        xborder + 0.5,
        xborder + 0.5 + xshift * 1,
        xborder + 0.5 + xshift * 2,
        xborder + 0.5 + xshift * 3,
        xborder + 0.5 + xshift * 4,
    ]
    fmts = [
        fmt_count,
        fmt_count,
        fmt_count,
        fmt_count,
        fmt_megabyte,
    ]

    if discriminator != 'path':
        figsize = np.asarray([10, 6])
    else:
        figsize = np.asarray([10, 50])
    fontsize_out=8.5
    fontsize_small=6
    fontsize=6.5
    fontstuff = dict(
        fontweight='semibold',
    )
    if 'path' in discriminator:
        xaxis_fontstuff = dict(
            fontfamily = 'monospace',
        )
    else:
        xaxis_fontstuff = {}

    plt.close('all')
    fig, ax = plt.subplots(
        subplot_kw=dict(aspect="equal"),
        dpi=100,
        figsize=figsize,
    )

    max_pct = (df[cols] / df[cols].sum()).max().max()
    for (col, fmt, x) in (zip(cols, fmts, xs)):
        fmt = fmt(df[col])

        total = df[col].sum()
        if total == 0: continue

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

        ys = []
        for y, ekind in enumerate(row_names):
            ys.append(-y)

            if ekind not in d.index: continue
            row = d.loc[ekind]
            if row.rad == 0: continue

            rec = plt.Circle(
                xy=[row.x, -y],
                radius=row.rad,
                zorder=10,
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
                row.x, -y + row.rad + 0.01, text0,
                horizontalalignment='center', verticalalignment='bottom',
                fontsize=fontsize,
                zorder=15,
                **fontstuff,
            )
            if row.pct > 0.25:
                ax.text(
                    row.x, -y + 0.012, text,
                    horizontalalignment='center', verticalalignment='center',
                    fontsize=fontsize,
                    zorder=15,
                )
            else:
                ax.text(
                    row.x, -y - row.rad - 0.037, text,
                    horizontalalignment='center', verticalalignment='top',
                    fontsize=fontsize_small,
                    zorder=15,
                )

    for axis in ['left', 'top','bottom','right']:
        ax.spines[axis].set_edgecolor('white')
        # ax.spines[axis].set_edgecolor('lightgrey')

    plt.tick_params(
        top=False, bottom=False, left=False, right=False,
        labelbottom=False,labeltop=True,
        labelleft=False,labelright=True,
    )
    ax.grid(True, axis='x', zorder=6, color='lightgrey')

    ax.set_xlim(0, max(xs) + 0.5 + xborder)
    ax.set_xticks(xs)
    ax.set_xticklabels(xlabs, rotation=45, ha='left', fontsize=fontsize_out)

    ax.set_ylim(-len(row_names) +0.5 - yborder, +0.5 + yborder)
    ax.set_yticks(ys)
    ax.set_yticklabels(ylabs, fontsize=fontsize_out, **xaxis_fontstuff)

    plt.tight_layout()
    plt.show()

def plot_area_curve_object_count(csv_path, xcolumn, subtitle, xbounds=None):
    df = pd.read_csv(csv_path)

    figsize = np.asarray([10, 6])
    plt.close('all')
    fig, ax = plt.subplots(
        dpi=100,
        figsize=figsize,
    )

    layout = dict(
        blob_count=("blue", "blob"),
        node_count=("orange", "node"),
        inner_count=("red", "hidden node"),
        count=("black", "total"),
    )

    if xbounds is not None:
        minx, maxx = xbounds
        mask = (df[xcolumn] >= minx) & (df[xcolumn] <= maxx)
        xs = df[xcolumn].values[mask]
    else:
        xs = df[xcolumn].values
    for col in "count inner_count node_count blob_count".split(' '):
        if xbounds is not None:
            ys = df[col].values[mask]
        else:
            ys = df[col].values
        color, title = layout[col]

        plt.plot(xs, ys, 'x', c=color)

        def fit_func(x, a, b):
            return a*x + b
        [slope, intercept], _ = spo.curve_fit(fit_func, xs, ys)
        plt.plot(xs, fit_func(xs, slope, intercept), c=color, alpha=0.33,
                 label=title + ' (slope {:+.0f}K per cycle)'.format(slope / 1e3))

    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.10),
              fancybox=True, shadow=True, ncol=2)
    plt.suptitle(subtitle)
    ax.grid(True, axis='y', color='lightgrey')

    ax.set_xlim(xs.min(), xs.max())
    ax.set_xticks(xs)
    ax.set_xlabel('area')

    ax.set_ylim(0, df['count'].max() * 1.0)
    xs = np.arange(0, df['count'].max() + 1e6, 1e6)
    ax.set_yticks(xs)
    ax.set_yticklabels([
        f'{x / 1e6:.0f}M'
        for x in xs
    ])
    ax.set_ylabel('object count')

    plt.show()

def plot_area_curve_byte_count(csv_path, xcolumn, subtitle, xbounds=None):
    df = pd.read_csv(csv_path)

    figsize = np.asarray([10, 6])
    plt.close('all')
    fig, ax = plt.subplots(
        dpi=100,
        figsize=figsize,
    )

    layout = dict(
        blob_count=("blue", "blob"),
        node_count=("orange", "node"),
        inner_count=("red", "hidden node"),
        count=("black", "total"),
        blob_bytes=("blue", "blob"),
        node_bytes=("orange", "node"),
        inner_bytes=("red", "hidden node"),
        bytes=("black", "total"),
    )

    if xbounds is not None:
        minx, maxx = xbounds
        mask = (df[xcolumn] >= minx) & (df[xcolumn] <= maxx)
        xs = df[xcolumn].values[mask]
    else:
        xs = df[xcolumn].values
    for col in "bytes inner_bytes node_bytes blob_bytes".split(' '):
        if xbounds is not None:
            ys = df[col].values[mask]
        else:
            ys = df[col].values
        color, title = layout[col]

        plt.plot(xs, ys, 'x', c=color)

        def fit_func(x, a, b):
            return a*x + b
        [slope, intercept], _ = spo.curve_fit(fit_func, xs, ys)
        if slope / 1e6 < 5:
            label = title + ' (slope {:+.1f}MB per cycle)'.format(slope / 1e6)
        else:
            label = title + ' (slope {:+.0f}MB per cycle)'.format(slope / 1e6)
        plt.plot(xs, fit_func(xs, slope, intercept), c=color, alpha=0.33, label=label)

    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.10),
              fancybox=True, shadow=True, ncol=2)
    plt.suptitle(subtitle)
    ax.grid(True, axis='y', color='lightgrey')

    ax.set_xlim(xs.min(), xs.max())
    ax.set_xticks(xs)
    ax.set_xlabel('area')

    ax.set_ylim(0, df['bytes'].max() * 1.0)
    xs = np.arange(0, df['bytes'].max() + 2.5e8, 2.5e8)
    ax.set_yticks(xs)
    ax.set_yticklabels([
        f'{x / 1e6:.0f}M'
        for x in xs
    ])
    ax.set_ylabel('bytes')

    plt.show()

def plot_areas_and_trees(path1, path2, indicator):
    df1 = pd.read_csv(path1).set_index('area')[indicator]
    df2 = pd.read_csv(path2).set_index(['parent_cycle_start', 'area'])[indicator]

    min_circle_area = 0.8 / 100
    max_circle_area = np.pi * 0.5 ** 2
    yborder = 0.2
    xborder = 0.4
    xshift = 1.3
    yshift = 1.
    fontsize_out=8.5
    fontsize_small=6.5
    fontsize=6.5
    fontstuff = dict(
        fontweight='semibold',
    )
    max_val = df1.max()

    if 'byte' in indicator:
        fmt = fmt_byte_short
    else:
        fmt = fmt_count_short

    figsize = np.asarray([10, 7.5])
    plt.close('all')
    fig, ax = plt.subplots(
        dpi=100,
        subplot_kw=dict(aspect="equal"),
        figsize=figsize,
    )

    areas = list(df1.index)
    commits = sorted(set(df2.reset_index()['parent_cycle_start']))
    xlabs = [f'area {area}' for area in areas]
    ylabs = ['area'] + [f'commit {commit}' for commit in commits]

    xs = xborder + 0.5 + np.arange(len(xlabs)) * xshift
    ys = (yborder + 0.5 + np.arange(len(ylabs)) * yshift)[::-1]

    for x, (area, val) in zip(xs, df1.items()):
        area = val / max_val * max_circle_area
        if area > 0:
            area = max(min_circle_area, area)
        rad = (area / np.pi) ** 0.5
        y = ys[0]
        ax.add_patch(plt.Circle(xy=[x, y], radius=rad, zorder=10))
        ax.text(
            x, y + rad + 0.01, fmt(val),
            horizontalalignment='center', verticalalignment='bottom',
            fontsize=fontsize,
            fontweight='semibold',
            zorder=15,
        )

    for y, commit in zip(ys[1:], commits):
        df = df2.loc[commit]
        for x, area in zip(xs, areas):
            if area not in df.index: continue
            val = df.loc[area]
            circle_area = val / max_val * max_circle_area
            if circle_area > 0:
                circle_area = max(min_circle_area, circle_area)
            rad = (circle_area / np.pi) ** 0.5
            ax.add_patch(plt.Circle(xy=[x, y], radius=rad, zorder=10))

            pct = val / df1.loc[area]
            if pct > 0.04:
                text = f'{pct:.0%}'
            elif pct > 0.003:
                text = f'{pct:.1%}'
            elif pct > 0.0003:
                text = f'{pct:.2%}'
            else:
                text = f'{pct:.3%}'
            if circle_area > 0.35:
                ax.text(
                    x, y + 0.012, text,
                    horizontalalignment='center', verticalalignment='center',
                    fontsize=fontsize_small,
                    zorder=15,
                    color='white',
                    fontweight='semibold',
                )
            else:
                ax.text(
                    x, y - rad - 0.037, text,
                    horizontalalignment='center', verticalalignment='top',
                    fontsize=fontsize_small,
                    zorder=15,
                )


    for axis in ['left', 'top','bottom','right']:
        ax.spines[axis].set_visible(False)

    plt.tick_params(
        top=False, bottom=False, left=False, right=False,
        labelbottom=False,labeltop=True,
        labelleft=False,labelright=True,
    )
    # plt.suptitle(indicator)

    ax.set_xlim(min(xs) - 0.5 - xborder, max(xs) + 0.5 + xborder)
    ax.set_xticks(xs)
    ax.set_xticklabels(xlabs, rotation=45, ha='left', fontsize=fontsize_out)

    ax.set_ylim(min(ys) - 0.5 - yborder, max(ys) + 0.5 + yborder)
    ax.set_yticks(ys)
    ax.set_yticklabels(ylabs, fontsize=fontsize_out)

    plt.tight_layout()
    plt.show()
