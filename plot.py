import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import numpy as np
import json


def sizeof_fmt(x, pos):
    if x<0:
        return ""
    for x_unit in ['bytes', 'kB', 'MB', 'GB', 'TB']:
        if x < 1024.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1024.0


def moving_average(x, w):
    return np.convolve(x, np.ones(w), 'valid') / w


def plot_io(fig_name, runs, title, datapoints=60, smooth=False, derivative=False):

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.set(xlabel='Rows written', ylabel='Disk writes (B)')

    for name, label, color, *other in runs:
        with open(f"results/{name}.json", "r") as f:
            data = json.load(f)

        rows_points = data["rows_written"][:datapoints]

        time_elapsed = data["time_elapsed"][:datapoints]

        io_points = data["io_write"][:datapoints]

        if smooth:
            io_points = moving_average(io_points, 7)
            rows_points = rows_points[3:-3]
            time_elapsed = time_elapsed[3:-3]

        if derivative:
            rows_diff = np.diff(rows_points)
            rows_points = rows_points[:-1]
            time_elapsed = time_elapsed[:-1]
            io_points = np.diff(io_points) / rows_diff

        # # ax2.set_xlim(ax.get_xlim())
        # ax2.set_xticks(time_elapsed)
        # # ax2.set_xticklabels(tick_function(new_tick_locations))
        # ax2.set_xlabel(r"Elapsed time:")
        kwargs = dict(label=label, color=color, linewidth=1.6)
        if other:
            kwargs = {**kwargs, **other[0]}

        ax.plot(rows_points, io_points, **kwargs)

    ax.grid()
    ax.yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
    leg = ax.legend()

    # change the line width for the legend
    for line in leg.get_lines():
        line.set_linewidth(4.0)
    plt.title(title)

    fig.savefig(f"figs/{fig_name}.png")
    # plt.tight_layout()
    plt.show()

def plot_bar(fig_name, runs, title, datapoints=60):

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.set(xlabel='Rows written', ylabel='Disk writes (B)')

    plt_labels = []
    plt_bytes = []
    plt_colors = []

    for name, label, color, *other in runs:
        with open(f"results/{name}.json", "r") as f:
            data = json.load(f)

        io_points = data["io_write"][:datapoints]
        total_io = io_points[-1]

        plt_labels.append(label)
        plt_bytes.append(total_io)
        plt_colors.append(color)

    ax.bar(plt_labels, plt_bytes, color=plt_colors)

    # ax.grid()
    ax.yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))

    for index, value in enumerate(plt_bytes):
        ax.text(index - 0.18, value + max(plt_bytes) / 100, sizeof_fmt(value, 0))

    plt.title(title)
    fig.savefig(f"figs/{fig_name}.png")
    # plt.tight_layout()
    plt.show()


plot_io(
"sqlite_clustered",
[
       ("sqlite_clustered_int64_random", "Random Int64", "tab:red"),
       ("sqlite_clustered_int64_sequential", "Sequential Int64", "orange"),
], title="Written bytes to disk (cumulative), clustered index")

plot_io(
"sqlite_clustered_differenciated",
[
       ("sqlite_clustered_int64_random", "Random Int64", "tab:red"),
       ("sqlite_clustered_int64_sequential", "Sequential Int64", "orange"),
], title="Written bytes to disk (per row), clustered index", smooth=True, derivative=True, datapoints=500)



plot_io(
"sqlite_nonclustered",
[
       ("sqlite_nonclustered_int64_random", "Random Int64", "tab:blue"),
       ("sqlite_nonclustered_int64_sequential", "Sequential Int64", "tab:green"),
], title="Written bytes to disk (cumulative), non-clustered index")


plot_io(
"sqlite_clustered_nonclustered",
[
       ("sqlite_nonclustered_int64_random", "Non-clustered, Random Int64", "tab:blue"),
       ("sqlite_nonclustered_int64_sequential", "Non-clustered, Sequential Int64", "tab:green"),
       ("sqlite_clustered_int64_random", "Clustered, Random Int64", "tab:red"),
       ("sqlite_clustered_int64_sequential", "Clustered, Sequential Int64", "orange"),
], title="Written bytes to disk (cumulative), clustered vs. non-clustered index")

plot_io(
"mariadb_writes",
[
       ("mariadb_random", "Random Int64", (0.9, 0.62, 0)),
       ("mariadb_sequential", "Sequential Int64", (0, 0.44, 0.69)),
], title="Written bytes to disk (cumulative), MariaDB (InnoDB)")

plot_io(
"mariadb_differenciated",
[
       ("mariadb_random", "Random Int64", (0.9, 0.62, 0)),
       ("mariadb_sequential", "Sequential Int64", (0, 0.44, 0.69)),
], title="Written bytes to disk (per row), MariaDB (InnoDB)", smooth=True, derivative=True, datapoints=500)

plot_io(
"postgres_writes_short",
[
       ("postgres_random", "Random Int64", "tab:blue"),
       ("postgres_sequential", "Sequential Int64", "tab:green"),
], title="Written bytes to disk (cumulative), PostgreSQL")

plot_io(
"postgres_writes_long",
[
       ("postgres_random", "Random Int64", "tab:blue"),
       ("postgres_sequential", "Sequential Int64", "tab:green"),
], title="Written bytes to disk (cumulative), PostgreSQL", datapoints=500)

plot_io(
"postgres_differenciated",
[
       ("postgres_random", "Random Int64", "tab:blue"),
       ("postgres_sequential", "Sequential Int64", "tab:green"),
], title="Written bytes to disk (per row), PostgreSQL", smooth=True, derivative=True, datapoints=500)


plot_io(
"sqlite_clustered_uuid1_uuid4",
[
       ("sqlite_clustered_uuid1", "UUID1", "black"),
       ("sqlite_clustered_uuid4", "UUID4", "tab:blue"),

], title="Written bytes to disk (cumulative), SQLite, clustered index", datapoints=500)

plot_io(
"sqlite_clustered_uuid1_fastrollover_uuid4",
[
       ("sqlite_clustered_uuid1", "UUID1", "black"),
       ("sqlite_clustered_uuid1_fast_rollover", "UUID1_fastrollover", "tab:red"),
       ("sqlite_clustered_uuid4", "UUID4", "tab:blue"),

], title="Written bytes to disk (cumulative), SQLite, clustered index", datapoints=500)


plot_io(
"sqlite_clustered_uuid",
[
       ("sqlite_clustered_uuid1_fast_rollover", "UUID1_fastrollover", "tab:red"),
       ("sqlite_clustered_uuid4", "UUID4", "tab:blue"),
       ("sqlite_clustered_uuid6", "UUID6", "tab:green"),
       ("sqlite_clustered_uuid7", "UUID7", "tab:orange"),


], title="Written bytes to disk (cumulative), SQLite, clustered index", datapoints=500)


plot_io(
"mariadb_uuid",
[
       ("mariadb_uuid1_fast_rollover", "UUID1_fastrollover", "tab:red"),
       ("mariadb_uuid4", "UUID4", "tab:blue"),
       ("mariadb_uuid7", "UUID7", "tab:orange", { "linewidth": 3}),  # uuid6 and uuid7 are identical
       ("mariadb_uuid6", "UUID6", "tab:green", {"linestyle": ":", "linewidth": 3}),
], title="Written bytes to disk (cumulative), MariaDB, clustered index", datapoints=1000)


plot_bar("mariadb_uuid_totals",
[
    ("mariadb_uuid1_fast_rollover", "UUID1_fastrollover", "tab:red"),
    ("mariadb_uuid4", "UUID4", "tab:blue"),
    ("mariadb_uuid6", "UUID6", "tab:green"),
    ("mariadb_uuid7", "UUID7", "tab:orange"),
],
title="Total written bytes after inserting 1M records, MariaDB, clustered index", datapoints=1000)


plot_io(
"postgres_uuid",
[
       ("postgres_uuid1_fast_rollover", "UUID1_fastrollover", "tab:red"),
       ("postgres_uuid4", "UUID4", "tab:blue"),
       ("postgres_uuid6", "UUID6", "tab:green"),
       ("postgres_uuid7", "UUID7", "tab:orange"),


], title="Written bytes to disk (cumulative), PostgreSQL, non-clustered index", datapoints=1000)


plot_bar("postgres_uuid_totals",
[
    ("postgres_uuid1_fast_rollover", "UUID1_fastrollover", "tab:red"),
    ("postgres_uuid4", "UUID4", "tab:blue"),
    ("postgres_uuid6", "UUID6", "tab:green"),
    ("postgres_uuid7", "UUID7", "tab:orange"),
],
title="Total written bytes after inserting 1M records, PostgreSQL, non-clustered index", datapoints=1000)
