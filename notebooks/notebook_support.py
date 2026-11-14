"""Presentation helpers for the bounded public-feed investigation."""

from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import Markdown, display


def theme() -> None:
    """Set the shared chart theme."""
    sns.set_theme(style="whitegrid", context="notebook", font="DejaVu Sans")
    plt.rcParams.update(
        {
            "figure.dpi": 130,
            "text.parse_math": False,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "grid.color": "#e3e9e5",
        }
    )


def prose(text: str) -> None:
    """Render result-derived Markdown."""
    display(Markdown(text))


def finish(fig: plt.Figure, title: str, subtitle: str, source: str, filename: str) -> None:
    """Display a chart and save a copy for review."""
    fig.suptitle(title, x=0.08, y=0.98, ha="left", fontsize=18, weight="bold", color="#253d31")
    fig.text(0.08, 0.91, subtitle, fontsize=11, color="#52675a")
    fig.text(
        0.08,
        0.025,
        f"Source: {source} · real vgi-bluesky / Haybarn",
        fontsize=9,
        color="#52675a",
    )
    fig.subplots_adjust(left=0.18, right=0.95, bottom=0.16, top=0.82)
    directory = Path("validation/charts")
    directory.mkdir(parents=True, exist_ok=True)
    fig.savefig(directory / filename, dpi=150)
    plt.show()
