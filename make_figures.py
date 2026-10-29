"""Create the report figures from results.csv (run after run_all.py).

Usage: python make_figures.py   -> figures/throughput.png, figures/behaviour.png (300 dpi)
"""
import csv
import os

import matplotlib

matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt

PUBLISH_COLOR, CONSUME_COLOR = "#2a78d6", "#eb6834"  # colour-blind-safe blue / orange
TEXT_COLOR, MUTED_COLOR = "#0b0b0b", "#52514e"
# Sized to stay readable when placed at half-page width (~8 cm)
plt.rcParams.update({"font.size": 12, "font.family": "DejaVu Sans", "text.color": TEXT_COLOR,
                     "axes.labelcolor": TEXT_COLOR, "xtick.color": MUTED_COLOR,
                     "ytick.color": TEXT_COLOR})


def load_results(path="results.csv"):
    """Return {metric: {column: value}} from results.csv."""
    with open(path) as csv_file:
        return {row["Metric"]: row for row in csv.DictReader(csv_file)}


def throughput_figure(results):
    """Grouped horizontal bars: publish and consume msg/s per queue type / protocol."""
    bars = [  # (label, publish row, consume row, column)
        ("Classic", "Test 3: publish rate (msg/s)", "Test 3: consume rate (msg/s)", "Classic"),
        ("Quorum", "Test 3: publish rate (msg/s)", "Test 3: consume rate (msg/s)", "Quorum"),
        ("Stream (AMQP)", "Test 3: publish rate (msg/s)", "Test 3: consume rate (msg/s)", "Stream"),
        ("Stream (stream protocol)", "Test 3b: stream protocol, publish WITH confirms (msg/s)",
         "Test 3b: stream protocol, consume (msg/s)", "Stream"),
        ("Kafka", "Test 3: publish rate (msg/s)", "Test 3: consume rate (msg/s)", "Kafka"),
    ]
    labels = [bar[0] for bar in bars]
    publish = [float(results[bar[1]][bar[3]]) for bar in bars]
    consume = [float(results[bar[2]][bar[3]]) for bar in bars]

    figure, axis = plt.subplots(figsize=(7.5, 4.6))
    positions = range(len(labels))
    height = 0.38
    publish_y = [y - height / 2 for y in positions]
    consume_y = [y + height / 2 for y in positions]
    axis.barh(publish_y, publish, height=height - 0.04, color=PUBLISH_COLOR, label="Publish (confirmed)")
    axis.barh(consume_y, consume, height=height - 0.04, color=CONSUME_COLOR, label="Consume")
    for y_values, values in ((publish_y, publish), (consume_y, consume)):
        for y, value in zip(y_values, values):
            axis.text(value + max(consume) * 0.01, y, f"{value:,.0f}", va="center", fontsize=10.5)

    axis.set_yticks(list(positions), labels)
    axis.invert_yaxis()  # first target at the top
    axis.set_xlim(0, max(publish + consume) * 1.2)
    axis.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    axis.set_xlabel("messages per second (average of 3 runs)")
    axis.set_title("Throughput by queue type and protocol", fontweight="bold", loc="left")
    axis.legend(frameon=False, loc="upper right")
    axis.tick_params(axis="y", length=0)
    for side in ("top", "right", "left"):
        axis.spines[side].set_visible(False)
    axis.spines["bottom"].set_color(MUTED_COLOR)
    figure.tight_layout()
    figure.savefig("figures/throughput.png", dpi=300)
    plt.close(figure)


def behaviour_figure(results):
    """Table-style grid for tests 1, 2 and 4 with a ✓/✗ symbol AND the number in each cell."""
    columns = ["Classic", "Quorum", "Stream", "Kafka"]
    rows = [
        ("Read twice\n(consumer B, of 1,000)", "Test 1: consumer B received"),
        ("Replay\n(of 1,000)", "Test 2: replay possible / messages replayed"),
        ("Readable during\nnode failure (of 1,000)", "Test 4: readable during node failure"),
        ("Readable after\nrestart (of 1,000)", "Test 4: readable after node restart"),
    ]

    figure, axis = plt.subplots(figsize=(7.5, 3.9))
    axis.set_xlim(-1.9, len(columns))
    axis.set_ylim(len(rows), -0.8)
    axis.axis("off")
    for column_index, column in enumerate(columns):
        axis.text(column_index + 0.5, -0.4, column, ha="center", va="center", fontweight="bold")
    for row_index, (row_label, metric) in enumerate(rows):
        axis.text(-1.85, row_index + 0.5, row_label, ha="left", va="center")
        for column_index, column in enumerate(columns):
            raw = results.get(metric, {}).get(column, "not tested")
            if raw in ("not tested", "", None):
                text, fill = "not tested", "#f0efec"
            else:
                count = int(raw.split("/")[-1])  # "Yes / 1000" -> 1000, "0" -> 0
                text, fill = (f"✓  {count:,}", "#dbe9fa") if count else (f"✗  {count}", "#f0efec")
            axis.add_patch(plt.Rectangle((column_index + 0.04, row_index + 0.06), 0.92, 0.88,
                                         color=fill, linewidth=0))
            axis.text(column_index + 0.5, row_index + 0.5, text, ha="center", va="center",
                      fontsize=13 if text != "not tested" else 11,
                      color=TEXT_COLOR if text != "not tested" else MUTED_COLOR)
    axis.set_title("Consumption semantics and failure behaviour (tests 1, 2, 4)",
                   fontweight="bold", loc="left")
    figure.text(0.01, 0.02, "✓ = messages available (count shown)   ✗ = none available",
                fontsize=10, color=MUTED_COLOR)
    figure.tight_layout()
    figure.savefig("figures/behaviour.png", dpi=300)
    plt.close(figure)


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs("figures", exist_ok=True)
    results = load_results()
    throughput_figure(results)
    behaviour_figure(results)
    print("Saved figures/throughput.png and figures/behaviour.png")


if __name__ == "__main__":
    main()
