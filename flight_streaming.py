# Name: Gerard Zhou
# Spark Streaming assignment: flight counts, cancellations, and cancellation rates.

"""Run with: spark-submit --master local[2] flight_streaming.py

Start send_flights_data_from_file.py in another terminal first.
The window uses arrival time, not the historical dates in the CSV.
Every 5 seconds, each task prints its top 10 airlines (winner first),
followed by its last 10 window winners, oldest first.
"""

import csv
from collections import deque

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext


HOST = "localhost"
PORT = 9009
BATCH_SECONDS = 5
WINDOW_SECONDS = 60
SLIDE_SECONDS = 5
DISPLAY_LIMIT = 10


def parse_flight(line):
    """Return [(airline, (total, cancelled))], or [] for a header/bad row."""
    try:
        # csv.reader handles quoted fields and empty columns correctly.
        row = next(csv.reader([line], strict=True))
        if len(row) < 25:
            return []

        # Column positions are zero-based in the supplied flights schema.
        airline = row[4].strip().upper()       # AIRLINE
        cancellation = row[24].strip()        # CANCELLED
        if not airline or airline == "AIRLINE":
            return []

        # Accept both integer flags (0/1) and decimal flags (0.0/1.0).
        cancelled = float(cancellation)
        if cancelled not in (0.0, 1.0):
            return []
        return [(airline, (1, int(cancelled)))]
    except (csv.Error, ValueError, StopIteration):
        return []


def add_counts(left, right):
    """Add (total flights, cancelled flights) for the same airline."""
    return left[0] + right[0], left[1] + right[1]


def make_printer(title, percentage=False, cancellations=False):
    """Keep only the 10 most recent answers for one task on the driver."""
    history = deque(maxlen=DISPLAY_LIMIT)

    def format_value(value):
        return f"{value:.2f}%" if percentage else str(value)

    def print_results(batch_time, rdd):
        # Rank with an RDD action. Ties are resolved by airline code.
        # Only the top 10 aggregated rows are brought to the driver.
        top = rdd.takeOrdered(
            DISPLAY_LIMIT, key=lambda item: (-item[1], item[0])
        )
        if not top:
            winner = "No flights in this window"
        elif cancellations and top[0][1] == 0:
            winner = "No cancellations in this window"
        else:
            airline, value = top[0]
            winner = f"{airline}: {format_value(value)}"

        history.append((batch_time, winner))
        print(f"\n{title} | window ending {batch_time}", flush=True)
        print("Top 10 airlines in the last 60 seconds (winner first):")
        for airline, value in top:
            print(f"  {airline}: {format_value(value)}")
        if not top:
            print("  No flights in this window")

        print("Last 10 window winners (oldest to newest):")
        for timestamp, answer in history:
            print(f"  {timestamp} -> {answer}")
        print(flush=True)

    return print_results


def main():
    conf = SparkConf().setAppName("FlightStreaming")
    # The socket receiver needs one thread; processing needs another.
    conf.setIfMissing("spark.master", "local[2]")
    conf.set("spark.ui.showConsoleProgress", "false")