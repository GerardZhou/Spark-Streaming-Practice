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
        airline = row[4].strip().upper() # AIRLINE
        cancellation = row[24].strip() # CANCELLED
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
    print(f"Starting flight streaming: {__file__}", flush=True)

    conf = SparkConf().setAppName("FlightStreaming")
    # The socket receiver needs one thread; processing needs another.
    conf.setIfMissing("spark.master", "local[2]")
    conf.set("spark.ui.showConsoleProgress", "false")

    print("Creating SparkContext ...", flush=True)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, BATCH_SECONDS)
    lines = ssc.socketTextStream(HOST, PORT)
    flights = lines.flatMap(parse_flight)

    # sum (total flights, cancelled flights) for each airline
    # over a 60-second window, updated every 5 seconds.
    counts = flights.reduceByKeyAndWindow(
        add_counts,
        invFunc=None,
        windowDuration=WINDOW_SECONDS,
        slideDuration=SLIDE_SECONDS,
        numPartitions=2,
    ).cache()

    # Task 1: most flights in the last minute
    totals = counts.mapValues(lambda value: value[0])
    totals.foreachRDD(make_printer("Task 1: Most flights"))

    # Task 2: most cancellations in the last minute
    cancelled = counts.mapValues(lambda value: value[1])
    cancelled.foreachRDD(
        make_printer(
            "Task 2: Most cancellations",
            cancellations=True,
        )
    )

    # Task 3: cancellation percentage for each airline
    rates = counts.mapValues(
        lambda value: 100.0 * value[1] / value[0]
    )
    rates.foreachRDD(
        make_printer(
            "Task 3: Highest cancellation percentage",
            percentage=True,
            cancellations=True,
        )
    )

    try:
        ssc.start()
        print(
            "Streaming started. Results update every 5 seconds.",
            flush=True,
        )
        ssc.awaitTermination()
    except KeyboardInterrupt:
        ssc.stop(stopSparkContext=True, stopGraceFully=False)


if __name__ == "__main__":
    main()