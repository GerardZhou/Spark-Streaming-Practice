# Name: Gerard Zhou
import bz2
import csv
import socket
import sys
import time


HOST = "localhost"
PORT = 9009
INTERVAL_SECONDS = 1.0


def main(filename):
    # decompress bz2 csv file
    open_file = bz2.open if filename.lower().endswith(".bz2") else open
    with open_file(filename, "rt", encoding="utf-8-sig", newline="") as source:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((HOST, PORT))
            server.listen(1)
            print(f"Waiting for Spark on {HOST}:{PORT} ...", flush=True)

            connection, address = server.accept()
            with connection:
                print(f"Connected to {address}. Sending one flight per second.",
                      flush=True)
                count = 0
                for line in source:
                    if not line.strip():
                        continue
                    row = next(csv.reader([line]))
                    if len(row) > 4 and row[4].strip().upper() == "AIRLINE":
                        continue  # The CSV header isn't a flight

                    message = line.rstrip("\r\n") + "\n"
                    connection.sendall(message.encode("utf-8"))
                    count += 1
                    if count == 1 or count % 10 == 0:
                        print(f"Sent {count} flights", flush=True)
                    time.sleep(INTERVAL_SECONDS)

                print(f"Finished sending {count} flights. Press Ctrl+C to stop.",
                      flush=True)
                # keeps socket open while Spark processes the last window
                # With no new data, window becomes empty after 60 sec
                while True:
                    time.sleep(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python send_flights_data_from_file.py <csv_or_csv.bz2>",
              file=sys.stderr)
        sys.exit(1)
    try:
        main(sys.argv[1])
    except KeyboardInterrupt:
        print("\nSender stopped.")
    except (BrokenPipeError, ConnectionResetError):
        print("Spark disconnected. Sender stopped.")

