import argparse
from pathlib import Path
import time
import threading
import pandas as pd
import random
import config

parser = argparse.ArgumentParser()
parser.add_argument("operation", help="either of load or run")
parser.add_argument("-d", "--database", default="postgres",
                    help="Type of database (postgres, mysql, dynamodb)")
parser.add_argument("-r", "--root", default="date/Stocks",
                    help="Root data directory")
parser.add_argument("-n", "--noperations", default="10000",
                    help="Number of operations to run")
parser.add_argument("-i", "--interval", default="10",
                    help="Store the statistics after these many seconds")
parser.add_argument("-w", "--workload", default="a",
                    help="Type of workload (a, b, c, d)")
parser.add_argument("-v", "--verbose", action="store_true",
                    help="To enable debug")
args = parser.parse_args()
print(args)

nor = int(args.noperations)
if nor < 1:
    print("Number of operations to perform should be a positive number")
    exit()

data_dir = Path(args.root)
interval = float(args.interval)
debug = args.verbose

db = None
max_latency = 0
min_latency = 10000
avg_latency = 0
total_latency = 0
nop = 0
tnop = 0
print_time = start_time = time.time()
exit_thread = False
stats = pd.DataFrame(columns=["Count", "Throughput", "TotalLatency", "MaxLatency", "MinLatency", "AvgLatency", "Total"])


def reset_values():
    global nop, max_latency, min_latency, avg_latency, total_latency, print_time
    max_latency = 0
    min_latency = 10000
    avg_latency = 0
    total_latency = 0
    nop = 0
    print_time = time.time()


def store_stats(interval_):
    global nop, max_latency, min_latency, avg_latency, total_latency, tnop, exit_thread, stats
    new_row = {"Count": nop, "Throughput": (nop / interval_), "TotalLatency": total_latency,
               "MaxLatency": max_latency, "MinLatency": min_latency, "AvgLatency": avg_latency, "Total": tnop}
    stats = stats.append(new_row, ignore_index=True)
    print(f"Count: {nop} operations | Throughput: {nop / interval_} current ops/sec |  "
          f"Latency: {total_latency} (ms) - MAX: {max_latency} (ms) - "
          f"MIN: {min_latency} (ms) - AVG: {avg_latency} (ms) | [Total: {tnop}]")


def print_stats():
    global nop, max_latency, min_latency, avg_latency, total_latency, tnop, interval, exit_thread
    thread = threading.Timer(interval, print_stats)
    thread.start()
    if exit_thread:
        thread.cancel()
    else:
        store_stats(interval)
    reset_values()


def create_random_operations(length_, inserts_, reads_, updates_):
    arr = []
    arr.extend(["insert" for i in range(int(length_ * inserts_))])
    arr.extend(["read" for i in range(int(length_ * reads_))])
    arr.extend(["update" for i in range(int(length_ * updates_))])

    if len(arr) != length_:
        if inserts_ != 0.0:
            arr.extend(["insert" for i in range(length_ - len(arr))])
        elif reads_ != 0.0:
            arr.extend(["read" for i in range(length_ - len(arr))])
        else:
            arr.extend(["update" for i in range(length_ - len(arr))])

    random.Random(310894).shuffle(arr)
    return arr


def get_new_row(data_dir_, type_):
    for symbol_ in Path(data_dir_ / type_).iterdir():
        df_ = pd.read_csv(symbol_)
        for index_, row_ in df_.iterrows():
            yield str(symbol_.name).split('.us.txt')[0], row_


def perform_operation(db_, type_, data_):
    global max_latency, min_latency, total_latency, avg_latency, debug
    start_of_query = time.time() * 1000
    symbol_, row_ = data_

    if debug:
        print(f"Performing {type_} for {symbol_, row_['Date']}")

    if type_ == "insert":
        db_.insert(symbol_, row_['Date'], row_['Open'], row_['High'], row_['Low'], row_['Close'], row_['Volume'])
    elif type_ == "read":
        db_.read(symbol_, row_['Date'])
    else:
        db_.update(symbol_, row_['Date'], 121.22, 143.4444, 113.1, 133.333, 1234567890)

    end_of_query = time.time() * 1000 - start_of_query
    if end_of_query > max_latency:
        max_latency = end_of_query
    if end_of_query < min_latency:
        min_latency = end_of_query
    total_latency += end_of_query


def load_database(db_, data_dir_):
    global nop, tnop, avg_latency
    load_gen = get_new_row(data_dir_=data_dir_, type_="load")
    while True:
        perform_operation(db_=db_, type_="insert", data_=next(load_gen))
        nop += 1
        avg_latency = total_latency / nop
        tnop += 1
        if tnop >= nor:
            break


def process_operations(db_, data_dir_, ops):
    global nop, tnop, avg_latency
    insert_gen = get_new_row(data_dir_=data_dir_, type_="insert")
    ru_gen = get_new_row(data_dir_=data_dir_, type_="load")
    for op in ops:
        if op == "insert":
            perform_operation(db_=db_, type_=op, data_=next(insert_gen))
        else:
            perform_operation(db_=db_, type_=op, data_=next(ru_gen))
        nop += 1
        avg_latency = total_latency / nop
        tnop += 1


if args.database == "postgres" or args.database == "rds-postgres" or args.database == "aurora-postgres":
    from db_postgres import PostgresDB

    db = PostgresDB(
        host=config.conf[args.database]["host"],
        port=config.conf[args.database]["port"],
        user=config.conf[args.database]["user"],
        password=config.conf[args.database]["password"],
        database=config.conf[args.database]["database"]
    )
elif args.database == "mysql" or args.database == "rds-mysql" or args.database == "aurora-mysql":
    from db_mysql import MySqlDB

    db = MySqlDB(
        host=config.conf[args.database]["host"],
        port=config.conf[args.database]["port"],
        user=config.conf[args.database]["user"],
        password=config.conf[args.database]["password"],
        database=config.conf[args.database]["database"]
    )
elif args.database == "dynamodb":
    from db_dynamo import DynamoDB

    db = DynamoDB(
        region=config.conf[args.database]["region"]
    )
elif args.database == "mongodb" or args.database == "documentdb":
    from db_mongo import MongoDB

    db = MongoDB(
        host=config.conf[args.database]["host"],
        port=config.conf[args.database]["port"],
        user=config.conf[args.database]["user"],
        password=config.conf[args.database]["password"],
        database=config.conf[args.database]["database"]
    )
else:
    print("Database type not supported")
    exit()

if args.operation == "load":
    db.create_table()
    print_stats()
    start_time = time.time()
    load_database(db_=db, data_dir_=data_dir)
    with open(f"results/{args.database}/{args.database}_{args.operation}_{nor}_{interval}.size", "w+") as f:
        if args.database == "dynamo" and db.get_size() == 111:
            f.write(str(db.get_size()*nor/(1024*1024))+"MB")
        else:
            f.write(str(db.get_size())+"MB")
elif args.operation == "run":
    db.create_table()
    load_database(db_=db, data_dir_=data_dir)
    # start the timer after loading the data
    reset_values()
    tnop = 0
    print_stats()
    start_time = time.time()
    if args.workload == "a":
        process_operations(db_=db, data_dir_=data_dir,
                           ops=create_random_operations(nor, inserts_=0.0, reads_=0.95, updates_=0.05))
    elif args.workload == "b":
        process_operations(db_=db, data_dir_=data_dir,
                           ops=create_random_operations(nor, inserts_=0.0, reads_=0.5, updates_=0.5))
    elif args.workload == "c":
        process_operations(db_=db, data_dir_=data_dir,
                           ops=create_random_operations(nor, inserts_=0.5, reads_=0.5, updates_=0.0))
    elif args.workload == "d":
        process_operations(db_=db, data_dir_=data_dir,
                           ops=create_random_operations(nor, inserts_=0.5, reads_=0.0, updates_=0.5))
    elif args.workload == "e":
        process_operations(db_=db, data_dir_=data_dir,
                           ops=create_random_operations(nor, inserts_=0.33, reads_=0.33, updates_=0.33))
    else:
        print("Please choose a valid workload (a, b, c, d, e)")
else:
    print("Please choose a valid operation (load, run)")

store_stats(time.time() - print_time)
end_time = time.time() - start_time
print("Total runtime:", end_time)
print("Exiting...")
print(stats)

stats.to_csv(f"results/{args.database}/{args.database}_{args.operation}_{args.workload}_{nor}_{interval}.csv", index=False)
db.close()
exit_thread = True
