echo "Running workloads for $1"
echo "Loading data"
python benchmark.py load -d $1 -n 100000 -i 2 -r "data/Stocks/"
echo "Running workload-a"
python benchmark.py run -d $1 -n 100000 -i 2 -r data/Stocks/ -w a
echo "Running workload-b"
python benchmark.py run -d $1 -n 100000 -i 2 -r data/Stocks/ -w b
echo "Running workload-c"
python benchmark.py run -d $1 -n 100000 -i 2 -r data/Stocks/ -w c
echo "Running workload-d"
python benchmark.py run -d $1 -n 100000 -i 2 -r data/Stocks/ -w d
echo "Running workload-e"
python benchmark.py run -d $1 -n 100000 -i 2 -r data/Stocks/ -w e
echo "Finished executing"
