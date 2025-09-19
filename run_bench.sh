# Installing dependencies if they are missing
sudo apt install gcc

if [ ! -d .venv ]; then
  virtualenv .venv
  source .venv/bin/activate
  pip install pandas
  pip install matplotlib
  pip install seaborn
else
  source .venv/bin/activate
fi

# Building benchmark
g++ seq_read_bench.cpp -o seq_read_bench
chmod +x ./seq_read_bench

# Launching the benchmark
python3 seq_read_bench.py
