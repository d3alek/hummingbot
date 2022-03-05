export CONDAPATH="/home/alek/miniconda3"
export PYTHON="${CONDAPATH}/envs/hummingbot/bin/python3"
${CONDAPATH}/bin/activate hummingbot
${PYTHON} -m unittest test.hummingbot.connector.derivative.ftx_perpetual.test_ftx_exchange
