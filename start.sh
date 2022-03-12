export CONDAPATH="/home/$USER/miniconda3"
export PYTHON="${CONDAPATH}/envs/hummingbot/bin/python3"
${CONDAPATH}/bin/activate hummingbot
${PYTHON} bin/hummingbot.py
