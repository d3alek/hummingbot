export CONDAPATH="/home/alek/miniconda3"
export PYTHON="${CONDAPATH}/envs/hummingbot/bin/python3"
${CONDAPATH}/bin/activate hummingbot && ${PYTHON} setup.py build_ext --inplace
