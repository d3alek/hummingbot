HOME=/home/$USER
cd $HOME
# SETUP DEPENDENCIES
# 1) Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential
# 2) Install Miniconda3
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
sh Miniconda3-latest-Linux-x86_64.sh
export CONDAPATH="$HOME/miniconda3"
export PYTHON="$HOME/miniconda3/envs/hummingbot/bin/python3"
export hummingbotPath="$HOME/hummingbot" && cd $hummingbotPath && git checkout funding_rate && ./install
# 5) Activate environment and compile code
${CONDAPATH}/bin/activate hummingbot && ${PYTHON} setup.py build_ext --inplace
# 6) Start Hummingbot
${PYTHON} bin/hummingbot.py
# 7) Update .bashrc to register `conda`
exec bash
