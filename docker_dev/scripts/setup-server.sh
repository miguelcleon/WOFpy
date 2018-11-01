#!/usr/bin/env bash

URL="http://bit.ly/miniconda"

if [ ! -f $HOME/miniconda/bin/conda ] ; then
    echo "Fresh miniconda installation."
    wget $URL -O miniconda.sh
    rm -rf $HOME/miniconda
    bash miniconda.sh -b -p $HOME/miniconda
fi

export PATH="$HOME/miniconda/bin:$PATH"

conda config --set always_yes yes --set changeps1 no --set show_channel_urls true
conda update conda
conda config --set show_channel_urls true
conda config --add channels conda-forge --force
conda create -n wofpy python=$PYTHON_VERSION --file requirements.txt --file requirements-dev.txt

source activate wofpy
conda install --yes -c conda-forge mysql-python

if [[ "$_PYTHON_VERSION" == "2.7" ]]; then
    conda install configparser ;
fi