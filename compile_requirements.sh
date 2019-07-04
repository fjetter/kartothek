#!/bin/bash
set -xeo pipefail

productionIndex=platform
developmentIndex=platform_dev

touch kartothek_env_reqs.txt
trap 'rm -f kartothek_env_reqs.txt' EXIT


if [ ! -z ${KARTOTHEK_ARROW_VERSION} ];
then
    echo pyarrow==$KARTOTHEK_ARROW_VERSION >> kartothek_env_reqs.txt
fi

if [ ! -z ${KARTOTHEK_PANDAS_VERSION} ];
then
    wget https://github.com/pandas-dev/pandas/releases/download/v0.25.0rc0/pandas-0.25.0rc0.tar.gz
    tar -xf pandas-0.25.0rc0.tar.gz
    pushd pandas-0.25.0rc0
    python setup.py bdist_wheel

    cp ${PWD}/dist/pandas-0.25.0rc0-cp36-cp36m-linux_x86_64.whl ${PWD}/..
    popd
    rm -rf pandas-0.25.0rc0
    echo ${PWD}/pandas-0.25.0rc0-cp36-cp36m-linux_x86_64.whl >> kartothek_env_reqs.txt
fi


pip-compile \
    --upgrade \
    --no-index \
    -o requirements-pinned.txt \
    kartothek_env_reqs.txt \
    requirements.txt

pip-compile \
    --upgrade \
    --no-index \
    -o test-requirements-pinned.txt \
    requirements-pinned.txt \
    test-requirements.txt
