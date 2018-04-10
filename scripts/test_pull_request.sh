#!/bin/bash

SPARK_HOME=/home/oap/spark/spark-2.1.0-bin-hadoop2.7
jarPath=/home/oap/oap
OAP_HOME=/home/oap
scriptName=$(basename $BASH_SOURCE)
workDir=`cd ${BASH_SOURCE%/*}; pwd`
suites="Suite"
declare -A prToCommits
# source environment variables and check requirements

function sourceEnv {
    [ -f $OAP_HOME/.bash_profile ] && . $OAP_HOME/.bash_profile
    [ -f $OAP_HOME/.bash_login ] && . $OAP_HOME/.bash_login
    [ -f $OAP_HOME/.profile ] && . $OAP_HOME/.profile
    [ -f $OAP_HOME/.bashrc ] && . $OAP_HOME/.bashrc
    if ! type git >/dev/null 2>&1; then
        echo "Git not installed, pls install it first!" >&3 && return 1
    fi
    if ! type sbt >/dev/null 2>&1; then
        echo "SBT not installed, pls install it first!" >&3 && return 1
    fi
    if ! type mvn >/dev/null 2>&1; then
        echo "Maven not installed, pls install it first!" >&3 && return 1
    fi
}

function cloneAndBuild {
    cd $workDir
    # git clone OAP and oap-perf-suite
    if ! [ -d OAP ]; then
        git clone -q https://github.com/Intel-bigdata/OAP.git
    else
        cd OAP && git pull origin master && cd ..
    fi
    if ! [ -d oap-perf-suite ]; then
        git clone -q https://github.com/yueawang/oap-perf-suite.git
    else
        cd oap-perf-suite && git pull origin master && cd ..
    fi
    if ! [ -d OAP ] || ! [ -d oap-perf-suite ]; then
        echo "Git clone OAP or oap-perf-suite fails!" >&3 && return 1
    fi
    # compile oap-perf-suite
    cd ${workDir}/oap-perf-suite
    if ! sbt -Dsbt.log.noformat=true --error assembly 1>&3 2>&3; then return 1; fi
}

function genData {
    # gen data and metestore_db
    cd $workDir
    if [ -d metastore_db ]; then return 0; fi
    [ -d OAP ] && rm -rf ./OAP
    [ -d oap-perf-suite ] && rm -rf ./oap-perf-suite
    cloneAndBuild
    if [ "$?" -ne 0 ]; then return 1; fi
    cd $workDir
    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class org.apache.spark.sql.OapPerfSuite \
    ${workDir}/oap-perf-suite/target/scala-2.11/oap-perf-suite-assembly-1.0.jar \
    -d
}

function compileOapAndRun {
    cd ${workDir}/OAP
    if ! mvn -q -DskipTests package 1>&3 2>&3; then return 1; fi
    versionNum=$(grep -o "<version>.*</version>" pom.xml | head -n 1 | sed "s/<[a-z/]*>//g")
    ln -f target/oap-${versionNum}.jar ${jarPath}/oap.jar
    cd ${workDir}
    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class org.apache.spark.sql.OapPerfSuite \
    ${workDir}/oap-perf-suite/target/scala-2.11/oap-perf-suite-assembly-1.0.jar \
    -r 5 \
    -s ${suites} \
    >./testres_$1 \
    2>&3
}
# create task directory and run periodic task
function runTask {
    cloneAndBuild
    if [ "$?" -ne 0 ]; then return 1; fi
    for pr in ${prList[@]}; do
        # git checkout oap commit
        cd ${workDir}/OAP && git fetch origin pull/${pr}/head:${pr}
        if [ "$?" -ne 0 ]; then
            echo "$pr is not valid pr number and passsed!"
            continue
        fi
        git checkout ${pr}
        echo `git rev-parse HEAD`
        compileOapAndRun ${pr}
        cd ${workDir}/OAP && git checkout master && git branch -D ${pr}
    done
    for pr in ${!prToCommits[@]}; do
        commits=${prToCommits[$pr]}
        commitList=(`echo ${commits#*:} | sed "s/,/ /g"`)
        cd ${workDir}/OAP && git fetch origin pull/${pr}/head:${pr}
        if [ "$?" -ne 0 ]; then
            echo "$pr is not valid pr number and passed!"
            continue
        fi
        cd ${workDir}/OAP && git checkout ${pr}
        curCommit=`git rev-parse HEAD`
        for commit in ${commitList[@]}; do
            cd ${workDir}/OAP && git reset --hard $commit
            if [ "$?" -ne 0 ]; then
                echo "$commit is not included in $pr pull request!"
                continue
            fi
            echo `git rev-parse HEAD`
            compileOapAndRun "${pr}_${commit}"
        done
        cd ${workDir}/OAP && git checkout master && git branch -D ${pr}
    done
}

function main {
    exec 3>/dev/null
    exec 3>&1
    sourceEnv
    if [ "$?" -ne 0 ]; then exit 1; fi
    while [ -n "$1" ]; do
        case "$1" in
            -p)
                shift
                if [ -n "$1" ]; then
                    prList=(${1//,/ })
                else
                    echo "Pls enter pr numbers, sep by domma!"
                    exit 1
                fi
                ;;
            -c)
                shift
                if [ -n "$1" ]; then
                    if echo "$1" | grep -Eq "^[[:digit:]]+:([a-zA-Z0-9]+,)*([a-zA-Z0-9])+$"; then
                        prNum=${1%%:*}
                        prToCommits[$prNum]=${1#*:}
                    fi
                else 
                    echo "Pls enter commit ids, sep by domma!"
                    exit 1
                fi
                ;;
            -s)
                shift
                if [ -n "$1" ]; then
                    suites="$1"
                else
                    echo "No suites assigned, default run all suites!"
                fi
                ;;
            *)
                echo "$1 is an invalid option!" && exit 1
                ;;
            esac
        shift
    done
    genData && runTask
}

if [ -n "$1" ]; then
    main "$@"
fi
