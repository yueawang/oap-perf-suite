#!/bin/bash

SPARK_HOME=/home/oap/spark/spark-2.1.0-bin-hadoop2.7
jarPath=/home/oap/oap
OAP_HOME=/home/oap
scriptName=$(basename $BASH_SOURCE)
workDir=`cd ${BASH_SOURCE%/*}; pwd`
mailList=(yang2.xu@intel.com yue.a.wang@intel.com carson.wang@intel.com hao.cheng@intel.com daoyuan.wang@intel.com linhong.liu@intel.com lei.l.li@intel.com chenzhao.guo@intel.com yi.cui@intel.com)

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

# check if task exists in crontab; otherwise create it
function checkCrontab {
    scriptPath=${workDir}/${scriptName}
    if ! crontab -l 2>&1 | grep -q "$scriptName"; then
        if crontab -l 2>&1 | grep -q "no crontab for"; then
            echo "30 2 * * * ${scriptPath} -c" | crontab -
        else
            (crontab -l; echo "30 2 * * * ${scriptPath} -c") | crontab -
        fi
    fi
}

function cloneAndBuild {
    # git clone OAP and oap-perf-suite
    git clone -q https://github.com/Intel-bigdata/OAP.git
    git clone -q https://github.com/yueawang/oap-perf-suite.git
    if ! [ -d OAP ] || ! [ -d oap-perf-suite ]; then
        echo "Git clone OAP or oap-perf-suite fails!" >&3 && return 1
    fi
    # compile OAP, parse its versionNum and copy it to jar path
    cd OAP
    if ! mvn -q -DskipTests package 1>&3 2>&3; then return 1; fi
    versionNum=$(grep -o "<version>.*</version>" pom.xml | head -n 1 | sed "s/<[a-z/]*>//g")
    ln -f target/oap-${versionNum}.jar ${jarPath}/oap.jar
    # compile oap-perf-suite
    cd ../oap-perf-suite
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

# create task directory and run periodic task
function runTask {
    cloneAndBuild
    if [ "$?" -ne 0 ]; then return 1; fi
    # copy hive database meta file to current directory
    cd ${workDir}/${resDir} && cp -r ../metastore_db ./
    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class org.apache.spark.sql.OapPerfSuite \
    ./oap-perf-suite/target/scala-2.11/oap-perf-suite-assembly-1.0.jar \
    -r 5 \
    1>./testres_${today} \
    2>&3
}

function main {
    # use -c option to check if called by cron or bash
    if [ -n "$1" ] && [ "$1" = "-c" ]; then
        cd $workDir
        today=$(date +%Y_%m_%d)
        mailTitle="Oap Benchmark Reporter"
        i=0
        while [ -d ${today}_$i ]; do (( i++ )); done
        resDir=${today}_$i
        mkdir $resDir && cd $resDir
        exec 3>./testlog
        fail="false"
        sourceEnv
        if [ "$?" -ne 0 ]; then fail="true"; fi
        [ $fail = "false" ] && runTask
        if [ "$?" -ne 0 ]; then fail="true"; fi
        if [ $fail = "true" ]; then
            echo -e "Hello guys, daily test fails due to following reason. Details:\n"`cat ${workDir}/${resDir}/testlog` | \
                mutt -s "$mailTitle" ${mailList[@]}
        else
            lastPath=${workDir}/last_test_info
            [ -f $lastPath ] && . $lastPath
            todayPath=${workDir}/${resDir}/testres_${today}
            if [ -n "$lastResPath" ]; then
                python ${workDir}/analyze.py $lastResPath $todayPath ${workDir}/${resDir}/cmp.html
            else
                python ${workDir}/analyze.py $todayPath $todayPath ${workDir}/${resDir}/cmp.html
            fi
            echo "lastResPath=${todayPath}" > $lastPath
            mutt -e "set content_type=text/html" -s "$mailTitle" ${mailList[@]} < ${workDir}/${resDir}/cmp.html
        fi
    elif [ -n "$1" ] && [ "$1" = "--rerun" ]; then
        cd $workDir
        today=$(date +%Y_%m_%d)
        i=0
        while [ -d ${today}_$i ]; do (( i++ )); done
        resDir=${today}_$i
        mkdir $resDir && cd $resDir
        exec 3>./testlog
        fail="false"
        sourceEnv
        if [ "$?" -ne 0 ]; then fail="true"; fi
        [ $fail = "false" ] && runTask
        if [ "$?" -ne 0 ]; then fail="true"; fi
        if [ $fail = "true" ]; then
            cat ${workDir}/${resDir}/testlog
            exit 1
        else
            lastPath=${workDir}/last_test_info
            [ -f $lastPath ] && . $lastPath
            todayPath=${workDir}/${resDir}/testres_${today}
            if [ -n "$lastResPath" ]; then
                python ${workDir}/analyze.py $lastResPath $todayPath ${workDir}/${resDir}/cmp.html
            else
                python ${workDir}/analyze.py $todayPath $todayPath ${workDir}/${resDir}/cmp.html
            fi
            echo "lastResPath=${todayPath}" > $lastPath
        fi
    else
        exec 3>/dev/null
        exec 3>&1
        # crontab only edited by root
        [ `id -u` -eq 0 ] && checkCrontab
        sourceEnv
        if [ -n "$1" ] && [ "$1" = "-g" ]; then genData; fi
    fi
}

if [ -n "$1" ]; then
    main $1;
else
    main
fi
