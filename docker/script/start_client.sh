#!/bin/bash

# Copyright 2018 The CubeFS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

MntPoint=/cfs/mnt
mkdir -p /cfs/bin /cfs/log /cfs/mnt
src_path=/go/src/github.com/chubaofs/cfs
cli=/cfs/bin/cfs-cli
conf_path=/cfs/conf

LeaderAddr=""
VolName=ltptest
Owner=ltptest
AccessKey=39bEF4RrAQgMj6RV
SecretKey=TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd
AuthKey="0e20229116d5a9a4a9e876806b514a85"
TryTimes=5

init_cli() {
    cp ${cli} /usr/bin/
    cd ${conf_path}
    ${cli} completion &> /dev/null
    echo 'source '${conf_path}'/cfs-cli.sh' >> ~/.bashrc
    echo -n "Installing ChubaoFS cli tool  ... "
    echo -e "\033[32m done\033[0m"
}

check_cluster() {
    echo -n "Checking cluster  ... "
    for i in $(seq 1 300) ; do
        ${cli} cluster info &> /tmp/cli_cluster_info
        LeaderAddr=$(grep -i "master leader" /tmp/cli_cluster_info | awk '{print$4}')
        if [[ "x$LeaderAddr" != "x" ]] ; then
            echo -e "\033[32m done\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31m fail\033[0m"
    exit 1
}

create_cluster_user() {
    echo -n "Creating user     ... "
    # check user exist
    ${cli} user info ${Owner} &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32m done\033[0m"
        return
    fi
    # try create user
    for i in $(seq 1 300) ; do
        curl -H "Content-Type: application/json" -X POST \
          "http://master.chubao.io/user/create" -d '
          {
              "id":"'${Owner}'",
              "pwd":"",
              "ak":"'${AccessKey}'",
              "sk":"'${SecretKey}'",
              "type":3,
              "description":""
          }
        ' > /tmp/cli_user_create 2>&1
        if [[ $? -eq 0 ]] ; then
            echo -e "\033[32m done\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31m fail\033[0m"
    exit 1
}

ensure_node_writable() {
    node=$1
    echo -n "Checking $node ... "
    for i in $(seq 1 300) ; do
        ${cli} "${node}" list &> /tmp/cli_"${node}"_list;
        res=$(grep "Yes" /tmp/cli_"${node}"_list | grep -c "Active")
        if [[ ${res} -ge 3 ]]; then
            echo -e "\033[32m done\033[0m"
            return
        fi
        sleep 1
    done
    echo -e "\033[31m fail\033[0m"
    cat /tmp/cli_"${node}"_list
    exit 1
}

create_volume() {
    echo -n "Creating volume   ... "
    # check volume exist
    ${cli} volume info ${VolName} &> /dev/null
    if [[ $? -eq 0 ]]; then
        curl -s "${LeaderAddr}/vol/update?name=${VolName}&authKey=${AuthKey}&flock=true" &> /dev/null
        echo -e "\033[32m done\033[0m"
        return
    fi
    create_volume_with_para ${VolName} ${Owner} 30 1 false &> /dev/null
    if [[ $? -ne 0 ]]; then
        echo -e "\033[31m fail\033[0m"
        exit 1
    fi
    curl -s "${LeaderAddr}/vol/update?name=${VolName}&authKey=${AuthKey}&flock=true" &> /dev/null
    echo -e "\033[32m done\033[0m"
}

# $1:name
# $2:owner
# $3:capacity
# $4:store-mode [1:Mem, 2:Rocks]
# $5:follower-read  true/false
create_volume_with_para() {
  curl http://master.chubao.io/admin/createVol \
    -d name=$1 \
    -d owner=$2 \
    -d mpCount=3 \
    -d size=120 \
    -d capacity=$3 \
    -d ecDataNum=4 \
    -d ecParityNum=2 \
    -d ecEnable=false \
    -d followerRead=$5 \
    -d forceROW=false \
    -d writeCache=false \
    -d crossRegion=0 \
    -d autoRepair=false \
    -d replicaNum=3 \
    -d mpReplicaNum=3 \
    -d volWriteMutex=false \
    -d zoneName=default \
    -d trashRemainingDays=0 \
    -d storeMode=$4 \
    -d metaLayout="0,0" \
    -d smart=false \
    -d smartRules="" \
    -d compactTag=false \
    -d hostDelayInterval=0 \
    -d batchDelInodeCnt=0 \
    -d delInodeInterval=0 \
    -d enableBitMapAllocator=0 \
    -d metaOut=false
}

show_cluster_info() {
    tmp_file=/tmp/collect_cluster_info
    ${cli} cluster info &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} metanode list &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} datanode list &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} codecnode list &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} ecnode list &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} user info ${Owner} &>> ${tmp_file}
    echo &>> ${tmp_file}
    ${cli} volume info ${VolName} &>> ${tmp_file}
    echo &>> ${tmp_file}
    grep -v "Master address" /tmp/collect_cluster_info
}

create_idc() {
    echo -n "Checking idc  ... "
    curl -s "${LeaderAddr}/idc/get?name=huitian" | grep '"default":"hdd"' &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32m done\033[0m"
        return
    fi
    echo -n "Create huitian idc   ... "
    curl -s "${LeaderAddr}/idc/create?name=huitian" | grep '"code":0' &> /dev/null
    if [[ $? -eq 0 ]] ; then
        echo -e "\033[32m done\033[0m"
    else
        echo -e "\033[31m fail\033[0m"
        exit 1
    fi
    echo -n "Set idc default zone mediumType hdd   ... "
    curl -s "${LeaderAddr}/zone/setIDC?zoneName=default&idcName=huitian&mediumType=hdd" | grep '"code":0' &> /dev/null
    if [[ $? -eq 0 ]] ; then
         echo -e "\033[32m done\033[0m"
    else
        echo -e "\033[31m fail\033[0m"
        exit 1
    fi
}

start_client() {
    echo -n "Starting client   ... "
    /cfs/bin/cfs-client-inner -c /cfs/conf/client.json
    for((i=0; i<TryTimes; i++)) ; do
        sleep 2
        sta=$( mount | grep -q "$VolName on $MntPoint" ; echo $? )
        if [[ $sta -eq 0 ]] ; then
#            ok=1
	          echo -e "\033[32m done\033[0m"
	          return
        fi
    done
    echo -e "\033[31m fail\033[0m"
    exit 1
}

start_repair_server() {
    echo -n "Starting repair server   ... "
    nohup /cfs/bin/repair_server -c /cfs/conf/repair_server.json &
	  echo -e "\033[32m done\033[0m"
    exit 0
}

init_cli
check_cluster
create_cluster_user
ensure_node_writable "metanode"
ensure_node_writable "datanode"
create_volume
create_idc
show_cluster_info
start_client
start_repair_server

