import gql from "graphql-tag"; // 引入graphql
const baseGql = {
  switchCluster: gql`
    mutation switchCluster($cluster: String) {
      switchCluster(cluster: $cluster) {
        code
        message
      }
    }
  `,
  listModuleType: gql`
    query listModuleType {
      listModuleType
    }
  `,
  listOp: gql`
    query listOp($module: String) {
      listOp(module: $module)
    }
  `,
  queryVolumeList: gql`
    query listVolume($keyword: String, $userID: String) {
      listVolume(keyword: $keyword, userID: $userID) {
        name
        capacity
        occupied
        dpReplicaNum
        owner
        createTime
        oSSAccessKey
        oSSSecretKey
        inodeCount
        toSimpleVolView {
          description
          dpCnt
          mpCnt
          rwDpCnt
        }
        status
      }
    }
  `,
  listTopIp: gql`
    query listTopIp(
      $module: String
      $cluster: String
      $interval: int
      $topN: int
      $op: string
      $startTime: int64
      $endTime: int64
      $volume: String
      $page: int
      $pageSize: int
    ) {
      listTopIp(
        module: $module
        cluster: $cluster
        interval: $interval
        topN: $topN
        op: $op
        startTime: $startTime
        endTime: $endTime
        volume: $volume
        page: $page
        pageSize: $pageSize
      ) {
        total
        data {
          ipAddr
          count
          size
          avgSize
        }
      }
    }
  `,
  listTopPartition: gql`
    query listTopPartition(
      $module: String
      $cluster: String
      $interval: Int
      $topN: Int
      $op: string
      $startTime: int64
      $endTime: int64
      $volume: String
      $ip: String
      $page: int32
      $pageSize: int32
    ) {
      listTopPartition(
        module: $module
        cluster: $cluster
        interval: $interval
        topN: $topN
        op: $op
        startTime: $startTime
        endTime: $endTime
        volume: $volName
        ip: $ip
        page: $page
        pageSize: $pageSize
      ) {
        total
        data {
          count
          size
          avgSize
          volumeName
          partitionID
        }
      }
    }
  `,
  listTopVol: gql`
    query listTopVol(
      $module: String
      $cluster: String
      $interval: int
      $topN: int
      $op: String
      $startTime: int64
      $endTime: int64
      $ip: String
      $page: int32
      $pageSize: int32
    ) {
      listTopVol(
        module: $module
        cluster: $cluster
        interval: $interval
        topN: $topN
        op: $op
        startTime: $startTime
        endTime: $endTime
        ip: $ip
        page: $page
        pageSize: $pageSize
      ) {
        total
        data {
          count
          size
          avgSize
          volumeName
        }
      }
    }
  `,
  zoneList: gql`
    query zoneList($cluster: String) {
      zoneList(cluster: $cluster)
    }
  `,
  volDetails: gql`
    query volDetails(
      $module: String
      $cluster: String
      $interval: int
      $op: String
      $startTime: int
      $endTime: int
      $volume: String
    ) {
      volDetails(
        module: $module
        cluster: $cluster
        interval: $interval
        op: $op
        startTime: $startTime
        endTime: $endTime
        volume: $volume
      ) {
        data {
          operationType
          time
          count
          size
        }
      }
    }
  `,

  ipDetails: gql`
    query ipDetails(
      $module: String
      $cluster: String
      $interval: int
      $op: String
      $startTime: int
      $endTime: int
      $ip: String
    ) {
      ipDetails(
        module: $module
        cluster: $cluster
        interval: $interval
        op: $op
        startTime: $startTime
        endTime: $endTime
        ip: $ip
      ) {
        data {
          operationType
          time
          count
          size
        }
      }
    }
  `,
  clusterDetails: gql`
    query clusterDetails(
      $module: String
      $cluster: String
      $interval: int
      $op: String
      $startTime: int
      $endTime: int
    ) {
      clusterDetails(
        module: $module
        cluster: $cluster
        interval: $interval
        op: $op
        startTime: $startTime
        endTime: $endTime
      ) {
        data {
          operationType
          time
          count
          size
        }
      }
    }
  `,
  healthInfo: gql`
    query healthInfo(
      $cluster: String
      $roleName: String
      $page: int32
      $pageSize: int32
    ) {
      healthInfo(
        cluster: $cluster
        roleName: $roleName
        page: $page
        pageSize: $pageSize
      ) {
        clusterName
        roleName
        totalNum
        alarmLevel
        alarmList {
          ipAddr
          alarmTypeDes
          alarmData
          startTime
          durationTime
        }
      }
    }
  `,
  healthHistoryInfo: gql`
    query healthHistoryInfo($cluster: String, $roleName: String) {
      healthHistoryInfo(cluster: $cluster, roleName: $roleName) {
        alarmList {
          ipAddr
          startTime
          iDCHandleProcess
          isInCluster
        }
      }
    }
  `,
  clusterCapacity: gql`
    query clusterCapacity(
      $cluster: String
      $zoneName: String
      $intervalType: Int
      $start: Uint64
      $end: Uint64
    ) {
      clusterCapacity(
        cluster: $cluster
        zoneName: $zoneName
        intervalType: $intervalType
        start: $start
        end: $end
      ) {
        date
        totalGB
        usedGB
      }
    }
  `,
  queryClusterList: gql`
    query clusterInfoList {
      clusterInfoList {
        clusterName
        clusterNameZH
        volNum
        totalTB
        usedTB
        increaseTB
        stateLevel
      }
    }
  `
};

export default baseGql;
