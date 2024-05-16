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
  clusterView: gql`
    query clusterView {
      clusterView {
        volumeCount
        masterCount
        metaNodeCount
        dataNodeCount
        dataPartitionCount
        metaPartitionCount
        maxMetaPartitionID
        maxDataPartitionID
        clientPkgAddr
        dataNodeBadDisks {
          addr
          badDiskPath
        }
        badPartitionIDs {
          partitionID
          path
        }
      }
    }
  `,
  zoneList: gql`
    query zoneList($cluster: String) {
      zoneList(cluster: $cluster)
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
