<template>
  <div class="cluster volume">
    <el-card class="box-card">
      <cluster @refresh="initPage"></cluster>
    </el-card>
    <el-tabs
      class="mt20"
      v-model="activeName"
      @tab-click="handleClick"
      type="border-card"
    >
      <el-tab-pane label="master" name="master">
        <span slot="label">
          master
          <i
            v-if="activeName == 'master' && info.alarmLevel == 0"
            class="state health"
          >
            {{ info.count }}
          </i>
          <i
            class="state normal"
            v-if="activeName == 'master' && info.alarmLevel == 1"
          >
            {{ info.count }}
          </i>
          <i
            class="state emergent"
            v-if="activeName == 'master' && info.alarmLevel == 2"
          >
            {{ info.count }}
          </i>
        </span>
        <health-table
          :activeName="activeName"
          v-if="activeName == 'master'"
          :resLists="resLists"
          :total="info.count"
          :faultList="faultList"
          @pageChange="queryHealthList"
        ></health-table>
      </el-tab-pane>
      <el-tab-pane label="datanode" name="datanode">
        <span slot="label">
          datanode
          <i
            v-if="activeName == 'datanode' && info.alarmLevel == 0"
            class="state health"
          >
            {{ info.count }}
          </i>
          <i
            class="state normal"
            v-if="activeName == 'datanode' && info.alarmLevel == 1"
          >
            {{ info.count }}
          </i>
          <i
            class="state emergent"
            v-if="activeName == 'datanode' && info.alarmLevel == 2"
          >
            {{ info.count }}
          </i>
        </span>
        <health-table
          :activeName="activeName"
          v-if="activeName == 'datanode'"
          :resLists="resLists"
          :total="info.count"
          :faultList="faultList"
          @pageChange="queryHealthList"
        ></health-table>
      </el-tab-pane>
      <el-tab-pane label="metanode" name="metanode">
        <span slot="label">
          metanode
          <i
            v-if="activeName == 'metanode' && info.alarmLevel == 0"
            class="state health"
          >
            {{ info.count }}
          </i>
          <i
            class="state normal"
            v-if="activeName == 'metanode' && info.alarmLevel == 1"
          >
            {{ info.count }}
          </i>
          <i
            class="state emergent"
            v-if="activeName == 'metanode' && info.alarmLevel == 2"
          >
            {{ info.count }}
          </i>
        </span>
        <health-table
          :activeName="activeName"
          v-if="activeName == 'metanode'"
          :resLists="resLists"
          :total="info.count"
          :faultList="faultList"
          @pageChange="queryHealthList"
        ></health-table>
      </el-tab-pane>
      <el-tab-pane label="objectnode" name="objectnode">
        <span slot="label">
          objectnode
          <i
            v-if="activeName == 'objectnode' && info.alarmLevel == 0"
            class="state health"
          >
            {{ info.count }}
          </i>
          <i
            class="state normal"
            v-if="activeName == 'objectnode' && info.alarmLevel == 1"
          >
            {{ info.count }}
          </i>
          <i
            class="state emergent"
            v-if="activeName == 'objectnode' && info.alarmLevel == 2"
          >
            {{ info.count }}
          </i>
        </span>
        <health-table
          :activeName="activeName"
          v-if="activeName == 'objectnode'"
          :resLists="resLists"
          :total="info.count"
          :faultList="faultList"
          @pageChange="queryHealthList"
        ></health-table>
      </el-tab-pane>
      <!-- <el-tab-pane label="LB负载" name="lbLoad">lbLoad</el-tab-pane> -->
    </el-tabs>
  </div>
</template>

<script>
import baseGql from "../../graphql/cluster";
import moment from "moment";
import healthTable from "./healthTable";

export default {
  name: "ClusterHealth",
  components: {
    healthTable
  },
  data() {
    return {
      activeName: "master",
      loading: false,
      clusterList: [],
      faultList: [],
      resLists: [],
      info: {
        alarmLevel: null,
        count: null
      }
    };
  },
  filters: {
    dateFormat(time) {
      if (time) {
        const str = time.toString().substring(0, 13);
        return moment(Number(str)).format("YYYY-MM-DD HH:mm:ss");
      }
    }
  },
  methods: {
    initPage() {},
    handleClick(tab, event) {
      console.log(tab, event);
      this.queryHealthList();
      this.queryFaultList();
    },
    queryHealthList(page) {
      const variables = {
        cluster: this.$route.query.clusterName,
        roleName: this.activeName,
        page: page ? page.pageNo : 1,
        pageSize: page ? page.pageSize : 10
      };
      // this.resData.loading = true;
      this.apollo
        .query(this.url.consoleCluster, baseGql.healthInfo, variables)
        .then(res => {
          // this.resData.loading = false;
          if (res.data) {
            this.resLists = res.data.healthInfo.alarmList;
            this.info.alarmLevel = res.data.healthInfo.alarmLevel;
            this.info.count = res.data.healthInfo.totalNum;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.resData.loading = false;
          this.$Message.error(error);
        });
    },
    queryFaultList() {
      const variables = {
        cluster: this.$route.query.clusterName,
        roleName: this.activeName
      };
      this.apollo
        .query(this.url.consoleCluster, baseGql.healthHistoryInfo, variables)
        .then(res => {
          if (res.data) {
            this.faultList = res.data.healthHistoryInfo.alarmList;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.resData.loading = false;
          this.$Message.error(error);
        });
    }
  },
  mounted() {
    this.queryHealthList();
    this.queryFaultList();
  }
};
</script>

<style scoped>
.volume p {
  margin-top: 30px;
  font-size: 13px;
}
.volume {
  min-height: 600px;
}
.volume-right {
  display: inline-block;
  float: right;
}

.volume-name {
  cursor: pointer;
  color: #466be4;
}
.stateLevel i {
  width: 3px;
  height: 3px;
  border-radius: 50%;
}
.state {
  color: #fff;
  border-radius: 50%;
  margin-left: 5px;
  font-size: 8px;
  padding: 2px 4px;
}
.state.health {
  background-color: #18af28;
}
.state.emergent {
  background-color: #f35e5e;
}
.state.normal {
  background-color: #fdb620;
}
</style>
