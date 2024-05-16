<template>
  <div class="cluster volume">
    <el-card class="box-card">
      <cluster @refresh="initPage"></cluster>
    </el-card>
    <el-tabs
      class="mt20"
      v-model="activeName"
      @tab-click="handleClick"
      type="card"
    >
      <el-tab-pane label="Top IP" name="topIP">
        <top-ip ref="topIp" v-if="activeName == 'topIP'"></top-ip>
      </el-tab-pane>
      <el-tab-pane label="Top Vol" name="topVol">
        <top-vol ref="topVol" v-if="activeName == 'topVol'"></top-vol>
      </el-tab-pane>
      <el-tab-pane label="Top Partition" name="topPartition">
        <top-partition
          ref="toppartition"
          v-if="activeName == 'topPartition'"
        ></top-partition>
      </el-tab-pane>
      <el-tab-pane label="Vol 详情" name="volDetail">
        <vol-detail
          ref="volDetail"
          v-if="activeName == 'volDetail'"
        ></vol-detail>
      </el-tab-pane>
      <el-tab-pane label="IP 详情" name="ipDetail">
        <ip-detail ref="ipDetail" v-if="activeName == 'ipDetail'"></ip-detail>
      </el-tab-pane>
      <el-tab-pane label="Cluster 详情" name="clusterDetail">
        <cluster-detail
          ref="clusterDetail"
          v-if="activeName == 'clusterDetail'"
        ></cluster-detail>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import baseGql from "../../graphql/cluster";
import moment from "moment";
import topIp from "./topIp";
import topVol from "./topVol";
import topPartition from "./topPartition";
import volDetail from "./volDetail";
import ipDetail from "./ipDetail";
import clusterDetail from "./clusterDetail";

export default {
  name: "monitor",
  components: {
    topIp,
    topVol,
    topPartition,
    volDetail,
    ipDetail,
    clusterDetail
  },
  data() {
    return {
      activeName: "topIP"
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
    initPage() {
      this.$refs[this.activeName].init();
    },
    handleClick(tab, event) {
      this.activeName = tab.name;
    }
  },
  mounted() {}
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
</style>
