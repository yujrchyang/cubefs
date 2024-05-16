<template>
  <div class="cluster volume">
    <el-card class="box-card">
      <cluster @refresh="initPage"></cluster>
    </el-card>

    <p>基本信息</p>
    <div class="basic">
      <ul>
        <span class="label">master节点数量:</span>
        <span class="value">{{ detailInfo.masterCount }}</span>
        <span class="label">volume个数:</span>
        <span class="value">{{ detailInfo.volumeCount }}</span>
        <span class="label">meta node节点数量:</span>
        <span class="value">{{ detailInfo.metaNodeCount }}</span>
        <!-- <span class="label">object节点数量:</span>
      <span class="value">{{ detailInfo.masterCount }}</span> -->
        <span class="label">data node节点数量:</span>
        <span class="value">{{ detailInfo.dataNodeCount }}</span>
      </ul>
      <ul>
        <span class="label">MP总数量:</span>
        <span class="value">{{ detailInfo.dataPartitionCount }}</span>
        <span class="label">DP总数量:</span>
        <span class="value">{{ detailInfo.metaPartitionCount }}</span>
        <!-- <span class="label">client数量:</span>
      <span class="value">{{ detailInfo.masterCount }}</span> -->
        <span class="label">max mpid:</span>
        <span class="value">{{ detailInfo.maxMetaPartitionID }}</span>
        <span class="label">max dpid:</span>
        <span class="value">{{ detailInfo.maxDataPartitionID }}</span>
      </ul>
      <ul>
        <span class="label">max nodeid:</span>
        <span class="value">{{ detailInfo.maxMetaNodeID || "-" }}</span>
        <span class="label">热升级地址:</span>
        <span class="value large">{{ detailInfo.clientPkgAddr || "-" }}</span>
      </ul>
      <ul>
        <span class="label">故障盘列表:</span>
        <span class="value">
          <el-button @click="showFaultList" type="text" class="text-btn">
            详情
          </el-button>
        </span>
        <span class="label">待恢复dp列表:</span>
        <span class="value">
          <el-button type="text" class="text-btn" @click="showRecoveryList">
            详情
          </el-button>
        </span>
        <span></span>
        <span></span>
        <span></span>
        <span></span>
      </ul>
      <!-- <span class="label">meta node容量:</span>
      <span class="value">{{ detailInfo.masterCount }}</span>
      <span class="label">meta node使用量:</span>
      <span class="value">{{ detailInfo.masterCount }}</span>
      <span class="label">磁盘总容量:</span>
      <span class="value">{{ detailInfo.masterCount }}</span>
      <span class="label">磁盘使用容量:</span>
      <span class="value">{{ detailInfo.masterCount }}</span> -->
    </div>
    <div class="zone pt30">
      <el-row>
        <!-- <el-col :span="2"></el-col> -->
        <el-col :span="5">
          <span>zone：</span>
          <el-select v-model="zoneName">
            <el-option
              v-for="item in zoneList"
              :value="item"
              :label="item"
              :key="item"
            ></el-option> </el-select
        ></el-col>
        <el-col :span="7">
          <span>时间：</span>
          <el-button-group>
            <el-button
              :type="intervalType == 1 ? 'primary' : ''"
              @click="changeTime(1)"
              >近10分钟</el-button
            >
            <el-button
              :type="intervalType == 2 ? 'primary' : ''"
              @click="changeTime(2)"
              >近1小时</el-button
            >
            <el-button
              :type="intervalType == 3 ? 'primary' : ''"
              @click="changeTime(3)"
              >近1天</el-button
            >
            <el-button
              :type="intervalType == 4 ? 'primary' : ''"
              @click="changeTime(4)"
              >近7天</el-button
            >
          </el-button-group></el-col
        >
        <el-col :span="7">
          <el-date-picker
            v-model="timerange"
            type="datetimerange"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            value-format="timestamp"
            :picker-options="pickerOptions"
          >
          </el-date-picker
        ></el-col>
        <el-col :span="2">
          <el-button type="primary" @click="getChart">查询</el-button>
        </el-col>
      </el-row>
    </div>

    <div class="charts" id="chart" v-loading="chartLoading"></div>
    <div class="charts nodata" v-if="chartNoData">
      暂无数据
    </div>

    <el-dialog title="故障盘列表" :visible.sync="showFault" width="40%">
      <div class="table-style recovery">
        <ul>
          <span class="head">addr</span>
          <span class="head">badDiskPath</span>
        </ul>
        <!-- <ul v-for="(item, index) in detailInfo.dataNodeBadDisks" :key="index">
          <span>{{ item.addr }}</span>
          <span>{{ item.badDiskPath }}</span>
        </ul> -->
      </div>
    </el-dialog>
    <el-dialog title="待恢复dp列表" :visible.sync="showRecovery" width="35%">
      <div class="table-style recovery">
        <ul>
          <span class="head">partitionID</span>
          <span class="head">path</span>
        </ul>
        <ul v-for="(item, index) in detailInfo.badPartitionIDs" :key="index">
          <span>{{ item.partitionID }}</span>
          <span>{{ item.path }}</span>
        </ul>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import baseGql from "../../graphql/cluster";
import moment from "moment";
// import cluster from "@/components/cluster";
import echarts from "echarts/lib/echarts";
import "echarts/lib/component/legend";
import "echarts/lib/chart/line";
// 引入提示框和标题组件
import "echarts/lib/component/tooltip";
import "echarts/lib/component/title";
import "echarts/lib/component/dataZoom";

export default {
  name: "ClusterDetail",
  components: {
    // cluster
  },
  data() {
    return {
      chartNoData: true,
      chartLoading: false,
      pickerOptions: {
        disabledDate(time) {
          return time.getTime() > Date.now() - 8.64e6;
        }
      },
      timerange: [],
      zoneList: [],
      intervalType: 1,
      zoneName: undefined,
      detailInfo: {},
      showRecovery: false,
      showFault: false
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
    initChart(item) {
      let chart = echarts.init(document.getElementById("chart"));
      let option = {
        title: {
          text: "zone容量曲线",
          left: "center",
          top: "top"
          // textStyle: {
          //   color: "#333",
          //   fontSize: 14
          // }
        },
        // color: dashboardParam.colorLists, // 修改曲线颜色
        color: [
          "#5470c6",
          "#91cc75",
          "#fac858",
          "#ee6666",
          "#73c0de",
          "#3ba272",
          "#fc8452",
          "#9a60b4",
          "#ea7ccc"
        ],
        tooltip: {
          trigger: "axis",
          confine: true
        },
        legend: {
          data: ["总容量", "使用量"],
          bottom: "10px"
        },
        toolbox: {
          feature: {
            saveAsImage: {}
          }
        },
        xAxis: {
          type: "category",
          boundaryGap: false,
          data: item.xData
        },
        yAxis: {
          axisTick: {
            show: false
          },
          axisLine: {
            show: false
          },
          type: "value",
          axisLabel: {
            formatter: "{value} GB"
          }
          // min: function(value) {
          //   return value.min;
          // },
          // max: function(value) {
          //   return value.max;
          // }
        },
        series: item.series
      };
      chart.setOption(option, true);
    },
    getChart() {
      if (!this.zoneName || this.zoneName == "") {
        this.$message.error("请选择zone");
        return false;
      }
      if (this.timerange.length > 0) {
        this.intervalType = 0;
      }
      if (this.timerange.length == 0 && this.intervalType == 0) {
        this.$message.error("请选择时间");
        return false;
      }
      this.chartLoading = true;
      const variables = {
        cluster: this.clusterName,
        zoneName: this.zoneName,
        intervalType: this.timerange.length > 0 ? 0 : this.intervalType,
        start:
          this.timerange.length > 0 ? Math.floor(this.timerange[0] / 1000) : 0,
        end:
          this.timerange.length > 0 ? Math.floor(this.timerange[1] / 1000) : 0
      };
      this.apollo
        .query(this.url.consoleCluster, baseGql.clusterCapacity, variables)
        .then(res => {
          if (res.data) {
            const data = res.data.clusterCapacity;
            this.chartNoData = data.length == 0;
            if (data.length == 0) {
              return;
            }
            console.log(res.data.clusterCapacity);
            let item = {
              xData: [],
              series: []
            };
            let total = {
              name: "总容量",
              type: "line",
              data: []
            };
            let used = {
              name: "使用量",
              type: "line",
              data: []
            };
            data.forEach(val => {
              item.xData.push(
                moment(val.date * 1000).format("YYYY/MM/DD HH:mm:ss")
              );
              total.data.push(val.totalGB);
              used.data.push(val.usedGB);
            });
            item.series.push(total);
            item.series.push(used);
            console.log(item);
            this.initChart(item);
          } else {
            this.$message.error(res.message);
          }
          this.chartLoading = false;
        })
        .catch(error => {
          console.log(error);
          this.$Message.error(error);
        });
    },
    changeTime(key) {
      this.intervalType = key;
      this.timerange = [];
    },
    showFaultList() {
      this.showFault = true;
    },
    showRecoveryList() {
      this.showRecovery = true;
    },
    queryClusterDetail(row) {
      this.loading = true;
      const variables = {
        clusterName: this.clusterName
      };
      this.apollo
        .query(this.url.cluster, baseGql.clusterView, variables)
        .then(res => {
          this.loading = false;
          if (res.data) {
            const result = res.data || {};
            this.detailInfo = result.clusterView || {};
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.resData.loading = false;
          this.$Message.error(error);
        });
    },
    queryZoneList(row) {
      const variables = {
        cluster: this.clusterName
      };
      this.apollo
        .query(this.url.consoleCluster, baseGql.zoneList, variables)
        .then(res => {
          if (res.data) {
            this.zoneList = res.data.zoneList;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.resData.loading = false;
          this.$Message.error(error);
        });
    },

    goDetail(row) {
      this.queryDepotList(row);
    },
    initPage() {
      this.queryClusterDetail();
      this.queryZoneList();
    }
  },
  mounted() {
    let query = this.$route.query;
    this.clusterName = query.clusterName;
    this.queryClusterDetail();
    this.queryZoneList();
  }
};
</script>

<style scoped>
.volume p {
  font-size: 16px;
  font-weight: bold;
  padding-bottom: 20px;
  padding-top: 20px;
}

.volume-right {
  display: inline-block;
  float: right;
}

.volume-name {
  cursor: pointer;
  color: #466be4;
}
.cluster {
  width: 100%;
  padding: 20px;
}
.recovery {
  width: 100%;
  border: 1px solid #ebeef5;
  border-radius: 4px;
  max-height: 500px;
  overflow: scroll;
}
.recovery ul {
  width: 100%;
  display: flex;
}
.recovery ul .head {
  font-weight: bold;
}
.recovery ul span {
  display: inline-block;
  padding: 6px 8px;
  border-bottom: 1px solid #ebeef5;
}
.recovery ul span:first-child {
  flex: 0 0 100px;
}
.recovery ul span:last-child {
  flex: 1;
  border-left: 1px solid #ebeef5;
}
.basic ul {
  text-align: center;
  font-size: 0;
  /* border-top: 1px solid #ebeef5; */
  width: 100%;
  display: flex;
}
.basic ul:last-child span {
  border-bottom: 1px solid #ebeef5;
}
.basic ul span {
  flex: 0 0 12.5%;
  padding: 5px 0;
  border-top: 1px solid #ebeef5;
  border-left: 1px solid #ebeef5;
  display: inline-block;
  line-height: 30px;
  font-size: 14px;
}
.basic ul span:last-child {
  border-right: 1px solid #ebeef5;
}
.basic ul span.large {
  flex: 0 0 62.8%;
}
.border-style {
  display: flex;
  text-align: center;
}
.border-style span {
  padding: 5px 0;
  display: inline-block;
  border: 1px solid #ebeef5;
  border-top: none;
  flex: 0 0 12.4%;
}
.right {
  float: right;
}
.charts {
  padding-top: 40px;
  width: 100%;
  height: 350px;
}
.charts.nodata {
  text-align: center;
  margin-top: -150px;
}
</style>
