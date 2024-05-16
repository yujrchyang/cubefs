<template>
  <div class="cluster volume">
    <el-form :inline="true" :model="query">
      <el-form-item label="模块：" required>
        <el-select
          v-model="query.module"
          placeholder="请选择"
          @change="queryOperateList"
        >
          <el-option
            v-for="item in moduleList"
            :label="item"
            :value="item"
            :key="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="时间：" required>
        <el-button-group>
          <el-button
            :type="query.interval == 1 ? 'primary' : ''"
            @click="changeTime(1)"
            >近10分钟</el-button
          >
          <el-button
            :type="query.interval == 2 ? 'primary' : ''"
            @click="changeTime(2)"
            >近1小时</el-button
          >
          <el-button
            :type="query.interval == 3 ? 'primary' : ''"
            @click="changeTime(3)"
            >近1天</el-button
          >
        </el-button-group>
      </el-form-item>
      <el-form-item label="">
        <el-date-picker
          v-model="query.timerange"
          type="datetimerange"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          value-format="timestamp"
          :picker-options="pickerOptions"
          @change="changeDateTime"
        >
        </el-date-picker>
      </el-form-item>
      <el-form-item label="操作类型：">
        <el-select v-model="query.op" placeholder="请选择">
          <el-option
            v-for="item in operateList"
            :label="item"
            :value="item"
            :key="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="机器IP：" required>
        <el-input v-model="query.ip" placeholder="请输入IP"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="onSubmit">查询</el-button>
      </el-form-item>
    </el-form>
    <div class="chooseChart">
      <el-button-group>
        <el-button :type="!isCount ? 'primary' : ''" @click="changeChart(false)"
          >请求次数</el-button
        >
        <el-button :type="isCount ? 'primary' : ''" @click="changeChart(true)"
          >请求数量</el-button
        >
      </el-button-group>
    </div>

    <div class="charts" id="chart" v-loading="chartLoading"></div>
    <div class="charts nodata" v-if="chartNoData">
      暂无数据
    </div>
  </div>
</template>

<script>
import baseGql from "../../graphql/monitor";
import moment from "moment";
import echarts from "echarts/lib/echarts";
import "echarts/lib/component/legend";
import "echarts/lib/chart/line";
// 引入提示框和标题组件
import "echarts/lib/component/tooltip";
import "echarts/lib/component/title";
import "echarts/lib/component/dataZoom";

export default {
  name: "ipDetail",
  components: {},
  data() {
    return {
      countData: {},
      sizeData: {},
      isCount: false,
      chartNoData: true,
      chartLoading: false,
      volumeList: [],
      userID: null,
      list: [],
      moduleList: [],
      operateList: [],
      query: {
        cluster: "",
        interval: 1,
        startTime: 0,
        endTime: 0,
        timerange: [],
        op: undefined,
        topN: 0,
        ip: undefined
      },
      pickerOptions: {
        disabledDate(time) {
          return time.getTime() > Date.now() - 8.64e6;
        }
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
    init() {
      this.queryModule();
      this.queryVolumeList();
      this.queryChart();
    },
    changeDateTime() {
      this.query.interval = 0;
    },
    queryVolumeList() {
      const variables = {
        keyword: "",
        userID: this.userID,
        num: 10000
      };
      this.apollo
        .query(this.url.volume, baseGql.queryVolumeList, variables)
        .then(res => {
          if (res.data) {
            this.volumeList = res.data.listVolume;
          } else {
            this.$Message.error(res.message);
          }
        })
        .catch(error => {
          this.$Message.error(error);
        });
    },
    queryChart() {
      if (!this.query.module || this.query.module == "") {
        this.$message.error("请选择模块！");
        return;
      }
      if (!this.query.ip || this.query.ip == "") {
        this.$message.error("请输入机器IP！");
        return;
      }
      if (this.query.timerange.length > 0) {
        this.query.interval = 0;
      }
      const params = {
        cluster: this.$route.query.clusterName,
        module: this.query.module,
        interval: this.query.interval,
        op: this.query.op,
        ip: this.query.ip,
        startTime: this.query.interval == 0 ? this.query.timerange[0] : 0,
        endTime: this.query.interval == 0 ? this.query.timerange[1] : 0
      };
      this.apollo
        .query(this.url.consoleTraffic, baseGql.ipDetails, params)
        .then(res => {
          if (res.data) {
            this.buildChart(res.data.ipDetails.data);
          } else {
            this.$Message.error(res.message);
          }
        })
        .catch(error => {
          this.$Message.error(error);
        });
    },
    buildChart(data) {
      if (data.length > 0) {
        this.chartNoData = false;
        let countData = {
          xData: [],
          series: [],
          legend: []
        };
        let sizeData = {
          xData: [],
          series: [],
          legend: []
        };
        data.forEach(item => {
          let countDemo = {
            name: item[0].operationType,
            type: "line",
            data: []
          };
          let sizeDemo = {
            name: item[0].operationType,
            type: "line",
            data: []
          };
          countData.legend.push(item[0].operationType);
          sizeData.legend.push(item[0].operationType);
          item.forEach(val => {
            countDemo.data.push([val.time, val.count]);
            sizeDemo.data.push([val.time, val.size]);
          });
          countData.series.push(countDemo);
          sizeData.series.push(sizeDemo);
        });
        console.log(countData, sizeData);
        this.countData = countData;
        this.sizeData = sizeData;
        this.initChart(sizeData);
      } else {
        this.chartNoData = true;
        this.initChart({});
      }
    },
    initChart(item) {
      let chart = echarts.init(document.getElementById("chart"));
      let option = {
        title: {
          text: "",
          left: "center",
          top: "top"
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
          "#ea7ccc",
          "#767CF0"
        ],
        tooltip: {
          trigger: "axis",
          confine: true
        },
        legend: {
          data: item.legend,
          bottom: "10px"
        },
        toolbox: {
          feature: {
            saveAsImage: {}
          }
        },
        xAxis: {
          type: "time",
          boundaryGap: false,
          splitNumber: 10,
          axisLabel: {
            hideOverlap: true
          }
          // data: item.xData
        },
        yAxis: {
          axisTick: {
            show: false
          },
          axisLine: {
            show: false
          },
          type: "value"
          // axisLabel: {
          //   formatter: "value"
          // }
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
    queryList(key) {
      if (!this.query.module || this.query.module == "") {
        this.$message.error("请选择模块！");
        return;
      }
      if (this.query.timerange.length > 0) {
        this.query.interval = 0;
      }
      const params = {
        page: key && key === "reset" ? 1 : this.page.pageNo,
        pageSize: key && key === "reset" ? 10 : this.page.pageSize,
        cluster: this.$route.query.clusterName,
        module: this.query.module,
        interval: this.query.interval,
        topN: this.query.topN,
        op: this.query.op,
        volume: this.query.volume,
        startTime: this.query.interval == 0 ? this.query.timerange[0] : 0,
        endTime: this.query.interval == 0 ? this.query.timerange[1] : 0
      };
      this.apollo
        .query(this.url.consoleTraffic, baseGql.listTopIp, params)
        .then(res => {
          if (res.data && res.data.listTopIp) {
            this.list = res.data.listTopIp.data || [];
            this.page.totalRecord = res.data.listTopIp.total;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.$Message.error(error);
        });
    },
    changeChart(isCount) {
      this.isCount = isCount;
      let data = isCount ? this.countData : this.sizeData;
      this.initChart(data);
    },
    onSubmit() {
      this.queryChart("reset");
    },
    changeTime(val) {
      this.query.interval = val;
      this.query.timerange = [];
    },
    queryModule() {
      this.apollo
        .query(this.url.consoleTraffic, baseGql.listModuleType)
        .then(res => {
          if (res.data) {
            this.moduleList = res.data.listModuleType || [];
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.$Message.error(error);
        });
    },
    queryOperateList() {
      const params = {
        module: this.query.module
      };
      this.apollo
        .query(this.url.consoleTraffic, baseGql.listOp, params)
        .then(res => {
          if (res.data) {
            this.operateList = res.data.listOp || [];
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.$Message.error(error);
        });
    }
  },
  mounted() {
    this.userID = sessionStorage.getItem("access_userID");
    this.init();
  }
};
</script>

<style>
.volume p {
  margin-top: 30px;
  font-size: 13px;
}
.volume {
  min-height: 600px;
}
.cluster .el-form-item .el-form-item__label {
  line-height: 40px !important;
}
.cluster .el-form-item .el-range-editor.el-input__inner {
  padding: 0 10px;
  margin-top: 5px;
}
.charts {
  width: 100%;
  height: 350px;
}
.charts.nodata {
  text-align: center;
  margin-top: -150px;
}
.chooseChart {
  width: 100%;
  text-align: center;
}
</style>
