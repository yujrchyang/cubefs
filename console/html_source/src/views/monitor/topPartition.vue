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
        <!-- <span>时间：</span> -->
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
      <el-form-item label="结果数量：">
        <el-select v-model="query.topN" placeholder="活动区域">
          <el-option
            v-for="i in numberList"
            :label="i"
            :value="i"
            :key="i"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="卷名：">
        <el-select v-model="query.volume" placeholder="请选择卷名">
          <el-option
            v-for="item in volumeList"
            :label="item.namr"
            :value="item.name"
            :key="item.name"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="机器IP：">
        <el-input v-model="query.ip" placeholder="请输入IP"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="onSubmit">查询</el-button>
      </el-form-item>
    </el-form>

    <el-table :data="list" class="mt10" style="width: 100%">
      <el-table-column type="index" label="#"></el-table-column>
      <el-table-column prop="partitionID" label="Partition ID">
      </el-table-column>
      <el-table-column prop="volumeName" label="卷名称"> </el-table-column>
      <el-table-column prop="count" label="请求次数"> </el-table-column>
      <el-table-column prop="size" label="请求流量"> </el-table-column>
      <el-table-column prop="avgSize" label="平均请求流量"> </el-table-column>
    </el-table>
    <div class="clearfix mt20">
      <el-pagination
        class="fr"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
        :page-sizes="page.pageSizes"
        :page-size="page.pageSize"
        layout="sizes, prev, pager, next"
        :total="page.totalRecord"
      ></el-pagination>
      <span class="fr page-tips pr10">{{
        $t("chubaoFS.commonTxt.eachPageShows")
      }}</span>
    </div>
  </div>
</template>

<script>
import baseGql from "../../graphql/monitor";
import moment from "moment";

export default {
  name: "TopPartition",
  components: {},
  data() {
    return {
      volumeList: [],
      userID: null,
      page: {
        pageSizes: [10, 20, 30, 40],
        pageNo: 1,
        pageSize: 10,
        totalRecord: 0,
        totalPage: 1
      },
      list: [],
      moduleList: [],
      operateList: [],
      numberList: [],
      query: {
        cluster: "",
        interval: 1,
        startTime: 0,
        endTime: 0,
        timerange: [],
        op: undefined,
        topN: 0,
        volume: undefined,
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
      this.queryList("reset");
      this.queryVolumeList();
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
    queryList(key) {
      if (!this.query.module || this.query.module == "") {
        this.$message.error("请选择模块！");
        return;
      }
      if (this.query.timerange.length > 0) {
        this.query.interval = 0;
      }
      const params = {
        cluster: this.$route.query.clusterName,
        module: this.query.module,
        interval: this.query.interval,
        topN: this.query.topN,
        op: this.query.op,
        volume: this.query.volume,
        ip: this.query.ip,
        page: key && key === "reset" ? 1 : this.page.pageNo,
        pageSize: key && key === "reset" ? 10 : this.page.pageSize,
        startTime: this.query.interval == 0 ? this.query.timerange[0] : 0,
        endTime: this.query.interval == 0 ? this.query.timerange[1] : 0
      };
      this.apollo
        .query(this.url.consoleTraffic, baseGql.listTopPartition, params)
        .then(res => {
          if (res.data && res.data.listTopPartition) {
            this.list = res.data.listTopPartition.data || [];
            this.page.totalRecord = res.data.listTopPartition.total;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.$Message.error(error);
        });
    },
    handleSizeChange(val) {
      this.page.pageSize = val;
      this.queryList();
    },
    handleCurrentChange(val) {
      this.page.pageNo = val;
      this.queryList();
    },
    onSubmit() {
      this.queryList("reset");
    },
    changeTime(val) {
      this.query.interval = val;
      this.query.timerange = [];
    },
    getNumber100() {
      for (let i = 1; i < 101; i++) {
        this.numberList.push(i);
      }
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
    this.getNumber100();
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
</style>
