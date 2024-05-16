<template>
  <div class="cluster volume">
    <div class="data-block" v-loading="loading">
      <el-table :data="resLists" class="mt10" style="width: 100%">
        <!-- <el-table-column type="index" label="#"></el-table-column> -->
        <el-table-column prop="ipAddr" label="异常节点IP" width="200px">
        </el-table-column>

        <el-table-column prop="alarmTypeDes" label="异常类型" width="200px">
        </el-table-column>
        <el-table-column prop="alarmData" label="异常原因"> </el-table-column>
        <el-table-column prop="startTime" label="开始时间"> </el-table-column>
        <el-table-column prop="durationTime" label="持续时间">
        </el-table-column>
      </el-table>
      <div class="clearfix mt20">
        <el-pagination
          class="fr"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
          :page-sizes="resData.page.pageSizes"
          :page-size="resData.page.pageSize"
          layout="sizes, prev, pager, next"
          :total="total"
        ></el-pagination>
        <span class="fr page-tips pr10">{{
          $t("chubaoFS.commonTxt.eachPageShows")
        }}</span>
      </div>

      <div class="label">报障记录</div>
      <el-table :data="faultList" class="mt10" style="width: 100%">
        <el-table-column type="index" label="#"></el-table-column>
        <el-table-column prop="ipAddr" label="IP"> </el-table-column>

        <el-table-column prop="iDCHandleProcess" label="报障进度">
        </el-table-column>
        <el-table-column prop="startTime" label="时间："> </el-table-column>
        <el-table-column prop="isInCluster" label="是否在集群内">
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import baseGql from "../../graphql/cluster";
import moment from "moment";

export default {
  name: "healthTable",
  props: {
    resLists: {
      type: Array,
      default: () => []
    },
    total: {
      type: Number,
      default: 0
    },
    faultList: {
      type: Array,
      default: () => []
    }
  },
  data() {
    return {
      clusterName: "",
      loading: false,
      clusterList: [],
      resData: {
        loading: true,
        page: {
          pageSizes: [10, 20, 30, 40],
          pageNo: 1,
          pageSize: 10,
          totalRecord: 0,
          totalPage: 1
        },
        resLists: []
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
    handleSizeChange(val) {
      this.resData.page.pageSize = val;
      this.$emit("pageChange", this.resData.page);
    },
    handleCurrentChange(val) {
      this.resData.page.pageNo = val;
      this.$emit("pageChange", this.resData.page);
    }
  },
  mounted() {
    let query = this.$route.query;
    this.clusterName = query.clusterName;
    // this.queryFaultList();
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
.label {
  font-weight: bold;
}
</style>
