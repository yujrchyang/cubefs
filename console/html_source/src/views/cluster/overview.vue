<template>
  <div class="cluster volume">
    <div class="data-block" v-loading="loading">
      <el-table :data="clusterList" class="mt10" style="width: 100%">
        <el-table-column type="index" label="#"></el-table-column>
        <el-table-column
          prop="clusterNameZH"
          :label="$t('chubaoFS.cluster.ClusterName')"
        >
        </el-table-column>

        <el-table-column
          prop="volNum"
          :label="$t('chubaoFS.cluster.VolumeCount')"
        >
        </el-table-column>
        <el-table-column
          prop="totalTB"
          :label="$t('chubaoFS.cluster.StorageCapacity')"
        >
        </el-table-column>
        <el-table-column
          prop="usedTB"
          :label="$t('chubaoFS.cluster.UsageCapacity')"
        >
        </el-table-column>
        <el-table-column prop="type" :label="$t('chubaoFS.cluster.UsageRate')">
          <template slot-scope="scope">
            <span>
              {{ (scope.row.usedTB / scope.row.totalTB).toFixed(2) }}</span
            >
          </template>
        </el-table-column>
        <el-table-column
          prop="increaseTB"
          :label="$t('chubaoFS.cluster.YesterdayAdd')"
        >
        </el-table-column>
        <el-table-column
          prop="stateLevel"
          :label="$t('chubaoFS.cluster.ClusterHealth')"
        >
          <template slot-scope="scope">
            <span v-if="scope.row.stateLevel == 0" class="stateLevel health">
              <i></i>
              {{ $t("chubaoFS.tools.Health") }}
            </span>
            <span v-if="scope.row.stateLevel == 1" class="stateLevel normal">
              {{ $t("chubaoFS.tools.Normal") }}
            </span>
            <span v-if="scope.row.stateLevel == 2" class="stateLevel emergent">
              {{ $t("chubaoFS.tools.Emergent") }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop :label="$t('chubaoFS.tools.Actions')">
          <template slot-scope="scope">
            <el-button
              type="text"
              class="text-btn"
              @click="goDetail(scope.row)"
              >{{ $t("chubaoFS.tools.Details") }}</el-button
            >
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import baseGql from "../../graphql/cluster";
import moment from "moment";

export default {
  name: "ClusterOverview",
  data() {
    return {
      loading: false,
      clusterList: []
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
    queryClusterList(row) {
      this.loading = true;
      this.apollo
        .query(this.url.consoleCluster, baseGql.queryClusterList)
        .then(res => {
          this.loading = false;
          if (res.data) {
            this.clusterList = res.data.clusterInfoList;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.loading = false;
          this.$Message.error(error);
        });
    },
    deleteVolume(row) {
      const variables = {
        authKey: this.md5(row.owner),
        name: row.name
      };
      this.$confirm(
        this.$t("chubaoFS.operations.VolumeManagement.deleteVolTip") +
          " " +
          row.name +
          "?",
        this.$t("chubaoFS.tools.Warning"),
        {
          confirmButtonText: this.$t("chubaoFS.tools.Yes"),
          cancelButtonText: this.$t("chubaoFS.tools.No"),
          type: "warning"
        }
      )
        .then(() => {
          this.apollo
            .mutation(this.url.volume, baseGql.deleteVolume, variables)
            .then(res => {
              if (res.code === 200) {
                this.queryVolumeList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success"
                });
              } else {
                this.$message.error(this.$t("chubaoFS.message.Error"));
              }
            });
        })
        .catch(() => {});
    },
    goDetail(row) {
      sessionStorage.setItem("cluster", row.clusterName);
      this.$router.push({
        name: "ClusterDetail",
        path: "/cluster/detail",
        query: {
          clusterName: row.clusterName
        }
      });
    }
  },
  mounted() {
    this.queryClusterList();
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
/* .stateLevel .health i {
  background-color: #18af28;
}
.stateLevel .health i {
  background-color: #fdb620;
}
.stateLevel .normal i {
  background-color: #f35e5e;
} */

.stateLevel.health {
  color: #18af28;
  padding: 4px 7px;
  border-radius: 4px;
  border: 1px solid #18af28;
}
.stateLevel.emergent {
  color: #f35e5e;
  padding: 4px 7px;
  border-radius: 4px;
  border: 1px solid #f35e5e;
}
.state-style.normal {
  color: #fdb620;
  padding: 4px 7px;
  border-radius: 4px;
  border: 1px solid #fdb620;
}
</style>
