<template>
  <div class="cluster volume">
    <el-breadcrumb separator=">">
      <el-breadcrumb-item>
        <a href="javascript:;" @click="goFile()">{{
          $t("chubaoFS.tools.Depot")
        }}</a>
      </el-breadcrumb-item>
      <el-breadcrumb-item :key="index" v-for="(item, index) in crumbList">
        <a href="javascript:;" @click="goFile(item)"> {{ item.name }}</a>
      </el-breadcrumb-item>
    </el-breadcrumb>
    <div class="volume-right">
      <el-button type="primary" class="ml5" @click="clearAll"
        >{{ $t("chubaoFS.tools.ClearAll") }}
      </el-button>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table :data="resData.resLists" class="mt10" style="width: 100%">
        <el-table-column type="index" label="#"></el-table-column>
        <el-table-column prop="name" :label="$t('chubaoFS.depot.VolumeName')">
          <template slot-scope="scope">
            <div
              v-if="scope.row.type == 1"
              class="volume-name"
              @click="goDetail(scope.row)"
            >
              {{ scope.row.name }}
            </div>
            <span v-else>{{ scope.row.name }}</span>
          </template>
        </el-table-column>

        <el-table-column prop="type" :label="$t('chubaoFS.depot.Type')">
          <template slot-scope="scope">
            <span v-if="scope.row.type == 0">
              {{ $t("chubaoFS.depot.File") }}</span
            >
            <span v-if="scope.row.type == 1"
              >{{ $t("chubaoFS.depot.Directory") }}
            </span>
          </template>
        </el-table-column>
        <el-table-column
          prop="timestamp"
          :label="$t('chubaoFS.depot.DeleteTime')"
        >
          <template slot-scope="scope">
            <div class="">
              {{ scope.row.timestamp | dateFormat }}
            </div>
          </template>
        </el-table-column>
        <el-table-column
          prop="from"
          :label="$t('chubaoFS.depot.DeleteIp')"
        ></el-table-column>
        <el-table-column prop :label="$t('chubaoFS.tools.Actions')">
          <template slot-scope="scope">
            <el-button
              type="text"
              class="text-btn"
              @click="recover(scope.row)"
              >{{ $t("chubaoFS.tools.Recover") }}</el-button
            >
          </template>
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
          :total="resData.page.totalRecord"
        ></el-pagination>
        <span class="fr page-tips pr10">{{
          $t("chubaoFS.commonTxt.eachPageShows")
        }}</span>
      </div>
    </div>
  </div>
</template>

<script>
import baseGql from "../../graphql/depot";
import moment from "moment";

export default {
  name: "depot",
  data() {
    return {
      crumbList: [],
      volume: undefined,
      crumbItem: [],
      path: "/",
      inode: 1,
      crumbInfo: "",
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
      },
      volumeList: [],
      accessKey: "",
      secretKey: "",
      userID: null,
      permissionRules: {
        userName: [
          {
            required: true,
            message: this.$t("chubaoFS.volume.userNameMsg"),
            trigger: "blur"
          }
        ],
        access: [
          {
            required: true,
            message: this.$t("chubaoFS.volume.accessMsg"),
            trigger: "change"
          }
        ]
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
    clearAll() {
      const variables = {
        volume: this.volume
      };
      this.$confirm(
        this.$t("chubaoFS.DepotManagement.ClearAll") +
          // " " +
          // row.name +
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
            .mutation(this.url.file, baseGql.ClearAll, variables)
            .then(res => {
              if (res.data.clearTrash.code === 0) {
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
    recover(row) {
      const arry = this.crumbList.map(item => {
        return item.name;
      });
      const path = "/" + arry.join("/") + "/" + row.name;
      const variables = {
        volume: this.volume,
        path: path
      };
      this.$confirm(this.$t("chubaoFS.DepotManagement.Recover"), {
        confirmButtonText: this.$t("chubaoFS.tools.Yes"),
        cancelButtonText: this.$t("chubaoFS.tools.No"),
        type: "warning"
      })
        .then(() => {
          this.apollo
            .mutation(this.url.file, baseGql.Recover, variables)
            .then(res => {
              if (res.data.recoverPath.code === 0) {
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
    handleSizeChange(val) {
      this.resData.page.pageSize = val;
      this.resData.page.pageNo = 1;
      this.handleCurrentChange(1);
    },
    handleCurrentChange(val) {
      this.resData.page.pageNo = val;
      const start = (val - 1) * this.resData.page.pageSize;
      const end = val * this.resData.page.pageSize;
      this.resData.resLists = this.volumeList.slice(start, end);
    },
    getUserInfo() {
      const variables = {
        userID: this.userID
      };
      this.apollo
        .query(this.url.user, baseGql.getUserInfo, variables)
        .then(res => {
          if (res) {
            this.accessKey = res.data.getUserInfo.access_key;
            this.secretKey = res.data.getUserInfo.secret_key;
          }
        })
        .catch(error => {
          this.resData.loading = false;
          this.$Message.error(error);
        });
    },
    goFile(row, index) {
      if (row) {
        this.crumbList.splice(index);
        console.log(123, this.crumbList);
        this.queryDepotList(row);
      } else {
        this.crumbList = [];
        this.queryDepotList();
      }
    },
    queryDepotList(row) {
      this.resData.loading = true;
      const variables = {
        volume: this.volume,
        path: row ? row.name : this.path,
        inode: row ? row.inode : this.inode
      };

      this.apollo
        .query(this.url.file, baseGql.queryFileList, variables)
        .then(res => {
          this.resData.loading = false;
          if (res.data) {
            this.volumeList = res.data.listTrash.deletedDentry.concat(
              res.data.listTrash.dentry
            );
            if (row) {
              this.crumbList.push({ name: row.name, inode: row.inode });
              console.log(456, this.crumbList);
            }
            this.resData.page.totalRecord = this.volumeList.length;
            this.handleCurrentChange(1);
          } else {
            this.$message.error(res.message);
          }
        })
        .catch(error => {
          this.resData.loading = false;
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
      this.queryDepotList(row);
    }
  },
  mounted() {
    let query = this.$route.query;
    this.volume = query.volumeName;
    this.userID = sessionStorage.getItem("access_userID");
    this.queryDepotList();
  }
};
</script>

<style scoped>
.volume p {
  margin-top: 30px;
  font-size: 13px;
}

.volume-right {
  display: inline-block;
  float: right;
}

.volume-name {
  cursor: pointer;
  color: #466be4;
}
</style>
