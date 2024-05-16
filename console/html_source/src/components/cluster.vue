<template>
  <div class="">
    <span>集群：</span>
    <el-select v-model="clusterName" @change="changeCluster">
      <el-option
        v-for="item in clusterList"
        :value="item.clusterName"
        :label="item.clusterNameZH"
        :key="item.clusterName"
      ></el-option>
    </el-select>
  </div>
</template>

<script>
import baseGql from "../graphql/cluster";

export default {
  name: "Cluster",
  data() {
    return {
      clusterName: undefined,
      clusterList: []
    };
  },
  computed: {},
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
    changeCluster() {
      const variables = {
        cluster: this.clusterName
      };
      this.apollo
        .mutation(this.url.consoleCluster, baseGql.switchCluster, variables)
        .then(res => {
          if (
            res.data &&
            res.data.switchCluster &&
            res.data.switchCluster.code == 0
          ) {
            this.$message.success("切换成功");

            sessionStorage.setItem("cluster", this.clusterName);
            // this.$router.push(
            //   `${this.$route.path}?clusterName=${this.clusterName}`
            // );
            this.$router.push({ query: { clusterName: this.clusterName } });
            this.$emit("refresh");
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
    this.queryClusterList();
  }
};
</script>

<style scoped></style>
