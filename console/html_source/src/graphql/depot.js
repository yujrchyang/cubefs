import gql from "graphql-tag"; // 引入graphql
const baseGql = {
  getUserInfo: gql`
    query getUserInfo($userID: String) {
      getUserInfo(userID: $userID) {
        access_key
        secret_key
      }
    }
  `,
  ClearAll: gql`
    mutation clearTrash($volume: String) {
      clearTrash(volume: $volume) {
        code
        message
      }
    }
  `,
  Recover: gql`
    mutation recoverPath($volume: String, $path: String) {
      recoverPath(volume: $volume, path: $path) {
        code
        message
      }
    }
  `,
  queryFileList: gql`
    query listTrash($inode: Number, $path: String, $volume: String) {
      listTrash(path: $path, volume: $volume, inode: $inode) {
        dentry {
          name
          inode
          type
        }
        deletedDentry {
          name
          inode
          type
          timestamp
          from
        }
      }
    }
  `
};

export default baseGql;
