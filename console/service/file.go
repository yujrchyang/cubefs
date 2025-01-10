package service

import (
	"context"
	"encoding/json"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/console/service/file"
	. "github.com/cubefs/cubefs/objectnode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type FileService struct {
	objectManager *file.ObjectManager // 文件列表管理
	trashManager  *file.TrashManager  // 回收站管理
}

func NewFileService(clusters []*model.ConsoleCluster) *FileService {
	return &FileService{
		objectManager: file.NewObjectManager(clusters),
		trashManager:  file.NewTrashManager(clusters),
	}
}

func (fs *FileService) SType() cproto.ServiceType {
	return cproto.FileService
}
func (fs *FileService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	fs.registerObject(schema)
	fs.registerQuery(schema)
	fs.registerMutation(schema)
	return schema.MustBuild()
}

func (fs *FileService) registerObject(schema *schemabuilder.Schema) {
	object := schema.Object("FSFileInfo", FSFileInfo{})
	object.FieldFunc("FSFileInfo", func(f *FSFileInfo) []*KeyValue {
		list := make([]*KeyValue, 0, len(f.Metadata))
		for k, v := range f.Metadata {
			list = append(list, &KeyValue{Key: k, Value: v})
		}
		return list
	})
}

func (fs *FileService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()

	query.FieldFunc("fileMeta", fs.fileMeta)
	query.FieldFunc("listFile", fs.listFile)
	query.FieldFunc("listTrash", fs.listTrash)
}

func (fs *FileService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()

	mutation.FieldFunc("signURL", fs.signURL)
	mutation.FieldFunc("createDir", fs.createDir)
	mutation.FieldFunc("deleteDir", fs.deleteDir)
	mutation.FieldFunc("deleteFile", fs.deleteFile)
	mutation.FieldFunc("recoverPath", fs.recoverPath)
	mutation.FieldFunc("clearTrash", fs.clearTrash)
}

func (fs *FileService) DoXbpApply(apply *model.XbpApplyInfo) error {
	return nil
}

type volPerm int

const (
	none volPerm = iota
	read
	write
)

func (v volPerm) read() error {
	if v < read {
		return fmt.Errorf("do not have permission read")
	}
	return nil
}

func (v volPerm) write() error {
	if v < write {
		return fmt.Errorf("do not have permission write")
	}
	return nil
}

func (fs *FileService) fileMeta(ctx context.Context, args struct {
	VolName string
	Path    string
}) (info *FSFileInfo, err error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.UserID, args.VolName).read(); err != nil {
		return nil, err
	}

	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return nil, err
	}

	volume, err := manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}
	return volume.FileInfo(context.Background(), args.Path, false)
}

type ListFileInfo struct {
	Infos       []*FSFileInfo
	NextMarker  string
	IsTruncated bool
	Prefixes    []string
}

func (fs *FileService) listFile(ctx context.Context, args struct {
	VolName string
	Request ListFilesV1Option
}) (*ListFileInfo, error) {
	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.UserID, args.VolName).read(); err != nil {
		return nil, err
	}
	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return nil, err
	}
	volume, err := manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}

	result, err := volume.ListFilesV1(&args.Request)

	if err != nil {
		return nil, err
	}

	return &ListFileInfo{
		Infos:       result.Files,
		NextMarker:  result.NextMarker,
		IsTruncated: result.Truncated,
		Prefixes:    result.CommonPrefixes,
	}, err
}

type TrashListResponse struct {
	Dentry        []proto.Dentry
	DeletedDentry []*proto.DeletedDentry
}

func (fs *FileService) listTrash(ctx context.Context, args struct {
	Volume string
	Path   string
	Inode  uint64
}) (*TrashListResponse, error) {
	task := &file.TrashTask{
		Cluster: cutil.GlobalCluster,
		Volume:  args.Volume,
		Path:    args.Path,
		Inode:   args.Inode,
	}
	var (
		dentrys  []proto.Dentry
		ddentrys []*proto.DeletedDentry
		err      error
	)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		ddentrys, err = fs.trashManager.ListTrashTaskHandler(task)
		wg.Done()
	}()
	go func() {
		dentrys, err = fs.trashManager.ListDirTaskHandler(task)
		wg.Done()
	}()
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return &TrashListResponse{
		Dentry:        dentrys,
		DeletedDentry: ddentrys,
	}, nil

}

func (fs *FileService) recoverPath(ctx context.Context, args struct {
	Volume string
	Path   string
}) error {
	req := &file.TrashTask{
		Cluster: cutil.GlobalCluster,
		Volume:  args.Volume,
		Path:    args.Path,
	}
	isSuccess, err := fs.trashManager.RecoverTaskHandler(req)
	if err != nil || !isSuccess {
		return err
	}
	return nil
}

func (fs *FileService) clearTrash(ctx context.Context, args struct {
	Volume string
}) error {
	req := &file.TrashTask{
		TaskType: file.CleanTrash,
		Cluster:  cutil.GlobalCluster,
		Volume:   args.Volume,
	}
	//异步
	fs.trashManager.PutToTaskChan(req)
	//isSuccess, err := fs.trashManager.CleanTrashTaskHandler(req)
	//if err != nil || !isSuccess {
	//	return nil, err
	//}
	return nil
}

func (fs *FileService) createDir(ctx context.Context, args struct {
	VolName string
	Path    string
}) (*FSFileInfo, error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err = fs.userVolPerm(ctx, userInfo.UserID, args.VolName).write(); err != nil {
		return nil, err
	}

	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return nil, err
	}
	volume, err := manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}

	return volume.PutObject(args.Path, nil, &PutFileOption{
		MIMEType: HeaderValueContentTypeDirectory,
		Tagging:  nil,
		Metadata: nil,
	})
}

func (fs *FileService) deleteDir(ctx context.Context, args struct {
	VolName string
	Path    string
}) (err error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return
	}

	if err = fs.userVolPerm(ctx, userInfo.UserID, args.VolName).write(); err != nil {
		return
	}

	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return
	}

	volume, err := manager.Volume(args.VolName)
	if err != nil {
		return
	}

	if err = _deleteDir(ctx, volume, args.Path, ""); err != nil {
		return
	}

	if err = volume.DeletePath(args.Path + "/"); err != nil {
		return
	}
	return nil
}

func _deleteDir(ctx context.Context, volume *Volume, path string, marker string) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("time out")
	default:
	}

	for {
		result, err := volume.ListFilesV1(&ListFilesV1Option{
			Prefix:    path,
			Delimiter: "/",
			Marker:    marker,
			MaxKeys:   10000,
		})

		for _, prefixe := range result.CommonPrefixes {
			if err := _deleteDir(ctx, volume, prefixe, ""); err != nil {
				return err
			}
			if err := volume.DeletePath(prefixe + "/"); err != nil {
				return err
			}
		}

		for _, info := range result.Files {
			if err := volume.DeletePath(info.Path); err != nil {
				return err
			}
		}

		if err != nil {
			return err
		}

		if !result.Truncated {
			return nil
		}

		marker = result.NextMarker
	}

}

func (fs *FileService) deleteFile(ctx context.Context, args struct {
	VolName string
	Path    string
}) (err error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return
	}

	if err = fs.userVolPerm(ctx, userInfo.UserID, args.VolName).write(); err != nil {
		return
	}

	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return
	}

	volume, err := manager.Volume(args.VolName)
	if err != nil {
		return
	}

	err = volume.DeletePath(args.Path)
	return
}

func (fs *FileService) signURL(ctx context.Context, args struct {
	VolName       string
	Path          string
	ExpireMinutes int64
}) (link string, err error) {

	endPoint, err := fs.objectManager.GetS3EndPoint(cutil.GlobalCluster)
	if err != nil {
		return
	}

	if args.Path == "" || args.ExpireMinutes <= 0 || args.VolName == "" {
		return "", fmt.Errorf("param has err")
	}

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return
	}

	if err = fs.userVolPerm(ctx, userInfo.UserID, args.VolName).read(); err != nil {
		return
	}

	sdkConfig := aws.NewConfig()
	sdkConfig.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(userInfo.AccessKey, userInfo.SecretKey, ""))
	s3Client := s3.NewFromConfig(*sdkConfig, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endPoint)
	})
	presignerClient := s3.NewPresignClient(s3Client)
	request, err := presignerClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(args.VolName),
		Key:    aws.String(args.Path),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(args.ExpireMinutes) * time.Minute
	})
	if err != nil {
		return "", err
	}
	return request.URL, nil
}

// ?vol_name=abc&path=/aaa/bbb/ddd.txt
func (fs *FileService) DownFile(writer http.ResponseWriter, request *http.Request) error {
	if err := request.ParseForm(); err != nil {
		return err
	}

	token, err := cutil.Token(request)
	if err != nil {
		return err
	}

	info, err := cutil.TokenValidate(token)
	if err != nil {
		return err
	}
	ctx := context.WithValue(request.Context(), proto.UserKey, info.UserID)

	volNames := request.Form["vol_name"]
	var volName string
	if len(volNames) > 0 {
		volName = volNames[0]
	} else {
		return fmt.Errorf("not found path in get param ?vol_name=[your path]")
	}

	//author validate
	if err := fs.userVolPerm(ctx, info.UserID, volName).read(); err != nil {
		return fmt.Errorf("the user does not have permission to access this volume")
	}

	var path string
	paths := request.Form["path"]
	if len(paths) > 0 {
		path = paths[0]
	} else {
		return fmt.Errorf("not found path in get param ?path=[your path]")
	}

	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return err
	}

	volume, err := manager.Volume(volName)
	if err != nil {
		return err
	}

	reader, err := volume.FileReader(context.Background(), path, true)
	if err != nil {
		return err
	}

	defer func() {
		_ = reader.Close()
	}()

	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.FormatInt(reader.FileInfo().Size, 10))

	if _, err := reader.WriteTo(writer, 0, uint64(reader.FileInfo().Size)); err != nil {
		return err
	}
	return nil
}

// ?vol_name=abc&path=/aaa/bbb/ddd.txt {file}
func (fs *FileService) UpLoadFile(writer http.ResponseWriter, request *http.Request) error {
	if err := request.ParseForm(); err != nil {
		return err
	}

	token, err := cutil.Token(request)
	if err != nil {
		return err
	}

	info, err := cutil.TokenValidate(token)
	if err != nil {
		return err
	}
	ctx := context.WithValue(request.Context(), proto.UserKey, info.UserID)

	volNames := request.Form["vol_name"]
	var volName string
	if len(volNames) > 0 {
		volName = volNames[0]
	} else {
		return fmt.Errorf("not found path in get param ?vol_name=[your path]")
	}

	//author validate
	if err := fs.userVolPerm(ctx, info.UserID, volName).write(); err != nil {
		return fmt.Errorf("the user:[%s] does not have permission to access this volume:[%s]", info.UserID, volName)
	}

	var path string
	paths := request.Form["path"]
	if len(paths) > 0 {
		path = paths[0]
	} else {
		return fmt.Errorf("not found path in get param ?path=[your path]")
	}

	manager, err := fs.objectManager.GetVolManager(cutil.GlobalCluster)
	if err != nil {
		return err
	}
	volume, err := manager.Volume(volName)
	if err != nil {
		return err
	}

	file, header, err := request.FormFile("file")
	if err != nil {
		return fmt.Errorf("get file from request has err:[%s]", err.Error())
	}

	filepath := path + "/" + header.Filename

	object, err := volume.PutObject(filepath, file, &PutFileOption{
		MIMEType: header.Header.Get("Content-Type"),
		Tagging:  nil,
		Metadata: nil,
	})

	if err != nil {
		return fmt.Errorf("put to object has err:[%s]", err.Error())
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("charset", "utf-8")

	v, e := json.Marshal(object)
	if e != nil {
		return e
	}
	_, _ = writer.Write(v)

	return nil
}

type KeyValue struct {
	Key   string
	Value string
}

func (fs *FileService) userVolPerm(ctx context.Context, userID string, vol string) volPerm {
	userClient := fs.objectManager.GetUserClient(cutil.GlobalCluster)

	userInfo, err := userClient.UserAPI().GetUserInfo(userID)
	if err != nil {
		log.LogErrorf("found user by id:[%s] has err:[%s]", userID, err.Error())
		return none
	}

	policy := userInfo.Policy

	for _, ov := range policy.OwnVols {
		if ov == vol {
			return write
		}
	}

	pm := none

	for av, authorized := range policy.AuthorizedVols {
		if av == vol {
			for _, a := range authorized {
				if strings.Contains(a, "ReadOnly") {
					if pm < read {
						pm = read
					}
				}
				if strings.Contains(a, "Write") {
					return write
				}
			}
		}
	}
	return pm
}
