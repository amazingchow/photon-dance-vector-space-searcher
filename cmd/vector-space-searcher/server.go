package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/amazingchow/photon-dance-vector-space-searcher/api"
	conf "github.com/amazingchow/photon-dance-vector-space-searcher/internal/config"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/pipeline"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/utils"
)

type QueryServiceServer struct {
	container *pipeline.MOFRPCContainer
}

func NewQueryServiceServer(cfg *conf.PipelineConfig) *QueryServiceServer {
	return &QueryServiceServer{
		container: pipeline.NewMOFRPCContainer(cfg),
	}
}

// Query 查询服务接口.
func (qss *QueryServiceServer) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if len(req.GetQuery()) == 0 || req.GetTopk() == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid input")
	}

	docs, err := qss.container.Query(ctx, req.GetTopk(), req.GetQuery())
	if err != nil {
		if err == utils.ErrServiceUnavailable {
			return nil, status.Errorf(codes.Unavailable, err.Error())
		} else if err == utils.ErrContextDone {
			return nil, status.Errorf(codes.DeadlineExceeded, err.Error())
		}
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pb.QueryResponse{
		Docs: docs,
	}, nil
}

// GetSystemInfo 获取系统信息接口.
func (qss *QueryServiceServer) GetSystemInfo(ctx context.Context, req *pb.GetSystemInfoRequest) (*pb.GetSystemInfoResponse, error) {
	info, err := qss.container.GetSystemInfo()
	if err != nil {
		if err == utils.ErrServiceUnavailable {
			return nil, status.Errorf(codes.Unavailable, err.Error())
		} else if err == utils.ErrContextDone {
			return nil, status.Errorf(codes.DeadlineExceeded, err.Error())
		}
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return info, nil
}
