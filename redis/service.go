package redis

import (
	"context"

	"github.com/dapr/components-contrib/state"
	statev1pb "github.com/dapr/components-contrib/state/proto/v1"
	"github.com/dapr/components-contrib/state/redis"
	"google.golang.org/protobuf/types/known/emptypb"
)

type StoreService struct {
	store *redis.StateStore
}

func NewStoreService(store *redis.StateStore) *StoreService {
	return &StoreService{
		store: store,
	}
}

func (s StoreService) Init(ctx context.Context, mdReqPb *statev1pb.MetadataRequest) (*emptypb.Empty, error) {
	md := state.Metadata{
		Properties: mdReqPb.Properties,
	}
	err := s.store.Init(md)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s StoreService) Features(context.Context, *emptypb.Empty) (*statev1pb.FeaturesResponse, error) {
	features := s.store.Features()

	fs := make([]string, len(features))
	for _, f := range features {
		fs = append(fs, string(f))
	}

	featuresRes := statev1pb.FeaturesResponse{
		Feature: fs,
	}

	return &featuresRes, nil
}

func (s StoreService) Delete(ctx context.Context, delReqPb *statev1pb.DeleteRequest) (*emptypb.Empty, error) {
	delReq := state.DeleteRequest{
		Key:      delReqPb.Key,
		ETag:     &delReqPb.Etag,
		Metadata: delReqPb.Metadata,
		Options: state.DeleteStateOption{
			Concurrency: delReqPb.Concurrency,
		},
	}
	err := s.store.Delete(&delReq)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) Get(ctx context.Context, getReqPb *statev1pb.GetRequest) (*statev1pb.GetResponse, error) {
	getReq := state.GetRequest{
		Key:      getReqPb.Key,
		Metadata: getReqPb.Metadata,
		Options: state.GetStateOption{
			Consistency: getReqPb.Consistency,
		},
	}
	getRes, err := s.store.Get(&getReq)
	if err != nil {
		return nil, err
	}

	getResPb := statev1pb.GetResponse{
		Data:     getRes.Data,
		Etag:     *getRes.ETag,
		Metadata: getRes.Metadata,
	}
	return &getResPb, nil
}

func (s StoreService) Set(ctx context.Context, setReqPb *statev1pb.SetRequest) (*emptypb.Empty, error) {
	setReq := state.SetRequest{
		Key:      setReqPb.Key,
		Value:    setReqPb.Value,
		ETag:     &setReqPb.Etag,
		Metadata: setReqPb.Metadata,
		Options: state.SetStateOption{
			Concurrency: setReqPb.Concurrency,
			Consistency: setReqPb.Consistency,
		},
	}
	err := s.store.Set(&setReq)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) Ping(ctx context.Context, none *emptypb.Empty) (*emptypb.Empty, error) {
	err := s.store.Ping()
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) BulkDelete(ctx context.Context, bulkDelReqPb *statev1pb.BulkDeleteRequest) (*emptypb.Empty, error) {
	bulkDelReq := make([]state.DeleteRequest, len(bulkDelReqPb.Requests))
	for _, r := range bulkDelReqPb.Requests {
		bulkDelReq = append(bulkDelReq, state.DeleteRequest{
			Key:      r.Key,
			ETag:     &r.Etag,
			Metadata: r.Metadata,
			Options: state.DeleteStateOption{
				Concurrency: r.Concurrency,
				Consistency: r.Consistency,
			},
		})
	}

	err := s.store.BulkDelete(bulkDelReq)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) BulkGet(ctx context.Context, bulkGetReqPb *statev1pb.BulkGetRequest) (*statev1pb.BulkGetResponse, error) {
	bulkGetReq := make([]state.GetRequest, len(bulkGetReqPb.Requests))
	for _, r := range bulkGetReqPb.Requests {
		bulkGetReq = append(bulkGetReq, state.GetRequest{
			Key:      r.Key,
			Metadata: r.Metadata,
			Options: state.GetStateOption{
				Consistency: r.Consistency,
			},
		})
	}

	got, getRes, err := s.store.BulkGet(bulkGetReq)
	if err != nil {
		return nil, err
	}

	getResPb := make([]*statev1pb.GetResponseWithError, len(getRes))
	for _, g := range getRes {
		getResPb = append(getResPb, &statev1pb.GetResponseWithError{
			Data:     g.Data,
			Etag:     *g.ETag,
			Metadata: g.Metadata,
			Key:      g.Key,
			Error:    g.Error,
		})
	}

	return &statev1pb.BulkGetResponse{
		Responses: getResPb,
		Got:       got,
	}, nil
}

func (s StoreService) BulkSet(ctx context.Context, bulkSetReqPb *statev1pb.BulkSetRequest) (*emptypb.Empty, error) {
	bulkSetReq := make([]state.SetRequest, len(bulkSetReqPb.Requests))
	for _, r := range bulkSetReqPb.Requests {
		bulkSetReq = append(bulkSetReq, state.SetRequest{
			Key:      r.Key,
			Metadata: r.Metadata,
			Value:    r.Value,
			Options: state.SetStateOption{
				Concurrency: r.Concurrency,
				Consistency: r.Consistency,
			},
		})
	}

	err := s.store.BulkSet(bulkSetReq)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
