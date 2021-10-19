package service

import (
	"context"

	"github.com/dapr/components-contrib/state"
	statev1pb "github.com/dapr/components-contrib/state/proto/v1"
	common "github.com/dapr/dapr/pkg/proto/common/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type StoreService struct {
	store state.Store
}

func NewStoreService(store state.Store) *StoreService {
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
		Metadata: delReqPb.Metadata,
	}

	if delReqPb.Options != nil {
		delReq.Options = state.DeleteStateOption{
			Concurrency: pbToConcurrency(delReqPb.Options.Concurrency),
			Consistency: pbToConsistency(delReqPb.Options.Consistency),
		}
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
			Consistency: pbToConsistency(getReqPb.Consistency),
		},
	}
	getRes, err := s.store.Get(&getReq)
	if err != nil {
		return nil, err
	}

	getResPb := statev1pb.GetResponse{
		Data:     getRes.Data,
		Metadata: getRes.Metadata,
	}

	if getRes.ETag != nil {
		getResPb.Etag = &common.Etag{
			Value: *getRes.ETag,
		}
	}
	return &getResPb, nil
}

func (s StoreService) Set(ctx context.Context, setReqPb *statev1pb.SetRequest) (*emptypb.Empty, error) {
	setReq := state.SetRequest{
		Key:      setReqPb.Key,
		Value:    setReqPb.Value, // TODO: Fix data encoding/decoding.
		Metadata: setReqPb.Metadata,
	}

	if setReqPb.Options != nil {
		setReq.Options = state.SetStateOption{
			Concurrency: pbToConcurrency(setReqPb.Options.Concurrency),
			Consistency: pbToConsistency(setReqPb.Options.Consistency),
		}
	}

	if setReqPb.Etag != nil {
		setReq.ETag = &setReqPb.Etag.Value
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
	items := make([]state.DeleteRequest, len(bulkDelReqPb.Items))
	for _, i := range bulkDelReqPb.Items {
		item := state.DeleteRequest{
			Key:      i.Key,
			Metadata: i.Metadata,
		}

		if i.Options != nil {
			item.Options = state.DeleteStateOption{
				Concurrency: pbToConcurrency(i.Options.Concurrency),
				Consistency: pbToConsistency(i.Options.Consistency),
			}
		}

		if i.Etag != nil {
			item.ETag = &i.Etag.Value
		}
		items = append(items, item)
	}

	err := s.store.BulkDelete(items)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) BulkGet(ctx context.Context, bulkGetReqPb *statev1pb.BulkGetRequest) (*statev1pb.BulkGetResponse, error) {
	reqItems := make([]state.GetRequest, len(bulkGetReqPb.Items))
	for _, i := range bulkGetReqPb.Items {
		reqItems = append(reqItems, state.GetRequest{
			Key:      i.Key,
			Metadata: i.Metadata,
			Options: state.GetStateOption{
				Consistency: pbToConsistency(i.Consistency),
			},
		})
	}

	got, getRes, err := s.store.BulkGet(reqItems)
	if err != nil {
		return nil, err
	}

	resItems := make([]*statev1pb.BulkStateItem, len(getRes))
	for _, r := range getRes {
		item := &statev1pb.BulkStateItem{
			Data:     r.Data,
			Metadata: r.Metadata,
			Key:      r.Key,
			Error:    r.Error,
		}

		if r.ETag != nil {
			item.Etag = &common.Etag{
				Value: *r.ETag,
			}
		}
		resItems = append(resItems, item)
	}

	return &statev1pb.BulkGetResponse{
		Items: resItems,
		Got:   got,
	}, nil
}

func (s StoreService) BulkSet(ctx context.Context, bulkSetReqPb *statev1pb.BulkSetRequest) (*emptypb.Empty, error) {
	items := make([]state.SetRequest, len(bulkSetReqPb.Items))
	for _, i := range bulkSetReqPb.Items {
		item := state.SetRequest{
			Key:      i.Key,
			Metadata: i.Metadata,
			Value:    i.Value, // TODO: Fix data encoding/decoding.
		}

		if i.Options != nil {
			item.Options = state.SetStateOption{
				Concurrency: pbToConcurrency(i.Options.Concurrency),
				Consistency: pbToConsistency(i.Options.Consistency),
			}
		}

		items = append(items, item)

	}

	err := s.store.BulkSet(items)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// TODO: Do this in a better way.
func pbToConcurrency(concurrency common.StateOptions_StateConcurrency) string {
	switch concurrency.Enum() {
	case common.StateOptions_CONCURRENCY_FIRST_WRITE.Enum():
		return "first_write"
	case common.StateOptions_CONCURRENCY_LAST_WRITE.Enum():
		return "last_write"
	case common.StateOptions_CONCURRENCY_UNSPECIFIED.Enum():
	default:
		return ""
	}

	return ""
}

// TODO: Do this in a better way.
func pbToConsistency(consistency common.StateOptions_StateConsistency) string {
	switch consistency.Enum() {
	case common.StateOptions_CONSISTENCY_EVENTUAL.Enum().Enum():
		return "eventual"
	case common.StateOptions_CONSISTENCY_STRONG.Enum().Enum():
		return "strong"
	case common.StateOptions_CONSISTENCY_UNSPECIFIED.Enum().Enum():
	default:
		return ""
	}

	return ""
}
