package service

import (
	"context"
	"errors"

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

func (s StoreService) Delete(ctx context.Context, in *statev1pb.DeleteRequest) (*emptypb.Empty, error) {
	req := state.DeleteRequest{
		Key:      in.Key,
		Metadata: in.Metadata,
	}
	if in.Etag != nil {
		req.ETag = &in.Etag.Value
	}
	if in.Options != nil {
		req.Options = state.DeleteStateOption{
			Consistency: stateConsistencyToString(in.Options.Consistency),
			Concurrency: stateConcurrencyToString(in.Options.Concurrency),
		}
	}
	err := s.store.Delete(&req)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) Get(ctx context.Context, in *statev1pb.GetRequest) (*statev1pb.GetResponse, error) {
	req := state.GetRequest{
		Key:      in.Key,
		Metadata: in.Metadata,
		Options: state.GetStateOption{
			Consistency: stateConsistencyToString(in.Consistency),
		},
	}

	getResponse, err := s.store.Get(&req)
	if err != nil {
		return nil, err
	}

	res := statev1pb.GetResponse{}
	if getResponse != nil {
		res.Etag = &common.Etag{
			Value: stringValueOrEmpty(getResponse.ETag),
		}
		res.Data = getResponse.Data
		res.Metadata = getResponse.Metadata
	}

	return &res, nil
}

func (s StoreService) Set(ctx context.Context, in *statev1pb.SetRequest) (*emptypb.Empty, error) {
	// TODO: Do we need to decode the data?
	req := state.SetRequest{
		Key:      in.Key,
		Value:    in.Value,
		Metadata: in.Metadata,
	}
	if in.Etag != nil {
		req.ETag = &in.Etag.Value
	}
	if in.Options != nil {
		req.Options = state.SetStateOption{
			Concurrency: stateConcurrencyToString(in.Options.Concurrency),
			Consistency: stateConsistencyToString(in.Options.Consistency),
		}
	}

	err := s.store.Set(&req)
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

func (s StoreService) BulkDelete(ctx context.Context, in *statev1pb.BulkDeleteRequest) (*emptypb.Empty, error) {
	reqs := make([]state.DeleteRequest, 0, len(in.Items))
	for _, items := range in.Items {
		req := state.DeleteRequest{
			Key:      items.Key,
			Metadata: items.Metadata,
		}
		if items.Etag != nil {
			req.ETag = &items.Etag.Value
		}
		if items.Options != nil {
			req.Options = state.DeleteStateOption{
				Concurrency: stateConcurrencyToString(items.Options.Concurrency),
				Consistency: stateConsistencyToString(items.Options.Consistency),
			}
		}
		reqs = append(reqs, req)
	}
	err := s.store.BulkDelete(reqs)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s StoreService) BulkGet(ctx context.Context, in *statev1pb.BulkGetRequest) (*statev1pb.BulkGetResponse, error) {
	reqs := make([]state.GetRequest, len(in.Items))
	for _, i := range in.Items {
		reqs = append(reqs, state.GetRequest{
			Key:      i.Key,
			Metadata: i.Metadata,
			Options: state.GetStateOption{
				Consistency: stateConsistencyToString(i.Consistency),
			},
		})
	}

	bulkGetResp := &statev1pb.BulkGetResponse{}
	if len(in.Items) == 0 {
		return bulkGetResp, nil
	}

	bulkGet, responses, err := s.store.BulkGet(reqs)
	if bulkGet {
		if err != nil {
			return nil, err
		}
		for _, r := range responses {
			item := &statev1pb.BulkStateItem{
				Key:  r.Key,
				Data: r.Data,
				Etag: &common.Etag{
					Value: stringValueOrEmpty(r.ETag),
				},
				Metadata: r.Metadata,
				Error:    r.Error,
			}
			bulkGetResp.Items = append(bulkGetResp.Items, item)
		}
		return bulkGetResp, nil
	}

	// TODO: Implement fallback...
	return nil, errors.New("bulk get not supported")
}

func (s StoreService) BulkSet(ctx context.Context, in *statev1pb.BulkSetRequest) (*emptypb.Empty, error) {
	reqs := make([]state.SetRequest, len(in.Items))
	for _, i := range in.Items {
		// TODO: Do we need to decode the data?
		req := state.SetRequest{
			Key:      i.Key,
			Metadata: i.Metadata,
			Value:    i.Value,
		}
		if i.Etag != nil {
			req.ETag = &i.Etag.Value
		}
		if i.Options != nil {
			req.Options = state.SetStateOption{
				Concurrency: stateConcurrencyToString(i.Options.Concurrency),
				Consistency: stateConsistencyToString(i.Options.Consistency),
			}
		}

		reqs = append(reqs, req)
	}

	err := s.store.BulkSet(reqs)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func stateConsistencyToString(c common.StateOptions_StateConsistency) string {
	switch c {
	case common.StateOptions_CONSISTENCY_EVENTUAL:
		return "eventual"
	case common.StateOptions_CONSISTENCY_STRONG:
		return "strong"
	}

	return ""
}

func stateConcurrencyToString(c common.StateOptions_StateConcurrency) string {
	switch c {
	case common.StateOptions_CONCURRENCY_FIRST_WRITE:
		return "first-write"
	case common.StateOptions_CONCURRENCY_LAST_WRITE:
		return "last-write"
	}

	return ""
}

func stringValueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}
