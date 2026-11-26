package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"bulkdoc/internal/domain"
)

// CreateBatch validates the input and stores the batch with all its documents
// as PENDING in one atomic repository call.
func (s *Service) CreateBatch(ctx context.Context, batchNo string, docs []domain.DocumentInput) (domain.Batch, []domain.Document, error) {
	batchNo = strings.TrimSpace(batchNo)
	if batchNo == "" {
		return domain.Batch{}, nil, fmt.Errorf("%w: batch number is required", domain.ErrInvalidInput)
	}
	if len(docs) == 0 {
		return domain.Batch{}, nil, fmt.Errorf("%w: a batch needs at least one document", domain.ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(docs))
	for i, d := range docs {
		if strings.TrimSpace(d.ExternalRef) == "" {
			return domain.Batch{}, nil, fmt.Errorf("%w: document %d has no external_ref", domain.ErrInvalidInput, i)
		}
		if _, dup := seen[d.ExternalRef]; dup {
			return domain.Batch{}, nil, fmt.Errorf("%w: duplicate external_ref %q", domain.ErrInvalidInput, d.ExternalRef)
		}
		seen[d.ExternalRef] = struct{}{}
		if len(d.Payload) > 0 && !json.Valid(d.Payload) {
			return domain.Batch{}, nil, fmt.Errorf("%w: payload of %q is not valid JSON", domain.ErrInvalidInput, d.ExternalRef)
		}
	}

	batch, created, err := s.repo.CreateBatch(ctx, batchNo, docs)
	if err != nil {
		return domain.Batch{}, nil, fmt.Errorf("create batch %q: %w", batchNo, err)
	}
	s.logger.InfoContext(ctx, "batch created", "batch_id", batch.ID, "batch_no", batchNo, "documents", len(created))
	return batch, created, nil
}
