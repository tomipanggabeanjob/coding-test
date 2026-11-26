package service

import (
	"context"
	"fmt"

	"bulkdoc/internal/domain"
)

// GetBatchStatus returns per-status counts (zeros included) and the derived
// batch status, read as one consistent snapshot.
func (s *Service) GetBatchStatus(ctx context.Context, batchID string) (domain.BatchStatusView, error) {
	view, err := s.repo.BatchStatus(ctx, batchID)
	if err != nil {
		return domain.BatchStatusView{}, fmt.Errorf("get batch status %s: %w", batchID, err)
	}
	return view, nil
}

// ListDocuments returns the documents of a batch ordered by ID.
func (s *Service) ListDocuments(ctx context.Context, batchID string) ([]domain.Document, error) {
	docs, err := s.repo.ListDocuments(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("list documents of batch %s: %w", batchID, err)
	}
	return docs, nil
}

// GetDocument returns one document.
func (s *Service) GetDocument(ctx context.Context, docID string) (domain.Document, error) {
	d, err := s.repo.GetDocument(ctx, docID)
	if err != nil {
		return domain.Document{}, fmt.Errorf("get document %s: %w", docID, err)
	}
	return d, nil
}
