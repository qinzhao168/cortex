package gcp

import "flag"

// GCSConfig is config for the GCS Chunk Client.
type GCSConfig struct {
	BucketName      string `yaml:"bucket_name"`
	ChunkBufferSize int    `yaml:"chunk_buffer_size"`
}

// RegisterFlagsWithPrefix registers flags.
func (cfg *GCSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucketname", "", "Name of GCS bucket to put chunks in.")
	f.IntVar(&cfg.ChunkBufferSize, prefix+"gcs.chunk-buffer-size", 0, "The size of the buffer that GCS client for each PUT request. 0 to disable buffering.")
}
