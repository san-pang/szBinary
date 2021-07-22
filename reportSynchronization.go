package szBinary

type ReportSynchronization struct {
	NoPartitions uint32
	PartitionSyncs []*PartitionSync
}

type PartitionSync struct {
	PartitionNo uint32
	ReportIndex uint64
}
