archive.entryType.block = archive.EntryType;
archive.entryType.proof = archive.EntryType;
archive.entryType.queueDiff = archive.EntryType;

archive.prefix = archive.Prefix;

archive.entryHeader
    shard_ident:block.shardIdent
    seqno:int
    root_hash:int256
    file_hash:int256
    type:archive.EntryType
    data_len:int
    = archive.EntryHeader;
