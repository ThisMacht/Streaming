db = db.getSiblingDB("cpg_lab");

// Migrate the earlier unique repo/file index to the metadata_id strategy.
if (db.source_metadata.getIndexes().some(index => index.name === "repo_name_1_file_path_1")) {
  db.source_metadata.dropIndex("repo_name_1_file_path_1");
}

db.source_metadata.createIndex(
  { metadata_id: 1 },
  { unique: true }
);

db.source_metadata.createIndex(
  { repo_name: 1, file_path: 1 }
);

db.source_metadata.createIndex(
  { file_hash: 1 }
);

db.source_metadata.createIndex(
  { event_time: -1 }
);
