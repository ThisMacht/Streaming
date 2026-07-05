# Conclusion

The project meets the major Lab 04 requirements: reproducible source discovery, one-file-at-a-time
CPG parsing, four Kafka event contracts, direct Kafka Connector ingestion into Neo4j, Spark
Structured Streaming metadata ingestion into MongoDB, and controlled modified-file replay.

## Results and evidence

- Topic creation, connector state, database indexes, pipeline runs, replay, checkpoint resume, and
  tests are recorded under `book/logs/`.
- Captured node, edge, metadata, and parser-error key/value messages are under `book/kafka/`.
- `book/images/` includes Neo4j Browser, MongoDB, Spark Structured Streaming, replay, and
  duplicate-check views.
- The modified probe changes from 14 nodes / 27 edges to 14 nodes / 26 edges in Neo4j; MongoDB
  updates the same metadata document to the final 14-node / 26-edge state.
- Duplicate Neo4j node-ID and edge-ID groups are both zero.
- Checkpoint resume retains 99 metadata documents before and after.
- The automated suite reports 19 passing tests.

## Limitations

AST extraction uses Python's standard `ast`; CFG, DFG, and CALL edges are educational
approximations rather than complete semantic analysis. Modified-file graph replacement uses a
file-scoped verification cleanup protocol and does not preserve graph history. More end-to-end,
failure-recovery, and connector integration tests would be appropriate for production readiness.

## Final assessment

Project scripts make the workflow reproducible, while this Jupyter Book combines the reasoning,
executed notebook output, Kafka samples, database query results, UI figures, and raw evidence needed
to audit it. Remaining submission work is operational: execute the notebook if its stored outputs
need refreshing, deploy the built book to GitHub Pages, and verify the public URL.
