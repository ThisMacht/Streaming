CREATE CONSTRAINT cpg_node_id_unique IF NOT EXISTS
FOR (n:CPGNode)
REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT source_file_id_unique IF NOT EXISTS
FOR (f:SourceFile)
REQUIRE f.id IS UNIQUE;

CREATE CONSTRAINT repository_name_unique IF NOT EXISTS
FOR (r:Repository)
REQUIRE r.name IS UNIQUE;