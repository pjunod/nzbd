# Reviewed fuzz seeds

`metainfo_preflight/` contains small, committed inputs whose exact admission
outcomes are pinned by `fuzz/tests/corpus_contract.rs`. Keep these inputs named,
reviewable, and stable.

`cargo fuzz` writes newly discovered inputs under `fuzz/corpus/`, which remains
ignored. Promote an evolved input here only when it represents a useful,
named contract class and add the corresponding assertion first.
