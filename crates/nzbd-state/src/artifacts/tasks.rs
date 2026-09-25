use super::*;

impl Inventory {
    /// Durable admission for filesystem inspection and recovery copying. A
    /// 202 response always names a persisted task, including before a restart.
    pub fn submit_task(
        &self,
        kind: &str,
        artifact: &str,
        request_id: &str,
        request: serde_json::Value,
    ) -> Result<Operation> {
        if !matches!(kind, "inspect" | "scan" | "stage") {
            return Err(Error::Conflict("unknown task kind".into()));
        }
        let _guard = self.mutation.lock().unwrap();
        let db = self.db.lock().unwrap();
        let key = if request_id.is_empty() {
            id(&db)?
        } else {
            request_id.into()
        };
        let request = serde_json::to_string(&request)?;
        match read::<Operation>(&db, "operations", &key) {
            Ok(op) => {
                return if op.request == request && op.kind == kind && op.artifact == artifact {
                    Ok(op)
                } else {
                    Err(Error::Conflict("idempotency key reused".into()))
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        let op = Operation {
            id: key,
            artifact: artifact.into(),
            kind: kind.into(),
            state: "queued".into(),
            request,
            created_at: now(),
            not_before: now(),
            attempts: 0,
            next_retry: 0,
            error: None,
        };
        save_operation(&db, &op)?;
        Ok(op)
    }
    pub fn execute_task(&self, key: &str) -> Result<Operation> {
        let mut op = self.operation(key)?;
        if !matches!(op.state.as_str(), "queued" | "running") {
            return Ok(op);
        }
        op.state = "running".into();
        op.attempts += 1;
        save_operation(&self.db.lock().unwrap(), &op)?;
        let request: serde_json::Value = serde_json::from_str(&op.request)?;
        let result = match op.kind.as_str() {
            "inspect" => self.inspect(&op.artifact).map(|_| ()),
            "stage" => (|| {
                let revision = request["revision"]
                    .as_u64()
                    .ok_or_else(|| Error::Conflict("missing revision".into()))?;
                let files: Vec<String> = serde_json::from_value(request["files"].clone())?;
                let root: PathBuf = serde_json::from_value(request["root"].clone())?;
                let recovery =
                    self.stage_recovery(&op.artifact, revision, &op.id, &files, &root)?;
                if recovery.state != "published" {
                    return Err(Error::Conflict(
                        recovery
                            .error
                            .unwrap_or_else(|| format!("recovery {}", recovery.state)),
                    ));
                }
                Ok(())
            })(),
            "scan" => (|| {
                let roots: Vec<PathBuf> = serde_json::from_value(request["roots"].clone())?;
                let excluded: Vec<PathBuf> = serde_json::from_value(request["excluded"].clone())?;
                let active: Vec<PathBuf> = serde_json::from_value(request["active"].clone())?;
                let mut count = 0;
                let mut failures = Vec::new();
                for root in roots {
                    if let Err(e) = fs::open_dir(&root) {
                        failures.push(format!("{}: {e}", root.display()));
                        continue;
                    }
                    for entry in std::fs::read_dir(&root)? {
                        if count >= 2000 {
                            return Err(Error::Conflict(
                                "scan reached 2000 directories; results are incomplete".into(),
                            ));
                        }
                        let entry = entry?;
                        let path = entry.path();
                        if excluded.iter().any(|p| path == *p || p.starts_with(&path)) {
                            continue;
                        }
                        if !entry.file_type()?.is_dir() {
                            continue;
                        }
                        count += 1;
                        self.discover(&root, &path, active.contains(&path))?;
                    }
                }
                if !failures.is_empty() {
                    return Err(Error::Conflict(failures.join("; ")));
                }
                Ok(())
            })(),
            _ => Err(Error::Conflict("unknown task kind".into())),
        };
        match result {
            Ok(()) => {
                op.state = "succeeded".into();
                op.error = None;
            }
            Err(e) => {
                op.state = "failed".into();
                op.error = Some(e.to_string());
            }
        }
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(op)
    }
    pub fn run_tasks(&self) -> Result<()> {
        let keys = {
            let db = self.db.lock().unwrap();
            let mut stmt=db.prepare("SELECT id FROM operations WHERE state IN ('queued','running') AND json_extract(data,'$.kind') IN ('inspect','scan','stage') LIMIT 5")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for key in keys {
            self.execute_task(&key)?;
        }
        Ok(())
    }
}
