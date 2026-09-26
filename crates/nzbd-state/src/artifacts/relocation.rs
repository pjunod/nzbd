use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek};

#[derive(Serialize, Deserialize)]
struct Relocation {
    source: Artifact,
    destination: PathBuf,
    destination_root: Identity,
    scratch: Option<Artifact>,
}

impl Inventory {
    /// Journal before moving. A cross-volume move publishes a verified copy,
    /// then retires only the exact source entries captured by this operation.
    pub fn relocate(&self, job: u32, destination: &Path) -> Result<()> {
        let guard = self.mutation.lock().unwrap();
        let mut source = self.for_job(job)?.ok_or(Error::NotFound)?;
        if source.path == destination {
            self.verify(&source)?;
            return Ok(());
        }
        if source.state != "active" || source.hold.as_deref().is_some_and(|h| h != "review") {
            return Err(Error::Conflict(
                "payload is not quiescent for relocation".into(),
            ));
        }
        let dir = self.verify(&source)?;
        source.files = fs::manifest(&dir, 100_000)?;
        fs::absolute(destination)?;
        let root = destination
            .parent()
            .ok_or_else(|| Error::Conflict("missing destination root".into()))?;
        std::fs::create_dir_all(root)?;
        let root_dir = fs::open_dir(root)?;
        if destination.try_exists()? {
            return Err(Error::Conflict("move destination already exists".into()));
        }
        let key = format!("move-{}-{}", source.id, source.revision);
        let mut relocation = Relocation {
            source: source.clone(),
            destination: destination.into(),
            destination_root: fs::identity(&root_dir.metadata()?),
            scratch: None,
        };
        let mut op = Operation {
            id: key.clone(),
            artifact: source.id.clone(),
            kind: "relocate".into(),
            state: "running".into(),
            request: serde_json::to_string(&relocation)?,
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        source.state = "transitioning".into();
        source.revision += 1;
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &source)?;
            save_operation(&tx, &op)?;
            tx.commit()?;
        }
        drop(guard);
        let result = (|| {
            match fs::rename_exclusive(&source.path, destination) {
                Ok(()) => (),
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::CrossesDevices
                        || e.raw_os_error() == Some(18) =>
                {
                    let scratch_path = root.join(format!(".runner-{key}"));
                    // An existing name is never authority, even after a crash.
                    std::fs::create_dir(&scratch_path)?;
                    fs::sync_directory(&root_dir)?;
                    let scratch_dir = fs::open_dir(&scratch_path)?;
                    let mut scratch = source.clone();
                    scratch.id = format!("scratch-{key}");
                    scratch.job = None;
                    scratch.generation = id(&self.db.lock().unwrap())?;
                    scratch.path = scratch_path.clone();
                    scratch.root = root.into();
                    scratch.root_identity = relocation.destination_root.clone();
                    scratch.identity = Some(fs::identity(&scratch_dir.metadata()?));
                    scratch.files.clear();
                    scratch.state = "active".into();
                    scratch.owned = true;
                    scratch.keep = true;
                    scratch.hold = Some("review: relocation staging".into());
                    self.sidecar(&scratch)?;
                    save_artifact(&self.db.lock().unwrap(), &scratch)?;
                    relocation.scratch = Some(scratch.clone());
                    op.request = serde_json::to_string(&relocation)?;
                    save_operation(&self.db.lock().unwrap(), &op)?;
                    for entry in &source.files {
                        let target = scratch_path.join(&entry.path);
                        if entry.identity.directory {
                            std::fs::create_dir(&target)?;
                            continue;
                        }
                        let mut input = fs::open_relative(&dir, &entry.path)?;
                        if fs::identity(&input.metadata()?) != entry.identity {
                            return Err(Error::Conflict("source changed during relocation".into()));
                        }
                        let mut output = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&target)?;
                        let bytes = std::io::copy(&mut input, &mut output)?;
                        output.sync_all()?;
                        input.rewind()?;
                        let hash = |f: &mut File| -> Result<Vec<u8>> {
                            let mut h = Sha256::new();
                            let mut b = [0u8; 1024 * 1024];
                            loop {
                                let n = f.read(&mut b)?;
                                if n == 0 {
                                    break;
                                }
                                h.update(&b[..n]);
                            }
                            Ok(h.finalize().to_vec())
                        };
                        if bytes != entry.identity.bytes
                            || hash(&mut input)? != hash(&mut File::open(&target)?)?
                            || fs::identity(&input.metadata()?) != entry.identity
                        {
                            return Err(Error::Conflict(
                                "relocation copy verification failed".into(),
                            ));
                        }
                    }
                    // Every nested directory entry must be durable before publication.
                    for entry in source.files.iter().rev().filter(|e| e.identity.directory) {
                        fs::sync_directory(&fs::open_relative(&scratch_dir, &entry.path)?)?;
                    }
                    fs::sync_directory(&scratch_dir)?;
                    scratch.files = fs::manifest(&scratch_dir, 100_000)?;
                    relocation.scratch = Some(scratch.clone());
                    op.request = serde_json::to_string(&relocation)?;
                    save_artifact(&self.db.lock().unwrap(), &scratch)?;
                    save_operation(&self.db.lock().unwrap(), &op)?;
                    fs::rename_exclusive(&scratch_path, destination)?;
                }
                Err(e) => return Err(e),
            }
            let _guard = self.mutation.lock().unwrap();
            self.commit_relocation(&mut op, &relocation)
        })();
        let guard = self.mutation.lock().unwrap();
        if let Err(e) = &result {
            op.state = "review".into();
            op.error = Some(e.to_string());
            save_operation(&self.db.lock().unwrap(), &op)?;
            if let Some(scratch) = &relocation.scratch {
                let mut row = self.get(&scratch.id)?;
                row.state = "retained".into();
                save_artifact(&self.db.lock().unwrap(), &row)?;
            }
            // Keep the source identity at its known location when publication
            // did not happen, allowing PP to report retained files accurately.
            if let Ok(_) = self.verify(&relocation.source) {
                let mut retained = self.get(&relocation.source.id)?;
                retained.state = "active".into();
                retained.error = Some(e.to_string());
                save_artifact(&self.db.lock().unwrap(), &retained)?;
            }
        }
        drop(guard);
        result?;
        if let Err(e) = self.retire_relocation_source(&op.id) {
            tracing::warn!(operation=%op.id, error=%e, "relocation committed; source retirement pending");
        }
        Ok(())
    }
    fn commit_relocation(&self, op: &mut Operation, movement: &Relocation) -> Result<()> {
        let root = movement.destination.parent().unwrap();
        let root_dir = fs::open_dir(root)?;
        if !movement
            .destination_root
            .same_object(&fs::identity(&root_dir.metadata()?))
        {
            return Err(Error::Conflict("move destination root changed".into()));
        }
        let target = fs::open_dir(&movement.destination)?;
        let expected = movement.scratch.as_ref().unwrap_or(&movement.source);
        let observed = fs::identity(&target.metadata()?);
        if !expected
            .identity
            .as_ref()
            .is_some_and(|i| i.same_object(&observed))
        {
            return Err(Error::Conflict("move publication identity changed".into()));
        }
        let files = fs::manifest(&target, 100_000)?;
        if files != expected.files {
            return Err(Error::Conflict("move publication contents changed".into()));
        }
        let mut current = self.get(&movement.source.id)?;
        current.path = movement.destination.clone();
        current.root = root.into();
        current.root_identity = movement.destination_root.clone();
        current.identity = Some(fs::identity(&target.metadata()?));
        current.files = files;
        current.state = "active".into();
        current.revision += 2;
        self.sidecar(&current)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if let Some(scratch) = &movement.scratch {
            let mut obsolete = scratch.clone();
            obsolete.state = "source_gone".into();
            obsolete.updated_at = now();
            save_artifact(&tx, &obsolete)?;
            let mut old = movement.source.clone();
            old.id = format!("source-{}", op.id);
            old.job = None;
            old.state = "retained".into();
            old.hold = if old.owned {
                None
            } else {
                Some("review".into())
            };
            old.keep = current.keep;
            old.retention_seconds = 0;
            old.deadline = None;
            self.sidecar(&old)?;
            save_artifact(&tx, &old)?;
            if old.owned && !old.keep && old.hold.is_none() {
                // Publication and retirement admission share one transaction.
                save_operation(
                    &tx,
                    &Operation {
                        id: format!("retire-{}", op.id),
                        artifact: old.id.clone(),
                        kind: "delete".into(),
                        state: "queued".into(),
                        request: serde_json::to_string(&(&old.id, old.revision, 0u64, false))?,
                        created_at: now(),
                        not_before: now(),
                        attempts: 0,
                        next_retry: 0,
                        error: None,
                    },
                )?;
            }
        }
        save_artifact(&tx, &current)?;
        op.state = "succeeded".into();
        op.error = None;
        save_operation(&tx, op)?;
        event(
            &tx,
            &current.id,
            "relocated",
            &current.path.to_string_lossy(),
        )?;
        tx.commit()?;
        Ok(())
    }
    fn retire_relocation_source(&self, key: &str) -> Result<()> {
        if let Ok(old) = self.get(&format!("source-{key}")) {
            if old.owned && !old.keep && old.hold.is_none() && !old.terminal() {
                let op = self.request_delete(&old.id, old.revision, &format!("retire-{key}"), 0)?;
                self.execute_delete(&op.id)?;
            }
        }
        Ok(())
    }
    pub fn reconcile_relocations(&self) -> Result<()> {
        let guard = self.mutation.lock().unwrap();
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT data FROM operations WHERE state='running' AND json_extract(data,'$.kind')='relocate' LIMIT 25")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut retired = Vec::new();
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            let movement: Relocation = serde_json::from_str(&op.request)?;
            if let Err(e) = self.commit_relocation(&mut op, &movement) {
                op.state = "review".into();
                op.error = Some(format!("interrupted move: {e}"));
                if let Some(scratch) = &movement.scratch {
                    if let Ok(mut row) = self.get(&scratch.id) {
                        row.state = "retained".into();
                        save_artifact(&self.db.lock().unwrap(), &row)?;
                    }
                }
                save_operation(&self.db.lock().unwrap(), &op)?;
                let mut a = self.get(&op.artifact)?;
                a.hold = Some("review: interrupted move".into());
                a.state = "retained".into();
                a.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &a)?;
            } else {
                retired.push(op.id);
            }
        }
        drop(guard);
        for key in retired {
            self.retire_relocation_source(&key)?;
        }
        Ok(())
    }
}
