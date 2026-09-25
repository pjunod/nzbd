use super::*;
use sha2::{Digest, Sha256};
use std::io::Read;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryFile {
    pub id: String,
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Recovery {
    pub id: String,
    pub installation: String,
    pub artifact: String,
    pub generation: String,
    pub request_id: String,
    pub request: String,
    pub state: String,
    pub manifest_digest: String,
    pub files: Vec<RecoveryFile>,
    pub published: PathBuf,
    pub consumer: Option<String>,
    pub import_id: Option<String>,
    pub receipt: Option<Receipt>,
    pub error: Option<String>,
    #[serde(default)]
    pub scratch_identity: Option<Identity>,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptFile {
    pub id: String,
    pub bytes: u64,
    pub sha256: String,
    pub result: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub import_id: String,
    pub manifest_digest: String,
    pub files: Vec<ReceiptFile>,
}
fn save(db: &Connection, r: &Recovery) -> Result<()> {
    db.execute("INSERT INTO recoveries(id,artifact,state,data) VALUES(?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET state=excluded.state,data=excluded.data",params![r.id,r.artifact,r.state,serde_json::to_string(r)?])?;
    Ok(())
}
fn digest(file: &mut File) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buf = [0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

impl Inventory {
    pub fn recovery(&self, key: &str) -> Result<Recovery> {
        read(&self.db.lock().unwrap(), "recoveries", key)
    }
    pub fn recoveries(&self, offset: usize) -> Result<Vec<Recovery>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT data FROM recoveries ORDER BY id LIMIT 100 OFFSET ?1")?;
        let rows = stmt.query_map([offset as i64], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    /// Copies are independent inodes. Originals are held before scratch is
    /// allocated, and remain held through ambiguous claims or partial receipts.
    pub fn stage_recovery(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        paths: &[String],
        root: &Path,
    ) -> Result<Recovery> {
        let _guard = self.mutation.lock().unwrap();
        if paths.is_empty() || paths.len() > 1000 || request_id.is_empty() || request_id.len() > 128
        {
            return Err(Error::Conflict(
                "select 1–1000 regular files and provide an idempotency key".into(),
            ));
        }
        let request = serde_json::to_string(&(key, revision, paths, root))?;
        {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM recoveries WHERE json_extract(data,'$.request_id')=?1",
            )?;
            let raw: Option<String> = stmt.query_row([request_id], |r| r.get(0)).optional()?;
            if let Some(raw) = raw {
                let existing: Recovery = serde_json::from_str(&raw)?;
                drop(stmt);
                drop(db);
                return if existing.request == request {
                    self.resume_publication(existing)
                } else {
                    Err(Error::Conflict("idempotency key reused".into()))
                };
            }
        }
        let mut a = self.get(key)?;
        if a.revision != revision
            || !a.owned
            || a.hold.is_some()
            || !matches!(a.state.as_str(), "parked_failed" | "retained" | "completed")
        {
            return Err(Error::Conflict(
                "stale revision, unowned files, or active operation".into(),
            ));
        }
        fs::absolute(root)?;
        if root.starts_with(&a.path)
            || a.path.starts_with(root)
            || root.starts_with(&self.state_dir)
            || self.state_dir.starts_with(root)
        {
            return Err(Error::Conflict(
                "recovery root overlaps source or state".into(),
            ));
        }
        let source = self.verify(&a)?;
        let observed = fs::manifest(&source, 100_000)?;
        if observed != a.files {
            return Err(Error::Conflict("source changed; inspect again".into()));
        }
        let mut selected = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for path in paths {
            if !seen.insert(path) {
                return Err(Error::Conflict("duplicate selection".into()));
            }
            let f = a
                .files
                .iter()
                .find(|f| f.path == *path && !f.identity.directory)
                .ok_or_else(|| {
                    Error::Conflict("select a regular file from the current inventory".into())
                })?;
            let name = path.to_ascii_lowercase();
            if name.ends_with(".tmp")
                || name.ends_with(".part")
                || name.ends_with(".par2")
                || name.ends_with(".rar")
                || name.ends_with(".7z")
            {
                return Err(Error::Conflict(
                    "temporary, archive and parity files cannot be staged as media".into(),
                ));
            }
            selected.push(f.clone());
        }
        // Persist both hold and intention atomically before making scratch.
        let mut r = {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            let rid = id(&tx)?;
            let r = Recovery {
                id: rid.clone(),
                installation: self.installation.clone(),
                artifact: key.into(),
                generation: id(&tx)?,
                request_id: request_id.into(),
                request,
                state: "staging".into(),
                manifest_digest: String::new(),
                files: Vec::new(),
                published: root.join("published").join(&rid),
                consumer: None,
                import_id: None,
                receipt: None,
                error: None,
                scratch_identity: None,
                created_at: now(),
                updated_at: now(),
            };
            a.hold = Some(format!("recovery:{}", r.id));
            a.revision += 1;
            save_artifact(&tx, &a)?;
            save(&tx, &r)?;
            event(&tx, key, "recovery_staging", &r.id)?;
            tx.commit()?;
            r
        };
        let result = (|| {
            let required: u64 = selected.iter().map(|f| f.identity.bytes).sum();
            let mut volume = root;
            while !volume.exists() {
                volume = volume
                    .parent()
                    .ok_or_else(|| Error::Conflict("recovery volume unavailable".into()))?;
            }
            let available = fs::available_bytes(&fs::open_dir(volume)?)?;
            if available < required.saturating_add(1024 * 1024 * 1024) {
                return Err(Error::Conflict(format!("recovery staging needs {required} bytes plus 1 GiB reserve; {available} available")));
            }
            std::fs::create_dir_all(root)?;
            fs::open_dir(root)?;
            let staging = root.join(".staging");
            let published = root.join("published");
            std::fs::create_dir_all(&staging)?;
            std::fs::create_dir_all(&published)?;
            fs::open_dir(&staging)?;
            fs::open_dir(&published)?;
            let scratch = staging.join(&r.id);
            std::fs::create_dir(&scratch)?;
            r.scratch_identity = Some(fs::identity(&fs::open_dir(&scratch)?.metadata()?));
            File::open(&staging)?.sync_all()?;
            save(&self.db.lock().unwrap(), &r)?;
            let payload = scratch.join("payload");
            std::fs::create_dir(&payload)?;
            for f in selected {
                let mut input = fs::open_relative(&source, &f.path)?;
                if fs::identity(&input.metadata()?) != f.identity {
                    return Err(Error::Conflict("source identity changed".into()));
                }
                let target = payload.join(&f.path);
                if let Some(p) = target.parent() {
                    std::fs::create_dir_all(p)?;
                }
                let mut out = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target)?;
                let bytes = std::io::copy(&mut input, &mut out)?;
                out.sync_all()?;
                let sha256 = digest(&mut File::open(&target)?)?;
                let source_hash = digest(&mut fs::open_relative(&source, &f.path)?)?;
                if bytes != f.identity.bytes
                    || sha256 != source_hash
                    || fs::identity(&input.metadata()?) != f.identity
                {
                    return Err(Error::Conflict(
                        "source changed while copying; original remains held".into(),
                    ));
                }
                let file_id = format!("{:x}", Sha256::digest(f.path.as_bytes()));
                r.files.push(RecoveryFile {
                    id: file_id,
                    path: f.path,
                    bytes,
                    sha256,
                });
                File::open(target.parent().unwrap())?.sync_all()?;
            }
            r.manifest_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&r.files)?));
            let mut mf = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(scratch.join("manifest.json"))?;
            mf.write_all(&serde_json::to_vec(&r)?)?;
            mf.sync_all()?;
            File::open(&payload)?.sync_all()?;
            File::open(&scratch)?.sync_all()?;
            File::open(&staging)?.sync_all()?;
            // Journal the complete manifest before publication. Crash recovery
            // may validate it and finish the acknowledgement without recopying.
            r.state = "publishing".into();
            save(&self.db.lock().unwrap(), &r)?;
            if r.published.exists() {
                return Err(Error::Conflict(
                    "recovery publication already exists".into(),
                ));
            }
            fs::rename_exclusive(&scratch, &r.published)?;
            File::open(&published)?.sync_all()?;
            File::open(&staging)?.sync_all()?;
            self.register_publication(&r)?;
            Ok(())
        })();
        match result {
            Ok(()) => r.state = "published".into(),
            Err(e) => {
                r.state = "failed".into();
                r.error = Some(e.to_string());
            }
        }
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
    /// Resolve publication from its durable manifest. Incomplete copying is
    /// retained for explicit review; it is never recursively erased on restart.
    fn resume_publication(&self, mut r: Recovery) -> Result<Recovery> {
        if !matches!(r.state.as_str(), "staging" | "publishing") {
            return Ok(r);
        }
        let root = r
            .published
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| Error::Conflict("invalid recovery root".into()))?;
        let scratch = root.join(".staging").join(&r.id);
        let result = (|| {
            if r.state != "publishing" {
                return Err(Error::Conflict("copy interrupted before its complete manifest was durable; inspect retained staging".into()));
            }
            let location = if r.published.try_exists()? {
                &r.published
            } else {
                &scratch
            };
            let dir = fs::open_dir(location)?;
            let observed = fs::identity(&dir.metadata()?);
            if !r
                .scratch_identity
                .as_ref()
                .is_some_and(|i| i.same_object(&observed))
            {
                return Err(Error::Conflict(
                    "publication directory identity changed".into(),
                ));
            }
            let raw = fs::open_relative(&dir, "manifest.json")?;
            let manifest: Recovery = serde_json::from_reader(raw)?;
            if manifest.id != r.id
                || manifest.generation != r.generation
                || manifest.manifest_digest != r.manifest_digest
            {
                return Err(Error::Conflict("publication manifest changed".into()));
            }
            let entries = fs::manifest(&dir, 100_000)?;
            if entries.iter().filter(|f| !f.identity.directory).count() != r.files.len() + 1 {
                return Err(Error::Conflict("unexpected files in publication".into()));
            }
            for f in &r.files {
                let mut input = fs::open_relative(&dir, &format!("payload/{}", f.path))?;
                if input.metadata()?.len() != f.bytes || digest(&mut input)? != f.sha256 {
                    return Err(Error::Conflict("publication digest changed".into()));
                }
            }
            if location == &scratch {
                fs::rename_exclusive(&scratch, &r.published)?;
            }
            self.register_publication(&r)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                r.state = "published".into();
                r.error = None;
            }
            Err(e) => {
                r.state = "failed".into();
                r.error = Some(e.to_string());
                // The inventory owns the diagnosis even when copying never
                // reached publication. Adoption remains an explicit action.
                if scratch.try_exists().unwrap_or(false) {
                    self.discover_unlocked(scratch.parent().unwrap(), &scratch)?;
                }
            }
        }
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
    pub fn reconcile_recoveries(&self) -> Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let pending = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM recoveries WHERE state IN ('staging','publishing') LIMIT 10",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in pending {
            self.resume_publication(serde_json::from_str(&raw)?)?;
        }
        Ok(())
    }
    pub fn claim_recovery(
        &self,
        key: &str,
        consumer: &str,
        import_id: &str,
        manifest: &str,
    ) -> Result<Recovery> {
        let _guard = self.mutation.lock().unwrap();
        let mut r = self.recovery(key)?;
        if r.manifest_digest != manifest || import_id.is_empty() {
            return Err(Error::Conflict(
                "manifest changed or missing import id".into(),
            ));
        }
        if r.consumer.as_deref() == Some(consumer) && r.import_id.as_deref() == Some(import_id) {
            return Ok(r);
        }
        if r.state != "published" || r.consumer.is_some() {
            return Err(Error::Conflict(
                "recovery already claimed or unavailable".into(),
            ));
        }
        r.consumer = Some(consumer.into());
        r.import_id = Some(import_id.into());
        r.state = "claimed".into();
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
    pub fn recovery_receipt(
        &self,
        key: &str,
        consumer: &str,
        receipt: Receipt,
    ) -> Result<Recovery> {
        let _guard = self.mutation.lock().unwrap();
        let mut r = self.recovery(key)?;
        if r.consumer.as_deref() != Some(consumer)
            || r.import_id.as_deref() != Some(&receipt.import_id)
            || r.manifest_digest != receipt.manifest_digest
        {
            return Err(Error::Conflict(
                "receipt does not belong to this claim".into(),
            ));
        }
        if let Some(previous) = &r.receipt {
            if previous == &receipt {
                return Ok(r);
            }
            return Err(Error::Conflict("conflicting receipt".into()));
        }
        if r.state != "claimed" {
            return Err(Error::Conflict("recovery is not claimed".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for f in &receipt.files {
            let expected = r
                .files
                .iter()
                .find(|x| x.id == f.id)
                .ok_or_else(|| Error::Conflict("receipt includes an unselected file".into()))?;
            if !seen.insert(&f.id)
                || f.bytes != expected.bytes
                || f.sha256 != expected.sha256
                || !matches!(
                    f.result.as_str(),
                    "imported" | "already_present" | "failed" | "skipped"
                )
            {
                return Err(Error::Conflict("invalid per-file receipt".into()));
            }
        }
        let complete = receipt.files.len() == r.files.len()
            && receipt
                .files
                .iter()
                .all(|f| matches!(f.result.as_str(), "imported" | "already_present"));
        r.state = if complete { "imported" } else { "partial" }.into();
        r.receipt = Some(receipt);
        r.updated_at = now();
        let mut a = self.get(&r.artifact)?;
        if complete {
            a.hold = None;
            a.eligible_seconds = 0;
            a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
            // Unselected media is never implicitly authorized for expiry.
            if a.files.iter().filter(|f| !f.identity.directory).count() != r.files.len() {
                a.hold = Some("review: unselected source files remain".into());
            }
            a.revision += 1;
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if complete {
            let mut staged: Artifact = read(&tx, "artifacts", &format!("recovery-{}", r.id))?;
            staged.state = "recovery_imported".into();
            staged.hold = None;
            staged.deadline = Some(now() + 86400);
            staged.eligible_seconds = 0;
            staged.revision += 1;
            save_artifact(&tx, &staged)?;
        }
        save(&tx, &r)?;
        save_artifact(&tx, &a)?;
        event(&tx, &a.id, "recovery_receipt", &r.state)?;
        tx.commit()?;
        Ok(r)
    }
    pub fn cancel_recovery(
        &self,
        key: &str,
        consumer: Option<&str>,
        ack: bool,
    ) -> Result<Recovery> {
        let _guard = self.mutation.lock().unwrap();
        let mut r = self.recovery(key)?;
        if r.state == "imported" {
            return Err(Error::Conflict("import already committed".into()));
        }
        let final_partial = r.state == "partial" && r.receipt.is_some();
        if r.consumer.is_some() && !ack && !final_partial {
            r.state = "cancel_pending".into();
        } else {
            if r.consumer.is_some()
                && !final_partial
                && (consumer != r.consumer.as_deref() || r.state != "cancel_pending")
            {
                return Err(Error::Conflict(
                    "claimed cancellation requires the worker's quiescence acknowledgement".into(),
                ));
            }
            r.state = "cancelled".into();
            let mut a = self.get(&r.artifact)?;
            if let Ok(mut staged) = self.get(&format!("recovery-{}", r.id)) {
                staged.hold = Some("review: cancelled recovery staging".into());
                staged.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &staged)?;
            }
            a.hold = Some("review: cancelled recovery".into());
            a.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &a)?;
        }
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
}
