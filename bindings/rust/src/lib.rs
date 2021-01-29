//! # genomicsqlite
//!
//! Genomics Extension for SQLite
//!
//! Installation & programming guide: [https://mlin.github.io/GenomicSQLite/](https://mlin.github.io/GenomicSQLite/)
use json::object::Object;
use rusqlite::{params, Connection, LoadExtensionGuard, OpenFlags, Result, NO_PARAMS};
use std::collections::HashMap;
use std::env;
#[cfg(feature = "bundle_libgenomicsqlite")]
use std::fs::File;
#[cfg(feature = "bundle_libgenomicsqlite")]
use std::io::prelude::*;
use std::path::Path;
use std::sync::Once;
use tempfile::TempDir;

/* Helper functions for bundling libgenomicsqlite.{so,dylib} into the compilation unit */

// extract slice into basename under a temp directory
#[cfg(feature = "bundle_libgenomicsqlite")]
fn extract_libgenomicsqlite_impl(
    bytes: &[u8],
    basename: &str,
) -> std::io::Result<(String, TempDir)> {
    let tmp = tempfile::tempdir()?;
    let libpath = match tmp.path().join(basename).to_str() {
        Some(p) => p.to_string(),
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "tempdir",
            ))
        }
    };
    {
        let mut file = File::create(&libpath)?;
        file.write_all(bytes)?;
    }
    Ok((libpath, tmp))
}

#[cfg(feature = "bundle_libgenomicsqlite")]
fn extract_libgenomicsqlite(bytes: &[u8], filename: &str) -> (String, TempDir) {
    match extract_libgenomicsqlite_impl(bytes, filename) {
        Ok(ans) => ans,
        Err(_) => panic!("genomicsqlite: failed extracting temp libgenomicsqlite; check temp free space & permissions")
    }
}

#[cfg(not(feature = "bundle_libgenomicsqlite"))]
fn bundle_libgenomicsqlite() -> Option<(String, TempDir)> {
    None
}

#[cfg(all(
    feature = "bundle_libgenomicsqlite",
    target_os = "linux",
    target_arch = "x86_64"
))]
fn bundle_libgenomicsqlite() -> Option<(String, TempDir)> {
    Some(extract_libgenomicsqlite(
        include_bytes!("../libgenomicsqlite.so"),
        &"libgenomicsqlite.so",
    ))
}

#[cfg(all(
    feature = "bundle_libgenomicsqlite",
    target_os = "macos",
    target_arch = "x86_64"
))]
fn bundle_libgenomicsqlite() -> Option<(String, TempDir)> {
    Some(extract_libgenomicsqlite(
        include_bytes!("../libgenomicsqlite.dylib"),
        "libgenomicsqlite.dylib",
    ))
}

// helper for simple queries
fn query1str<P>(conn: &Connection, sql: &str, params: P) -> Result<String>
where
    P: IntoIterator,
    P::Item: rusqlite::ToSql,
{
    let ans: Result<String> = conn.query_row(sql, params, |row| row.get(0));
    ans
}

static START: Once = Once::new();

/// Open a [rusqlite::Connection] for a compressed database with the [ConnectionMethods] available.
///
/// # Arguments
///
/// * `path` - Database filename
/// * `flags` - SQLite open mode flags, as in [rusqlite::Connection::open_with_flags]
/// * `config` - [GenomicSQLite tuning options](https://mlin.github.io/GenomicSQLite/guide/#tuning-options),
///              as JSON keys & values; may be empty `Object::new()` to use defaults
///
/// # Examples
///
/// ```
/// use genomicsqlite::ConnectionMethods;
/// let conn = genomicsqlite::open(
///     "/tmp/rustdoc_example.genomicsqlite",
///     rusqlite::OpenFlags::SQLITE_OPEN_CREATE | rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
///     &json::object::Object::new()
/// );
/// println!("GenomicSQLite {}", conn.unwrap().genomicsqlite_version());
/// ```
pub fn open<P: AsRef<Path>>(path: P, flags: OpenFlags, config: &Object) -> Result<Connection> {
    // once: load libgenomicsqlite extension
    START.call_once(|| {
        let mut _tmpdir;
        let libgenomicsqlite = match env::var("LIBGENOMICSQLITE") {
            Ok(v) => v,
            Err(_) => match bundle_libgenomicsqlite() {
                Some((ans, td)) => {
                    _tmpdir = td;
                    ans
                }
                None => "libgenomicsqlite".to_string(),
            },
        };
        let memconn = Connection::open_in_memory().unwrap();
        let _guard = LoadExtensionGuard::new(&memconn).unwrap();
        match memconn.load_extension(libgenomicsqlite.clone(), None) {
            Err(err) => panic!(
                "genomicsqlite failed to load_extension(\"{}\"): {}",
                libgenomicsqlite, err
            ),
            Ok(()) => (),
        }
    });

    // generate config & connection strings
    let memconn = Connection::open_in_memory().unwrap();
    let config_json_str = config.dump();
    let uri: String = query1str(
        &memconn,
        "SELECT genomicsqlite_uri(?,?)",
        params![path.as_ref().to_str().unwrap(), &config_json_str],
    )?;

    // open GenomicSQLite connection
    let conn = Connection::open_with_flags(uri, flags | OpenFlags::SQLITE_OPEN_URI)?;

    // generate and execute tuning SQL script
    let tuning_sql = query1str(
        &conn,
        "SELECT genomicsqlite_tuning_sql(?)",
        params![&config_json_str],
    )?;
    conn.execute_batch(&tuning_sql)?;

    Ok(conn)
}

/// Genomic reference sequence metadata
#[derive(Clone)]
pub struct RefSeq {
    pub rid: i64,
    pub name: String,
    pub length: i64,
    pub assembly: Option<String>,
    pub refget_id: Option<String>,
    pub meta_json: Object,
}

/// Methods for GenomicSQLite [rusqlite::Connection]s; see [Programming Guide](https://mlin.github.io/GenomicSQLite/guide/)
/// for each method's semantics. The methods can also be invoked on an open
/// [rusqlite::Transaction], via its implicit `Deref<Target=Connection>`.
pub trait ConnectionMethods {
    /// Get Genomics Extension version
    fn genomicsqlite_version(&self) -> String;

    /// Generate SQL command to attach another GenomicSQLite database to an existing connection
    fn genomicsqlite_attach_sql<P: AsRef<Path>>(
        &self,
        path: P,
        schema_name: &str,
        config: &Object,
    ) -> Result<String>;

    /// Generate SQL command to `VACUUM INTO` the database into a new file
    fn genomicsqlite_vacuum_into_sql<P: AsRef<Path>>(
        &self,
        path: P,
        config: &Object,
    ) -> Result<String>;

    /// Generate SQL script to create Genomic Range Index for a table
    fn create_genomic_range_index_sql(
        &self,
        table_name: &str,
        chromosome: &str,
        begin_pos: &str,
        end_pos: &str,
    ) -> Result<String>;
    fn create_genomic_range_index_sql_with_floor(
        &self,
        table_name: &str,
        chromosome: &str,
        begin_pos: &str,
        end_pos: &str,
        floor: i64,
    ) -> Result<String>;

    /// Generate SQL script to add a reference assembly to the stored metadata
    fn put_reference_assembly_sql(&self, assembly: &str) -> Result<String>;
    fn put_reference_assembly_sql_with_schema(
        &self,
        assembly: &str,
        schema: &str,
    ) -> Result<String>;

    /// Generate SQL script to add one reference sequence to the stored metadata. Set `rid: -1` to
    /// let the database generate a value.
    fn put_reference_sequence_sql(&self, refseq: &RefSeq) -> Result<String>;
    fn put_reference_sequence_sql_with_schema(
        &self,
        refseq: &RefSeq,
        schema: &str,
    ) -> Result<String>;

    /// Read the stored reference sequence metadata into a lookup table keyed by `rid`
    fn get_reference_sequences_by_rid(&self) -> Result<HashMap<i64, RefSeq>>;
    fn get_reference_sequences_by_rid_with_options(
        &self,
        assembly: Option<&str>,
        schema: Option<&str>,
    ) -> Result<HashMap<i64, RefSeq>>;

    /// Read the stored reference sequence metadata into a lookup table keyed by `name`
    fn get_reference_sequences_by_name(&self) -> Result<HashMap<String, RefSeq>>;
    fn get_reference_sequences_by_name_with_options(
        &self,
        assembly: Option<&str>,
        schema: Option<&str>,
    ) -> Result<HashMap<String, RefSeq>>;
}

impl ConnectionMethods for Connection {
    fn genomicsqlite_version(&self) -> String {
        query1str(self, "SELECT genomicsqlite_version()", NO_PARAMS).unwrap()
    }

    fn genomicsqlite_attach_sql<P: AsRef<Path>>(
        &self,
        path: P,
        schema_name: &str,
        config: &Object,
    ) -> Result<String> {
        query1str(
            self,
            "SELECT genomicsqlite_attach_sql(?,?,?)",
            params![path.as_ref().to_str(), schema_name, config.dump()],
        )
    }

    fn genomicsqlite_vacuum_into_sql<P: AsRef<Path>>(
        &self,
        path: P,
        config: &Object,
    ) -> Result<String> {
        query1str(
            self,
            "SELECT genomicsqlite_vacuum_into_sql(?,?)",
            params![path.as_ref().to_str(), config.dump()],
        )
    }

    fn create_genomic_range_index_sql(
        &self,
        table_name: &str,
        chromosome: &str,
        begin_pos: &str,
        end_pos: &str,
    ) -> Result<String> {
        self.create_genomic_range_index_sql_with_floor(
            table_name, chromosome, begin_pos, end_pos, -1,
        )
    }

    fn create_genomic_range_index_sql_with_floor(
        &self,
        table_name: &str,
        chromosome: &str,
        begin_pos: &str,
        end_pos: &str,
        floor: i64,
    ) -> Result<String> {
        query1str(
            self,
            "SELECT create_genomic_range_index_sql(?,?,?,?,?)",
            params![table_name, chromosome, begin_pos, end_pos, floor],
        )
    }

    fn put_reference_assembly_sql(&self, assembly: &str) -> Result<String> {
        self.put_reference_assembly_sql_with_schema(assembly, "")
    }

    fn put_reference_assembly_sql_with_schema(
        &self,
        assembly: &str,
        schema: &str,
    ) -> Result<String> {
        query1str(
            self,
            "SELECT put_genomic_reference_assembly_sql(?,?)",
            params![assembly, schema],
        )
    }

    fn put_reference_sequence_sql(&self, refseq: &RefSeq) -> Result<String> {
        self.put_reference_sequence_sql_with_schema(refseq, "")
    }

    fn put_reference_sequence_sql_with_schema(
        &self,
        refseq: &RefSeq,
        schema: &str,
    ) -> Result<String> {
        query1str(
            self,
            "SELECT put_genomic_reference_sequence_sql(?,?,?,?,?,?)",
            params![
                refseq.length,
                refseq.assembly,
                refseq.refget_id,
                refseq.meta_json.dump(),
                refseq.rid,
                schema
            ],
        )
    }

    fn get_reference_sequences_by_rid(&self) -> Result<HashMap<i64, RefSeq>> {
        self.get_reference_sequences_by_rid_with_options(None, None)
    }

    fn get_reference_sequences_by_rid_with_options(
        &self,
        assembly: Option<&str>,
        schema: Option<&str>,
    ) -> Result<HashMap<i64, RefSeq>> {
        let sql = format!(
            "SELECT
                _gri_rid,
                gri_refseq_name, gri_refseq_length,
                gri_assembly, gri_refget_id,
                gri_refseq_meta_json
             FROM {}_gri_refseq{}",
            schema.map_or("".to_string(), |s| { s.to_string() + "." }),
            assembly.map_or("", |_| " WHERE gri_assembly = ?")
        );
        let mut stmt = self.prepare(&sql)?;
        let assembly_param = params![assembly];
        let refseqs = stmt.query_map(assembly.map_or(NO_PARAMS, |_| &assembly_param), |row| {
            let meta_json_str: String = row.get(5)?;
            Ok(RefSeq {
                rid: row.get(0)?,
                name: row.get(1)?,
                length: row.get(2)?,
                assembly: row.get(3)?,
                refget_id: row.get(4)?,
                meta_json: match json::parse(&meta_json_str.as_str()) {
                    Ok(json::JsonValue::Object(obj)) => Ok(obj),
                    _ => Err(rusqlite::Error::ModuleError(
                        "genomicsqlite::get_reference_sequences_by_rid: invalid meta_json"
                            .to_string(),
                    )),
                }?,
            })
        })?;
        let mut ans = HashMap::new();
        for res in refseqs {
            let refseq = res?;
            ans.insert(refseq.rid, refseq);
        }
        Ok(ans)
    }

    fn get_reference_sequences_by_name(&self) -> Result<HashMap<String, RefSeq>> {
        self.get_reference_sequences_by_name_with_options(None, None)
    }

    fn get_reference_sequences_by_name_with_options(
        &self,
        assembly: Option<&str>,
        schema: Option<&str>,
    ) -> Result<HashMap<String, RefSeq>> {
        let mut ans = HashMap::new();
        for (_, refseq) in self
            .get_reference_sequences_by_rid_with_options(assembly, schema)?
            .drain()
        {
            ans.insert(refseq.name.clone(), refseq);
        }
        Ok(ans)
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionMethods;
    use rusqlite::{OpenFlags, NO_PARAMS};

    #[test]
    fn smoke_test() {
        let dbfn = format!(
            "/tmp/genomicsqlite_rust_smoke_test.{}.db",
            uuid::Uuid::new_v4()
        );
        let mut config = json::object::Object::new();
        config.insert("threads", json::JsonValue::from(3));
        let mut conn = super::open(
            dbfn,
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
            &config,
        )
        .unwrap();

        // check that tuning SQL applied successfully
        let mut ans: i64 = conn
            .query_row("PRAGMA threads", NO_PARAMS, |row| row.get(0))
            .unwrap();
        assert_eq!(ans, 3);

        // begin transaction
        {
            let txn = conn.transaction().unwrap();

            // load table & create GRI
            txn.execute_batch(
                "CREATE TABLE feature(rid INTEGER, beg INTEGER, end INTEGER);
                INSERT INTO feature VALUES(3, 12, 34);
                INSERT INTO feature VALUES(3, 0, 23);
                INSERT INTO feature VALUES(3, 34, 56)",
            )
            .unwrap();
            let gri_sql = txn
                .create_genomic_range_index_sql("feature", "rid", "beg", "end")
                .unwrap();
            txn.execute_batch(&gri_sql).unwrap();

            txn.commit().unwrap();
        }

        // GRI query
        ans = conn
            .query_row(
                "SELECT COUNT(*) FROM genomic_range_rowids('feature',3,34,34)",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ans, 2);

        // reference genome stuff
        conn.execute_batch(
            &conn
                .put_reference_assembly_sql("GRCh38_no_alt_analysis_set")
                .unwrap(),
        )
        .unwrap();
        let refseqs = conn.get_reference_sequences_by_name().unwrap();
        let chr3 = refseqs.get("chr3").unwrap();
        assert_eq!(chr3.name, "chr3");
        assert_eq!(chr3.rid, 3);
        assert_eq!(chr3.length, 198295559);
    }

    #[test]
    fn web_test() {
        let config = json::object::Object::new();
        let conn = super::open(
            "https://github.com/mlin/sqlite_zstd_vfs/releases/download/web-test-db-v1/TxDb.Hsapiens.UCSC.hg38.knownGene.vacuum.genomicsqlite",
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            &config,
        ).unwrap();
        let ans: i64 = conn
            .query_row("SELECT COUNT(1) FROM sqlite_master", NO_PARAMS, |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(ans, 12);
    }
}
