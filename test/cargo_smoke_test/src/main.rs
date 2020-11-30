use genomicsqlite::ConnectionMethods;
use rusqlite::{Connection, OpenFlags};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dbconn: Connection = genomicsqlite::open(
        ":memory:",
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        &json::object::Object::new(),
    )?;
    println!("GenomicSQLite {}", dbconn.genomicsqlite_version());
    Ok(())
}
