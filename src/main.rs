use std::{collections::hash_map::DefaultHasher, hash::Hasher, io::Write, sync::Arc, time::Instant};

use redb::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<(&[u8;32], u64), &[u8;64]> = TableDefinition::new("outboard");

fn main() -> Result<(), Error> {
    let n = 100000u64;

    let mut builder = Database::builder();
    builder.set_cache_size(1024 * 1024 * 1024);
    std::fs::remove_file("test.redb").ok();
    let db = builder.create("test.redb")?;

    println!("writing to redb with non durable transactions");
    let t0 = Instant::now();
    for i in 0..n {
        let mut write_txn = db.begin_write()?;
        write_txn.set_durability(redb::Durability::None);
        {
            let mut table = write_txn.open_table(TABLE)?;
            let hash = [0u8; 32];
            let pair = [0u8; 64];
            table.insert(&(&hash, i), &pair)?;
        }
        write_txn.commit()?;
    }
    let txn = db.begin_write()?;
    txn.commit()?;
    drop(db);
    let dt_redb = t0.elapsed().as_secs_f64();
    println!("writing done {} {}s", n, dt_redb);
    std::fs::remove_file("test.redb").ok();

    std::fs::remove_file("test.obao4").ok();
    let mut file = std::fs::File::create("test.obao4")?;
    println!("appending to a file");
    let t0 = Instant::now();
    for i in 0..n {
        file.write([0u8; 64].as_ref())?;
    }
    file.flush()?;
    drop(file);
    let dt_file = t0.elapsed().as_secs_f64();
    println!("writing done {} {}s", n, dt_file);
    std::fs::remove_file("test.obao4")?;

    println!(":-( {}", dt_redb / dt_file);
    Ok(())
}
