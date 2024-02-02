use std::{
    collections::hash_map::DefaultHasher, hash::Hasher, io::Write, sync::Arc, time::Instant,
};

use redb::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<(&[u8; 32], u64), &[u8; 64]> = TableDefinition::new("outboard");

fn bench(
    text: &str,
    n: u64,
    redb_cb: impl Fn(&mut redb::WriteTransaction<'_>),
    fs_cb: impl Fn(&mut std::fs::File),
) -> Result<(), Error> {
    let mut builder = Database::builder();
    builder.set_cache_size(1024 * 1024 * 1024);
    std::fs::remove_file("test.redb").ok();
    let db = builder.create("test.redb")?;

    println!("{}: writing to redb", text);
    let t0 = Instant::now();
    for i in 0..n {
        let mut write_txn = db.begin_write()?;
        redb_cb(&mut write_txn);
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
    println!("{}: writing done {} {}s", text, n, dt_redb);
    std::fs::remove_file("test.redb").ok();

    std::fs::remove_file("test.obao4").ok();
    let mut file = std::fs::File::create("test.obao4")?;
    println!("{}: appending to a file", text);
    let t0 = Instant::now();
    for _ in 0..n {
        file.write([0u8; 64].as_ref())?;
        fs_cb(&mut file);
    }
    file.flush()?;
    drop(file);
    let dt_file = t0.elapsed().as_secs_f64();
    println!("{}: writing done {} {}s", text, n, dt_file);
    std::fs::remove_file("test.obao4")?;

    println!(":-( {}", dt_redb / dt_file);
    Ok(())
}

fn main() -> Result<(), Error> {
    bench(
        "no sync",
        100000,
        |tx| tx.set_durability(redb::Durability::None),
        |_| {},
    )?;
    bench(
        "sync",
        100,
        |tx| tx.set_durability(redb::Durability::Immediate),
        |f| {
            f.flush().ok();
        },
    )?;
    Ok(())
}
