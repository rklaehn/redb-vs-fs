use std::{time::Instant, collections::hash_map::DefaultHasher, hash::Hasher, sync::Arc};

use redb::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pages");

fn mk_key(i: u64, j: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&i.to_be_bytes());
    key[8..].copy_from_slice(&j.to_be_bytes());
    key
}

fn main() -> Result<(), Error> {
    let mut builder = Database::builder();

    builder.set_cache_size(1024 * 1024 * 1024);
    let db = builder.create("test.redb")?;

    println!("writing");
    for i in 0..100u64 {
        let mut write_txn = db.begin_write()?;
        write_txn.set_durability(redb::Durability::Eventual);
        {
            let mut table = write_txn.open_table(TABLE)?;
            for j in 0..100u64 {
                let key = mk_key(i, j);
                table.insert(key.as_slice(), [0u8; 1024 * 1024].as_slice())?;
            }
        }
        write_txn.commit()?;
        println!("{}", i);
    }

    println!("reading");
    let t0 = Instant::now();
    let db = Arc::new(db);
    let handles = (0..16).map(|i| {
        let db = db.clone();
        std::thread::spawn(move || {
            let mut hasher = DefaultHasher::new();
            let mut total = 0;
            for i in 0..100u64 {
                let read_txn = db.begin_read()?;
                let table = read_txn.open_table(TABLE)?;
                for j in 0..100u64 {
                    let key = mk_key(i, j);
                    let data = table.get(key.as_slice())?.unwrap();
                    total += data.value().len();
                    hasher.write(data.value());
                }
                println!("{}", i);
            }
            let hash = hasher.finish();
            Ok::<_, redb::Error>(hash)
        })
    }).collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap()?;
    }

    println!("reading done {}", t0.elapsed().as_secs_f64());
    // assert_eq!(table.get("my_key")?.unwrap().value(), 123);

    Ok(())
}
