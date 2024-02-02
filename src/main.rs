use std::{
    fs::OpenOptions, io::{self, Write}, path::Path, sync::Mutex, time::Instant
};

use redb::{backends::FileBackend, Database, Error, StorageBackend, TableDefinition};

const TABLE: TableDefinition<(&[u8; 32], u64), &[u8; 64]> = TableDefinition::new("outboard");

fn fs_exists_bench(n: u64) {
    let path = std::env::temp_dir().join("fs_exists_bench");
    std::fs::create_dir_all(&path).ok();
    let t0 = Instant::now();
    for i in 0..n {
        let path = path.join(format!("file-{}", i));
        std::fs::write(path, vec![]).ok();
    }
    println!("create: {} {}", n, t0.elapsed().as_secs_f64());
    let t0 = Instant::now();
    for i in 0..n {
        let path = path.join(format!("file-{}", i));
        let _ = path.exists();
    }
    println!("fs_exists_bench: {} {}", n, t0.elapsed().as_secs_f64());
    std::fs::remove_dir_all(&path).ok();
}

#[derive(Debug)]
struct LogBackend<T: StorageBackend>(T);

impl<T: StorageBackend> StorageBackend for LogBackend<T> {
    fn len(&self) -> io::Result<u64> {
        let res = self.0.len()?;
        println!("len {}", res);
        Ok(res)
    }

    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        println!("read {} {}", offset, len);
        self.0.read(offset, len)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        println!("set_len {}", len);
        self.0.set_len(len)
    }

    fn sync_data(&self, eventual: bool) -> io::Result<()> {
        println!("sync_data {}", eventual);
        self.0.sync_data(eventual)
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
        println!("write {} {}", offset, data.len());
        self.0.write(offset, data)
    }
}

#[derive(Debug)]
struct FastBackend {
    inner: FileBackend,
    file: Mutex<std::fs::File>,
}

impl FastBackend {
    fn new(file: std::fs::File) -> std::result::Result<Self, redb::Error> {
        let inner = FileBackend::new(file.try_clone()?)?;
        Ok(Self {
            inner,
            file: Mutex::new(file),
        })
    }
}

impl StorageBackend for FastBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.inner.read(offset, len)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self, eventual: bool) -> io::Result<()> {
        if !eventual {
            self.inner.sync_data(eventual)
        } else {
            self.file.lock().unwrap().flush()
        }
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
        self.inner.write(offset, data)
    }
}


#[derive(Debug)]
struct SuperFastBackend {
    inner: FileBackend,
}

impl SuperFastBackend {
    fn new(file: std::fs::File) -> std::result::Result<Self, redb::Error> {
        let inner = FileBackend::new(file)?;
        Ok(Self {
            inner,
        })
    }
}

impl StorageBackend for SuperFastBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.inner.read(offset, len)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self, eventual: bool) -> io::Result<()> {
        if !eventual {
            self.inner.sync_data(eventual)
        } else {
            Ok(())
        }
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
        self.inner.write(offset, data)
    }
}

fn bench<B: StorageBackend>(
    text: &str,
    make_backend: impl Fn(std::fs::File) -> std::result::Result<B, redb::Error>,
    n: u64,
    redb_cb: impl Fn(&mut redb::WriteTransaction<'_>),
    fs_cb: impl Fn(&mut std::fs::File),
) -> Result<(), Error> {
    let mut builder = Database::builder();
    builder.set_cache_size(1024 * 1024 * 1024);
    std::fs::remove_file("test.redb").ok();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("test.redb")?;
    let fb = make_backend(file)?;
    let db = builder.create_with_backend(fb)?;

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
    println!(
        "{}: file size is {}",
        text,
        std::fs::metadata("test.redb")?.len()
    );
    std::fs::remove_file("test.redb").ok();

    std::fs::remove_file("test.obao4").ok();
    let mut file = std::fs::File::create("test.obao4")?;
    println!("{}: appending to a file", text);
    let t0 = Instant::now();
    for _ in 0..n {
        file.write([0u8; 64].as_ref())?;
        fs_cb(&mut file);
    }
    file.sync_all()?;
    drop(file);
    let dt_file = t0.elapsed().as_secs_f64();
    println!("{}: writing done {} {}s", text, n, dt_file);
    std::fs::remove_file("test.obao4")?;

    println!(":-( {}", dt_redb / dt_file);
    Ok(())
}

fn main() -> Result<(), Error> {
    fs_exists_bench(100000);
    bench(
        "no sync",
        |file| Ok(FileBackend::new(file)?),
        100000,
        |tx| tx.set_durability(redb::Durability::None),
        |_| {},
    )?;
    bench(
        "flush/fast eventual",
        |file| Ok(FastBackend::new(file)?),
        100000,
        |tx| tx.set_durability(redb::Durability::Eventual),
        |f| {
            f.flush().ok();
        },
    )?;
    bench(
        "no sync/super fast eventual",
        |file| Ok(SuperFastBackend::new(file)?),
        100000,
        |tx| tx.set_durability(redb::Durability::Eventual),
        |f| {
            f.flush().ok();
        },
    )?;
    bench(
        "flush/eventual",
        |file| Ok(FileBackend::new(file)?),
        100,
        |tx| tx.set_durability(redb::Durability::Eventual),
        |f| {
            f.flush().ok();
        },
    )?;
    bench(
        "sync_data/immediate",
        |file| Ok(FileBackend::new(file)?),
        100,
        |tx| tx.set_durability(redb::Durability::Immediate),
        |f| {
            f.sync_data().ok();
        },
    )?;
    Ok(())
}
