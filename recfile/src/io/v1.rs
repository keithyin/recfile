use std::{
    fs,
    io::{Read, Seek, Write},
    num::NonZero,
    ops::{Deref, DerefMut},
    path::{self, Path},
    sync::{Arc, Barrier, Mutex, MutexGuard, atomic::AtomicBool},
    thread, usize,
};

use crossbeam::channel::{Receiver, Sender};

use super::get_bincode_cfg;

#[derive(Debug, Clone, Default, bincode::Encode, bincode::Decode)]
struct WritePositions(pub Vec<u64>);
impl Deref for WritePositions {
    type Target = Vec<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for WritePositions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, Default, bincode::Encode, bincode::Decode)]
struct WritePositionsMeta(Vec<(u64, u64)>);
impl Deref for WritePositionsMeta {
    type Target = Vec<(u64, u64)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WritePositionsMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct Locations {
    pub cur_position: u64,
    pub write_positions: WritePositions,
    pub write_positions_meta: WritePositionsMeta,

    pub meta_cursor: usize, // this two cursor are for file reader, 下一个要处理哪块
    pub write_position_cursor: usize,
}

#[allow(unused)]
impl Locations {
    pub fn new(cur_pos: u64) -> Self {
        Self {
            cur_position: cur_pos,
            write_positions: WritePositions::default(),
            write_positions_meta: WritePositionsMeta::default(),
            meta_cursor: 0,
            write_position_cursor: 0,
        }
    }

    ///
    pub fn compute_write_position_and_serial_of_write_positions(
        locations: &mut MutexGuard<'_, Locations>,
    ) -> Option<(u64, Vec<u8>)> {
        let cfg = get_bincode_cfg();
        let cur_pos = locations.cur_position;
        if locations.write_positions.is_empty() {
            return None;
        }
        let serialize = bincode::encode_to_vec(&locations.write_positions, cfg).unwrap();
        let write_pos = locations.cur_position;
        locations.cur_position += serialize.len() as u64;

        let write_pos_and_serial = (write_pos, serialize.len() as u64);
        locations.write_positions_meta.push(write_pos_and_serial);
        locations.write_positions.clear();
        Some((write_pos, serialize))
    }
}

impl Default for Locations {
    fn default() -> Self {
        Self {
            cur_position: 2 * 1024 * 1024 + 8, // 初始位置为2M + 8bytes
            write_positions: WritePositions(vec![]),
            write_positions_meta: WritePositionsMeta(vec![]),
            meta_cursor: 0,
            write_position_cursor: 0,
        }
    }
}

impl From<WritePositionsMeta> for Locations {
    fn from(value: WritePositionsMeta) -> Self {
        let mut res = Self::default();
        res.write_positions_meta = value;
        res
    }
}

const RFF_VERSION: u32 = 1;

/// 存储序列化的对象，核心实现是二级存储
/// 开头的 u32 存储 文件格式的版本，之后的 u32 存储的是 一级索引的长度，一级索引是 Vec<(usize, usize)> 序列化的结果。
///     一级索引 提供 2M 空间进行存储。
/// 之后每1000次写入会记录其每次写入的位置。所以rfffile 最多大概支持 32,000,000 次写入。
/// ----file
/// u32,u32,Vec<(usize, usize)>(2M+8bytes) .....(1000 write) positionsOfEachWrite
pub struct RffWriter {
    fname: path::PathBuf,
    threads: usize,
    barrier: Barrier,

    positions: Mutex<Locations>,
    worker_threads_started_flag: AtomicBool,
    writer_recv: Receiver<Vec<u8>>,
    handlers: Mutex<Option<Vec<thread::JoinHandle<()>>>>,
    writer_drop_barrier: Barrier,
}

impl RffWriter {
    ///
    /// sender is used for to send data to be written. the data should be bytes stream
    pub fn new_writer<P>(p: P, threads: NonZero<usize>) -> (Arc<Self>, Sender<Vec<u8>>)
    where
        P: AsRef<Path>,
    {
        let (sender, recv) = crossbeam::channel::bounded::<Vec<u8>>(1000);

        let p = p.as_ref().to_owned();
        (
            Self {
                fname: p.into(),
                threads: threads.get(),
                barrier: Barrier::new(threads.get()),
                positions: Mutex::new(Locations::default()),
                worker_threads_started_flag: AtomicBool::new(false),
                writer_recv: recv,
                handlers: Mutex::new(Some(vec![])),
                writer_drop_barrier: Barrier::new(2),
            }
            .into(),
            sender,
        )
    }
    pub fn start_write_worker(self: &Arc<Self>) {
        if self
            .worker_threads_started_flag
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.worker_threads_started_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut file = fs::File::create(&self.fname).unwrap();
        println!("create file success: {:?}", &self.fname);
        file.write_all(&RFF_VERSION.to_le_bytes()).unwrap();
        file.flush().unwrap();
        // file.set_len(1024 * 1024 * 1024 * 30).unwrap();
        drop(file);

        for idx in 0..self.threads {
            let handler = {
                let self_clone = Arc::clone(self);
                thread::spawn(move || {
                    self_clone.write_worker(idx);
                })
            };
            self.handlers
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(handler);
        }
    }

    fn write_worker(self: &Arc<Self>, idx: usize) {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&self.fname)
            .unwrap();
        let recv = self.writer_recv.clone();
        for data in recv {
            self.write(&data, &mut file);
        }
        self.barrier.wait();
        if idx == 0 {
            let cfg = get_bincode_cfg();
            {
                let mut locations = self.positions.lock().unwrap();
                if let Some((pos, serial)) =
                    Locations::compute_write_position_and_serial_of_write_positions(&mut locations)
                {
                    file.seek(std::io::SeekFrom::Start(pos)).unwrap();
                    file.write_all(&serial).unwrap()
                }
            }

            // println!(
            //     "write_positions_meta:{:?}",
            //     &self.positions.lock().unwrap().write_positions_meta
            // );

            let serialize =
                bincode::encode_to_vec(&self.positions.lock().unwrap().write_positions_meta, cfg)
                    .unwrap();
            // println!("write_positions_meta_serial_len:{}", serialize.len());
            file.seek(std::io::SeekFrom::Start(4)).unwrap();
            file.write_all(&(serialize.len() as u32).to_le_bytes())
                .unwrap();
            file.seek(std::io::SeekFrom::Start(8)).unwrap();
            file.write_all(&serialize).unwrap();
            file.flush().unwrap();
            self.writer_drop_barrier.wait();
        }
    }

    fn write(self: &Arc<Self>, data: &[u8], file: &mut fs::File) {
        let cur_pos = {
            let mut locations = self.positions.lock().unwrap();
            let cur_pos = locations.cur_position;
            locations.cur_position += data.len() as u64;
            locations.write_positions.push(cur_pos);
            cur_pos
        };

        file.seek(std::io::SeekFrom::Start(cur_pos as u64)).unwrap();
        file.write_all(data).unwrap();

        let value2write = {
            let mut locations = self.positions.lock().unwrap();

            // 每1000次写入记录一次位置
            let value2write = if locations.write_positions.len() >= 1000 {
                Some(
                    Locations::compute_write_position_and_serial_of_write_positions(&mut locations)
                        .unwrap(),
                )
            } else {
                None
            };

            value2write
        };

        if let Some((write_pos, serialize)) = value2write {
            file.seek(std::io::SeekFrom::Start(write_pos as u64))
                .unwrap();
            file.write_all(&serialize).unwrap();
        }
    }

    pub fn wait_for_write_done(self: Arc<Self>) {
        self.writer_drop_barrier.wait();
    }
}

pub struct RffReader {
    threads: usize,
    fname: path::PathBuf,
    positions: Mutex<Locations>,
    read_sender: Mutex<Option<Sender<Vec<u8>>>>,
}

impl RffReader {
    pub fn new_reader<P>(p: P, threads: NonZero<usize>) -> (Arc<Self>, Receiver<Vec<u8>>)
    where
        P: AsRef<Path>,
    {
        let p = p.as_ref().to_owned();
        let mut file = fs::File::open(&p).unwrap();
        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes).unwrap();
        let version = u32::from_le_bytes(version_bytes);

        let mut meta_len = [0u8; 4];
        file.seek(std::io::SeekFrom::Start(4)).unwrap();
        file.read_exact(&mut meta_len).unwrap();
        let meta_len = u32::from_le_bytes(meta_len);
        // println!("version:{}, metalen:{}", version, meta_len);
        file.seek(std::io::SeekFrom::Start(8)).unwrap();
        let mut positions_meta = vec![0_u8; meta_len as usize];
        file.read_exact(&mut positions_meta).unwrap();

        let (write_positions_meta, nbytes): (WritePositionsMeta, usize) =
            bincode::decode_from_slice(&positions_meta, get_bincode_cfg()).unwrap();
        assert_eq!(nbytes, meta_len as usize);

        assert_eq!(
            version, RFF_VERSION,
            "Unsupported rff version. expected {}, found {}",
            RFF_VERSION, version
        );

        let (sender, recv) = crossbeam::channel::bounded(1000);

        (
            Self {
                fname: p.into(),
                threads: threads.get(),
                positions: Mutex::new(write_positions_meta.into()),
                read_sender: Mutex::new(sender.into()),
            }
            .into(),
            recv,
        )
    }

    pub fn start_read_worker(self: &Arc<Self>) {
        let sender = self.read_sender.lock().unwrap().take();
        if let Some(sender) = sender {
            for _ in 0..self.threads {
                thread::spawn({
                    let reader = Arc::clone(&self);
                    let sender = sender.clone();
                    move || {
                        reader.read_worker(sender);
                    }
                });
            }
        } else {
            return;
        }
    }

    pub fn read_worker(self: Arc<Self>, sender: Sender<Vec<u8>>) {
        let mut file = fs::File::open(&self.fname).unwrap();
        while let Some(data) = self.read(&mut file) {
            sender.send(data).unwrap();
        }
        for _ in 0..self.threads {
            thread::spawn({
                let reader = Arc::clone(&self);
                let sender = sender.clone();
                move || {
                    let mut file = fs::File::open(&reader.fname).unwrap();
                    while let Some(data) = reader.read(&mut file) {
                        sender.send(data).unwrap();
                    }
                }
            });
        }
    }

    pub fn read(self: &Arc<Self>, file: &mut fs::File) -> Option<Vec<u8>> {
        let (start, len) = {
            let mut position = self.positions.lock().unwrap();

            // TODO: check it
            if position.meta_cursor == 0
                || position.write_position_cursor + 1 >= position.write_positions.len()
            {
                // read new write positions
                if position.meta_cursor >= position.write_positions_meta.len() {
                    return None;
                }
                let (start, len) = position.write_positions_meta[position.meta_cursor];
                file.seek(std::io::SeekFrom::Start(start)).unwrap();
                let mut buf = vec![0; len as usize];
                file.read_exact(&mut buf).unwrap();
                let (mut write_positions, nbytes): (WritePositions, usize) =
                    bincode::decode_from_slice(&buf, get_bincode_cfg()).unwrap();
                write_positions.push(start);
                position.write_positions = write_positions;

                assert_eq!(nbytes, len as usize);
                position.write_position_cursor = 0;
                position.meta_cursor += 1;
            }
            let start = position.write_positions[position.write_position_cursor];
            let len = position.write_positions[position.write_position_cursor + 1] - start;
            position.write_position_cursor += 1;

            // println!("reader: start:{},len:{}", start, len);

            (start, len)
        };
        let mut buf = vec![0; len as usize];
        file.seek(std::io::SeekFrom::Start(start)).unwrap();
        file.read_exact(&mut buf).unwrap();

        Some(buf)
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZero;

    use tempfile::NamedTempFile;

    use super::{RffReader, RffWriter, get_bincode_cfg};

    #[test]
    fn test_rff_rw() {
        let named_file = NamedTempFile::new().unwrap();
        let (writer, sender) = RffWriter::new_writer(named_file.path(), NonZero::new(2).unwrap());
        writer.start_write_worker();
        for i in 0_u32..33559 {
            sender
                .send(bincode::encode_to_vec(i, get_bincode_cfg()).unwrap())
                .unwrap();
        }
        drop(sender);
        writer.wait_for_write_done();
        // drop(named_file);

        println!("send done");
        // thread::sleep(Duration::from_secs(2));

        let (reader, recv) = RffReader::new_reader(named_file.path(), NonZero::new(2).unwrap());
        reader.start_read_worker();
        let mut results = vec![];
        for v in recv {
            let (v, _nbytes): (u32, usize) =
                bincode::decode_from_slice(&v, get_bincode_cfg()).unwrap();
            println!("v:{}", v);
            results.push(v);
        }
        drop(reader);

        results.sort();
        println!("{:?}", results);
    }
}
