use std::{
    collections::HashSet,
    num::NonZero,
    ops::{Deref, DerefMut},
    path::{self, Path, PathBuf},
    str::FromStr,
    time::Instant,
};

use clap::Parser;
use crossbeam::channel::{Receiver, Sender};
use gass::codec::{lz4_block_compress, zstd_block_compress, zstd_block_decompress};
use gskits::{
    gsbam::bam_record_ext::BamRecordExt,
    pbar::{DEFAULT_INTERVAL, get_spin_pb},
};
use recfile::io::{
    get_bincode_cfg,
    v1::{RffReader, RffWriter},
    v2,
};
use rust_htslib::bam::{self, Read, Record, record::Aux};
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    pub in_path: String,

    pub out_path: String,

    #[arg(long="mode", default_value_t=String::from_str("b2g").unwrap(), help="b2g/g2b, bam2rff or rff2bam")]
    pub mode: String,
    #[arg(long = "in-threads", default_value_t = 4)]
    pub in_threads: usize,

    #[arg(long = "codec-threads", default_value_t = 4)]
    pub codec_threads: usize,

    #[arg(long = "o-threads", default_value_t = 4)]
    pub writer_threads: usize,

    #[arg(long = "rep-times", help = "only valid for b2g")]
    pub rep_times: Option<usize>,

    #[arg(long = "batch-size", help = "only valid for b2g")]
    pub batch_size: Option<usize>,
}

impl Cli {
    fn get_out_path(&self) -> PathBuf {
        path::Path::new(&self.out_path).into()
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct ReadInfo {
    pub name: String,
    pub seq: String,
    pub cx: Option<u8>,
    pub ch: Option<u32>,
    pub np: Option<u32>,
    pub rq: Option<f32>,
    pub qual: Option<Vec<u8>>, // phreq, no offset
    pub dw: Option<Vec<u8>>,
    pub ar: Option<Vec<u8>>,
    pub cr: Option<Vec<u8>>,
    pub be: Option<Vec<u32>>,
    pub nn: Option<Vec<u8>>,
    pub wd: Option<Vec<u8>>, // linker width
    pub sd: Option<Vec<u8>>, // standard devition
    pub sp: Option<Vec<u8>>, // slope
}

impl ReadInfo {
    pub fn new_fa_record(name: String, seq: String) -> Self {
        let mut res = Self::default();
        res.name = name;
        res.seq = seq;
        res
    }

    pub fn new_fq_record(name: String, seq: String, qual: Vec<u8>) -> Self {
        let mut res = ReadInfo::new_fa_record(name, seq);
        res.qual = Some(qual);
        res
    }

    pub fn from_bam_record(
        record: &Record,
        qname_suffix: Option<&str>,
        tags: &HashSet<String>,
    ) -> Self {
        let mut qname = unsafe { String::from_utf8_unchecked(record.qname().to_vec()) };
        if let Some(suffix) = qname_suffix {
            qname.push_str(suffix);
        }
        let record_ext = BamRecordExt::new(record);
        let seq = unsafe { String::from_utf8_unchecked(record.seq().as_bytes()) };

        let dw = if tags.contains("dw") {
            record_ext
                .get_dw()
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        let ar = if tags.contains("ar") {
            record_ext
                .get_ar()
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        let cr = if tags.contains("cr") {
            record_ext
                .get_cr()
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        let nn = if tags.contains("nn") {
            record_ext
                .get_nn()
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        let wd = if tags.contains("wd") {
            record_ext
                .get_uint_list(b"wd")
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        let sd = if tags.contains("sd") {
            record_ext
                .get_uint_list(b"sd")
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        let sp = if tags.contains("sp") {
            record_ext
                .get_uint_list(b"sd")
                .map(|v| v.into_iter().map(|v| v as u8).collect())
        } else {
            None
        };

        Self {
            name: qname,
            seq: seq,
            cx: record_ext.get_cx(),
            ch: record_ext.get_ch(),
            np: record_ext.get_np().map(|v| v as u32),
            rq: record_ext.get_rq(),
            qual: Some(record_ext.get_qual().to_vec()),
            dw: dw,
            ar: ar,
            cr: cr,
            be: record_ext.get_be(),
            nn: nn,
            wd: wd,
            sd: sd,
            sp: sp,
        }
    }

    pub fn to_record(&self) -> Record {
        let mut record = Record::new();

        // 设置 qname

        // 设置 seq
        record.set(
            self.name.as_bytes(),
            None,
            self.seq.as_bytes(),
            self.qual.as_ref().unwrap(),
        );
        // 设置 tags
        macro_rules! push_aux {
            ($tag:expr, $value:expr) => {
                record.push_aux($tag, $value).unwrap();
            };
        }

        if let Some(cx) = self.cx {
            push_aux!(b"cx", Aux::U8(cx));
        }
        if let Some(ch) = self.ch {
            push_aux!(b"ch", Aux::U32(ch));
        }
        if let Some(np) = self.np {
            push_aux!(b"np", Aux::U32(np));
        }
        if let Some(rq) = self.rq {
            push_aux!(b"rq", Aux::Float(rq));
        }
        if let Some(be) = &self.be {
            push_aux!(b"be", Aux::ArrayU32(be.into()));
        }
        if let Some(dw) = &self.dw {
            push_aux!(b"dw", Aux::ArrayU8(dw.into()));
        }
        if let Some(ar) = &self.ar {
            push_aux!(b"ar", Aux::ArrayU8(ar.into()));
        }
        if let Some(cr) = &self.cr {
            push_aux!(b"cr", Aux::ArrayU8(cr.into()));
        }
        if let Some(nn) = &self.nn {
            push_aux!(b"nn", Aux::ArrayU8(nn.into()));
        }
        if let Some(wd) = &self.wd {
            push_aux!(b"wd", Aux::ArrayU8(wd.into()));
        }
        if let Some(sd) = &self.sd {
            push_aux!(b"sd", Aux::ArrayU8(sd.into()));
        }
        if let Some(sp) = &self.sp {
            push_aux!(b"sp", Aux::ArrayU8(sp.into()));
        }

        record
    }
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct BatchReads(pub Vec<ReadInfo>);
impl Deref for BatchReads {
    type Target = Vec<ReadInfo>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BatchReads {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn bam_reader<P>(
    bam_path: P,
    bam_threads: usize,
    sender: Sender<bam::Record>,
    rep_times: Option<usize>,
) where
    P: AsRef<Path>,
{
    let mut bam_reader = bam::Reader::from_path(&bam_path).expect(&format!("read error"));
    bam_reader.set_threads(bam_threads).unwrap();

    let pb = get_spin_pb(
        format!("reading {}", bam_path.as_ref().to_str().unwrap()),
        DEFAULT_INTERVAL,
    );
    let rep_times = rep_times.unwrap_or(1);
    loop {
        let mut record = bam::Record::new();
        if let Some(Ok(_)) = bam_reader.read(&mut record) {
            for _ in 0..rep_times {
                let rec = record.clone();
                pb.inc(1);
                sender.send(rec).unwrap();
            }

            if rep_times > 1 {
                break;
            }
        } else {
            break;
        }
    }

    pb.finish();
}

fn bam_reader_to_vec<P>(bam_path: P, bam_threads: usize, rep_times: Option<usize>) -> Vec<Record>
where
    P: AsRef<Path>,
{
    let mut bam_reader = bam::Reader::from_path(&bam_path).expect(&format!("read error"));
    bam_reader.set_threads(bam_threads).unwrap();

    let pb = get_spin_pb(
        format!("reading {}", bam_path.as_ref().to_str().unwrap()),
        DEFAULT_INTERVAL,
    );
    let rep_times = rep_times.unwrap_or(1);
    let mut result = vec![];
    loop {
        let mut record = bam::Record::new();
        if let Some(Ok(_)) = bam_reader.read(&mut record) {
            for _ in 0..rep_times {
                let rec = record.clone();
                pb.inc(1);
                result.push(rec);
            }

            if rep_times > 1 {
                break;
            }
        } else {
            break;
        }
    }

    pb.finish();
    return result;
}

fn enc_worker(recv: Receiver<bam::Record>, sender: Sender<Vec<u8>>, batch_size: Option<usize>) {
    let mut tags = HashSet::new();
    tags.insert("dw".to_string());
    tags.insert("ar".to_string());
    tags.insert("cr".to_string());
    tags.insert("be".to_string());
    tags.insert("nn".to_string());
    tags.insert("wd".to_string());
    tags.insert("sd".to_string());
    tags.insert("sp".to_string());

    let batch_size = batch_size.unwrap_or(1);
    let cfg = get_bincode_cfg();
    let mut record_batch = BatchReads(vec![]);
    let mut tot_len = 0;
    for record in recv {
        let read = ReadInfo::from_bam_record(&record, None, &tags);
        record_batch.push(read);
        if record_batch.len() == batch_size {
            let serial = bincode::encode_to_vec(&record_batch, cfg).unwrap();
            tot_len += serial.len();

            sender.send(serial).unwrap();
            record_batch = BatchReads(vec![]);
        }
    }
    println!("len:{}", tot_len);

    if !record_batch.is_empty() {
        let serial = bincode::encode_to_vec(&record_batch, cfg).unwrap();

        sender.send(serial).unwrap();
    }
}

fn enc_worker_with_codec(
    recv: Receiver<bam::Record>,
    sender: Sender<Vec<u8>>,
    batch_size: Option<usize>,
) {
    let mut tags = HashSet::new();
    tags.insert("dw".to_string());
    tags.insert("ar".to_string());
    tags.insert("cr".to_string());
    tags.insert("be".to_string());
    tags.insert("nn".to_string());
    tags.insert("wd".to_string());
    tags.insert("sd".to_string());
    tags.insert("sp".to_string());

    let batch_size = batch_size.unwrap_or(1);
    let cfg = get_bincode_cfg();
    let mut record_batch = BatchReads(vec![]);
    let mut tot_len = 0;
    for record in recv {
        let read = ReadInfo::from_bam_record(&record, None, &tags);
        record_batch.push(read);
        if record_batch.len() == batch_size {
            let serial = bincode::encode_to_vec(&record_batch, cfg).unwrap();
            tot_len += serial.len();

            sender.send(zstd_block_compress(&serial)).unwrap();
            record_batch = BatchReads(vec![]);
        }
    }
    println!("len:{}", tot_len);

    if !record_batch.is_empty() {
        let serial = bincode::encode_to_vec(&record_batch, cfg).unwrap();

        sender.send(zstd_block_compress(&serial)).unwrap();
    }
}

fn enc_worker_with_codec_lz4(
    recv: Receiver<bam::Record>,
    sender: Sender<Vec<u8>>,
    batch_size: Option<usize>,
) {
    let mut tags = HashSet::new();
    tags.insert("dw".to_string());
    tags.insert("ar".to_string());
    tags.insert("cr".to_string());
    tags.insert("be".to_string());
    tags.insert("nn".to_string());
    tags.insert("wd".to_string());
    tags.insert("sd".to_string());
    tags.insert("sp".to_string());

    let batch_size = batch_size.unwrap_or(1);
    let cfg = get_bincode_cfg();
    let mut record_batch = BatchReads(vec![]);
    let mut tot_len = 0;
    for record in recv {
        let read = ReadInfo::from_bam_record(&record, None, &tags);
        record_batch.push(read);
        if record_batch.len() == batch_size {
            let serial = bincode::encode_to_vec(&record_batch, cfg).unwrap();
            tot_len += serial.len();

            sender.send(lz4_block_compress(&serial)).unwrap();
            record_batch = BatchReads(vec![]);
        }
    }
    println!("len:{}", tot_len);

    if !record_batch.is_empty() {
        let serial = bincode::encode_to_vec(&record_batch, cfg).unwrap();

        sender.send(lz4_block_compress(&serial)).unwrap();
    }
}

fn b2g(cli: &Cli) {
    let out_path = cli.get_out_path();
    println!("{:?}", out_path);
    std::thread::scope(|thread_scope| {
        let (writer, sender4writer) =
            RffWriter::new_writer(&out_path, NonZero::new(cli.writer_threads).unwrap());
        writer.start_write_worker();
        let (bam_record_sender, bam_record_recv) = crossbeam::channel::bounded(1000);
        thread_scope.spawn({
            let bam_path = cli.in_path.clone();
            let bam_threads = cli.in_threads;
            let rep_times = cli.rep_times.clone();
            move || {
                bam_reader(&bam_path, bam_threads, bam_record_sender, rep_times);
            }
        });

        for _ in 0..cli.codec_threads {
            thread_scope.spawn({
                let recv = bam_record_recv.clone();
                let sender = sender4writer.clone();
                let batch_size = cli.batch_size.clone();
                move || {
                    enc_worker(recv, sender, batch_size);
                }
            });
        }
        drop(sender4writer);
        writer.wait_for_write_done();
    });
}

fn decode_worker(sender: Sender<bam::Record>, recv: Receiver<Vec<u8>>) {
    let cfg = get_bincode_cfg();
    for data in recv {
        let (batch_records, _nbytes): (BatchReads, usize) =
            bincode::decode_from_slice(&data, cfg).unwrap();
        batch_records.iter().for_each(|read| {
            sender.send(read.to_record()).unwrap();
        });
    }
}

fn decode_worker_codec(sender: Sender<bam::Record>, recv: Receiver<Vec<u8>>) {
    let cfg = get_bincode_cfg();
    for data in recv {
        let data = zstd_block_decompress(&data);
        let (batch_records, _nbytes): (BatchReads, usize) =
            bincode::decode_from_slice(&data, cfg).unwrap();
        batch_records.iter().for_each(|read| {
            sender.send(read.to_record()).unwrap();
        });
    }
}

fn bam_writer<P>(fname: P, recv: Receiver<bam::Record>, bam_threads: Option<usize>)
where
    P: AsRef<Path>,
{
    let bam_threads = bam_threads.unwrap_or(4);
    let mut header = bam::Header::new();
    let mut hd = bam::header::HeaderRecord::new(b"HD");
    hd.push_tag(b"VN", "1.5");
    hd.push_tag(b"SO", "unknown");
    header.push_record(&hd);

    let mut hd = bam::header::HeaderRecord::new(b"SQ");
    hd.push_tag(b"SN", "chr1");
    hd.push_tag(b"LN", "1234");
    header.push_record(&hd);

    let mut bam_writer = bam::Writer::from_path(&fname, &header, bam::Format::Bam).unwrap();
    bam_writer.set_threads(bam_threads).unwrap();
    let pb = get_spin_pb(
        format!("writing {}", fname.as_ref().to_str().unwrap()),
        DEFAULT_INTERVAL,
    );
    for record in recv {
        bam_writer.write(&record).unwrap();
        pb.inc(1);
    }
    pb.finish();
}

fn bam_writer_from_vec<P>(fname: P, recv: Vec<Record>, bam_threads: Option<usize>)
where
    P: AsRef<Path>,
{
    let bam_threads = bam_threads.unwrap_or(4);
    let mut header = bam::Header::new();
    let mut hd = bam::header::HeaderRecord::new(b"HD");
    hd.push_tag(b"VN", "1.5");
    hd.push_tag(b"SO", "unknown");
    header.push_record(&hd);

    let mut hd = bam::header::HeaderRecord::new(b"SQ");
    hd.push_tag(b"SN", "chr1");
    hd.push_tag(b"LN", "1234");
    header.push_record(&hd);

    let mut bam_writer = bam::Writer::from_path(&fname, &header, bam::Format::Bam).unwrap();
    bam_writer.set_threads(bam_threads).unwrap();
    let pb = get_spin_pb(
        format!("writing {}", fname.as_ref().to_str().unwrap()),
        DEFAULT_INTERVAL,
    );
    for record in recv {
        bam_writer.write(&record).unwrap();
        pb.inc(1);
    }
    pb.finish();
}

fn bam_writer_from_vec_uncompressed<P>(fname: P, recv: Vec<Record>, bam_threads: Option<usize>)
where
    P: AsRef<Path>,
{
    let bam_threads = bam_threads.unwrap_or(4);
    let mut header = bam::Header::new();
    let mut hd = bam::header::HeaderRecord::new(b"HD");
    hd.push_tag(b"VN", "1.5");
    hd.push_tag(b"SO", "unknown");
    header.push_record(&hd);

    let mut hd = bam::header::HeaderRecord::new(b"SQ");
    hd.push_tag(b"SN", "chr1");
    hd.push_tag(b"LN", "1234");
    header.push_record(&hd);

    let mut bam_writer = bam::Writer::from_path(&fname, &header, bam::Format::Bam).unwrap();
    bam_writer.set_threads(bam_threads).unwrap();
    bam_writer
        .set_compression_level(bam::CompressionLevel::Uncompressed)
        .unwrap();
    let pb = get_spin_pb(
        format!("writing {}", fname.as_ref().to_str().unwrap()),
        DEFAULT_INTERVAL,
    );
    for record in recv {
        bam_writer.write(&record).unwrap();
        pb.inc(1);
    }
    pb.finish();
}

fn g2b(cli: &Cli) {
    let (reader, recv) = RffReader::new_reader(&cli.in_path, NonZero::new(cli.in_threads).unwrap());
    reader.start_read_worker();
    std::thread::scope(|scope| {
        let (decode_sender, decode_recv) = crossbeam::channel::bounded(1000);
        for _ in 0..cli.codec_threads {
            scope.spawn({
                let recv = recv.clone();
                let sender = decode_sender.clone();
                move || {
                    decode_worker(sender, recv);
                }
            });
        }
        drop(decode_sender);
        bam_writer(&cli.get_out_path(), decode_recv, None);
    });
}

fn b2g2_with_codec(cli: &Cli) {
    let out_path = cli.get_out_path();
    println!("{:?}", out_path);
    std::thread::scope(|thread_scope| {
        let (bam_record_sender, bam_record_recv) = crossbeam::channel::bounded(1000);
        thread_scope.spawn({
            let bam_path = cli.in_path.clone();
            let bam_threads = cli.in_threads;
            let rep_times = cli.rep_times.clone();
            move || {
                bam_reader(&bam_path, bam_threads, bam_record_sender, rep_times);
            }
        });

        let (writer_send, write_recv) = crossbeam::channel::bounded(1000);
        for _ in 0..cli.codec_threads {
            thread_scope.spawn({
                let recv = bam_record_recv.clone();
                let sender = writer_send.clone();
                let batch_size = cli.batch_size.clone();
                move || {
                    enc_worker_with_codec(recv, sender, batch_size);
                }
            });
        }
        drop(writer_send);
        drop(bam_record_recv);

        let mut writer =
            v2::RffWriter::new_writer(&out_path, NonZero::new(cli.writer_threads).unwrap());
        let pb = get_spin_pb(format!("writing {:?}", out_path), DEFAULT_INTERVAL);
        for data in write_recv {
            let _ = writer.write_serialized_data(&data);
            pb.inc(1);
        }
        pb.finish();
    });
}

fn b2g2_with_codec_lz4(cli: &Cli) {
    let out_path = cli.get_out_path();
    println!("{:?}", out_path);
    std::thread::scope(|thread_scope| {
        let (bam_record_sender, bam_record_recv) = crossbeam::channel::bounded(1000);
        thread_scope.spawn({
            let bam_path = cli.in_path.clone();
            let bam_threads = cli.in_threads;
            let rep_times = cli.rep_times.clone();
            move || {
                bam_reader(&bam_path, bam_threads, bam_record_sender, rep_times);
            }
        });

        let (writer_send, write_recv) = crossbeam::channel::bounded(1000);
        for _ in 0..cli.codec_threads {
            thread_scope.spawn({
                let recv = bam_record_recv.clone();
                let sender = writer_send.clone();
                let batch_size = cli.batch_size.clone();
                move || {
                    enc_worker_with_codec_lz4(recv, sender, batch_size);
                }
            });
        }
        drop(writer_send);
        drop(bam_record_recv);

        let mut writer =
            v2::RffWriter::new_writer(&out_path, NonZero::new(cli.writer_threads).unwrap());
        let pb = get_spin_pb(format!("writing {:?}", out_path), DEFAULT_INTERVAL);
        for data in write_recv {
            let _ = writer.write_serialized_data(&data);
            pb.inc(1);
        }
        pb.finish();
    });
}

fn b2g2(cli: &Cli) {
    let out_path = cli.get_out_path();
    println!("{:?}", out_path);
    std::thread::scope(|thread_scope| {
        let (bam_record_sender, bam_record_recv) = crossbeam::channel::bounded(1000);
        thread_scope.spawn({
            let bam_path = cli.in_path.clone();
            let bam_threads = cli.in_threads;
            let rep_times = cli.rep_times.clone();
            move || {
                bam_reader(&bam_path, bam_threads, bam_record_sender, rep_times);
            }
        });

        let (writer_send, write_recv) = crossbeam::channel::bounded(1000);
        for _ in 0..cli.codec_threads {
            thread_scope.spawn({
                let recv = bam_record_recv.clone();
                let sender = writer_send.clone();
                let batch_size = cli.batch_size.clone();
                move || {
                    enc_worker(recv, sender, batch_size);
                }
            });
        }
        drop(writer_send);
        drop(bam_record_recv);

        let mut writer =
            v2::RffWriter::new_writer(&out_path, NonZero::new(cli.writer_threads).unwrap());
        let pb = get_spin_pb(format!("writing {:?}", out_path), DEFAULT_INTERVAL);
        for data in write_recv {
            let _ = writer.write_serialized_data(&data);
            pb.inc(1);
        }
        pb.finish();
    });
}

fn g2b2(cli: &Cli) {
    std::thread::scope(|scope| {
        let (read_sender, read_recv) = crossbeam::channel::bounded(1000);

        scope.spawn({
            let in_path = cli.in_path.clone();
            let threads = cli.in_threads;
            move || {
                let mut reader = v2::RffReader::new_reader(in_path, NonZero::new(threads).unwrap());
                while let Some(v) = reader.read_serialized_data() {
                    read_sender.send(v).unwrap();
                }
            }
        });

        let (decode_sender, decode_recv) = crossbeam::channel::bounded(1000);
        for _ in 0..cli.codec_threads {
            scope.spawn({
                let recv = read_recv.clone();
                let sender = decode_sender.clone();
                move || {
                    decode_worker(sender, recv);
                }
            });
        }
        drop(decode_sender);
        bam_writer(&cli.get_out_path(), decode_recv, Some(cli.writer_threads));
    });
}

fn g2b2_with_codec(cli: &Cli) {
    std::thread::scope(|scope| {
        let (read_sender, read_recv) = crossbeam::channel::bounded(1000);

        scope.spawn({
            let in_path = cli.in_path.clone();
            let threads = cli.in_threads;
            move || {
                let mut reader = v2::RffReader::new_reader(in_path, NonZero::new(threads).unwrap());
                while let Some(v) = reader.read_serialized_data() {
                    read_sender.send(v).unwrap();
                }
            }
        });

        let (decode_sender, decode_recv) = crossbeam::channel::bounded(1000);
        for _ in 0..cli.codec_threads {
            scope.spawn({
                let recv = read_recv.clone();
                let sender = decode_sender.clone();
                move || {
                    decode_worker_codec(sender, recv);
                }
            });
        }
        drop(decode_sender);
        bam_writer(&cli.get_out_path(), decode_recv, Some(cli.writer_threads));
    });
}

fn b2b(cli: &Cli) {
    std::thread::scope(|scope| {
        let (record_sender, record_recv) = crossbeam::channel::bounded(1000);
        scope.spawn({
            let bam_path = cli.in_path.clone();
            let in_threads = cli.in_threads;
            move || {
                bam_reader(bam_path, in_threads, record_sender, None);
            }
        });

        scope.spawn({
            let bam_path = cli.out_path.clone();
            let out_threads = cli.writer_threads;
            move || {
                bam_writer(bam_path, record_recv, Some(out_threads));
            }
        });
    });
}

fn b_read_write(cli: &Cli) {
    let bam_path = cli.in_path.clone();
    let in_threads = cli.in_threads;
    let all_records = bam_reader_to_vec(bam_path, in_threads, None);

    let bam_path = cli.out_path.clone();
    let out_threads = cli.writer_threads;
    bam_writer_from_vec(bam_path, all_records, Some(out_threads));
}

fn b_read_write_uncompressed(cli: &Cli) {
    let bam_path = cli.in_path.clone();
    let in_threads = cli.in_threads;
    let all_records = bam_reader_to_vec(bam_path, in_threads, None);

    let bam_path = cli.out_path.clone();
    let out_threads = cli.writer_threads;
    bam_writer_from_vec_uncompressed(bam_path, all_records, Some(out_threads));
}

fn g2g(cli: &Cli) {
    std::thread::scope(|scope| {
        let (record_sender, record_recv) = crossbeam::channel::bounded(1000);
        scope.spawn({
            let in_path = cli.in_path.clone();
            let in_threads = cli.in_threads;
            move || {
                let mut reader =
                    v2::RffReader::new_reader(in_path, NonZero::new(in_threads).unwrap());
                while let Some(v) = reader.read_serialized_data() {
                    record_sender.send(v).unwrap();
                }
            }
        });

        scope.spawn({
            let out_path = cli.out_path.clone();
            let out_threads = cli.writer_threads;
            move || {
                let pb = get_spin_pb(format!("writing {}", out_path), DEFAULT_INTERVAL);
                let mut writer =
                    v2::RffWriter::new_writer(out_path, NonZero::new(out_threads).unwrap());
                for v in record_recv {
                    let _ = writer.write_serialized_data(&v);
                    pb.inc(1);
                }
                pb.finish();
            }
        });
    });
}

fn gread(cli: &Cli) -> Vec<Vec<u8>> {
    let in_path = cli.in_path.clone();
    let in_threads = cli.in_threads;
    let mut reader = v2::RffReader::new_reader(in_path, NonZero::new(in_threads).unwrap());
    let mut bytes = 0;
    let pb = get_spin_pb(format!("reading {}", cli.in_path), DEFAULT_INTERVAL);

    let instant = Instant::now();
    let mut all_data: Vec<Vec<u8>> = vec![];
    // let mut all_data = vec![];
    while let Some(v) = reader.read_serialized_data() {
        bytes += v.len();
        // TODO：这行代码会明显的影响执行速度。 可能是 堆内存分配的问题。后续会尝试 预分配 一个 大空间，是否这问题依旧存在
        all_data.push(v);
        pb.inc(1);
    }
    println!("bytes:{}", bytes);
    pb.finish();
    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = bytes as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("Read. MB per second: {:.2}MB/s", mb_per_sec);
    all_data
}

fn gread_and_drop(cli: &Cli) {
    let in_path = cli.in_path.clone();
    let in_threads = cli.in_threads;
    let mut reader = v2::RffReader::new_reader(in_path, NonZero::new(in_threads).unwrap());
    let mut bytes = 0;
    let pb = get_spin_pb(format!("reading {}", cli.in_path), DEFAULT_INTERVAL);

    let instant = Instant::now();
    while let Some(v) = reader.read_serialized_data() {
        bytes += v.len();
        pb.inc(1);
    }
    println!("bytes:{}", bytes);
    pb.finish();
    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = bytes as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("Read. MB per second: {:.2}MB/s", mb_per_sec);
}

fn gread2big_buf(cli: &Cli) -> Vec<u8> {
    let in_path = cli.in_path.clone();
    let in_threads = cli.in_threads;
    let mut reader = v2::RffReader::new_reader(in_path, NonZero::new(in_threads).unwrap());
    let mut bytes = 0;

    let mut all_data = vec![0_u8; 30 * 1024 * 1024 * 1024];

    let pb = get_spin_pb(format!("reading {}", cli.in_path), DEFAULT_INTERVAL);
    let instant = Instant::now();
    // let mut all_data = vec![];
    while let Some(v) = reader.read_serialized_data_to_buf(&mut all_data[bytes..]) {
        bytes += v;
        // TODO：这行代码会明显的影响执行速度。 可能是 堆内存分配的问题。后续会尝试 预分配 一个 大空间，是否这问题依旧存在
        pb.inc(1);
    }
    println!("bytes:{}", bytes);
    pb.finish();
    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = bytes as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("Read. MB per second: {:.2}MB/s", mb_per_sec);
    all_data
}

pub fn gwrite(cli: &Cli, data: Vec<Vec<u8>>) {
    let pb = get_spin_pb(format!("writing {}", cli.out_path), DEFAULT_INTERVAL);
    let mut writer =
        v2::RffWriter::new_writer(&cli.out_path, NonZero::new(cli.writer_threads).unwrap());
    let mut bytes = 0;
    let instant = Instant::now();

    for v in data {
        bytes += v.len();
        let _ = writer.write_serialized_data(&v);
        pb.inc(1);
    }
    drop(writer);
    pb.finish();
    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = bytes as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("Write. MB per second: {:.2}MB/s", mb_per_sec);
}

fn g_read_write(cli: &Cli) {
    let all_data = gread(cli);
    gwrite(cli, all_data);
}

fn main() {
    let cli = Cli::parse();
    match cli.mode.as_ref() {
        "b2g" => b2g(&cli),
        "b2g2" => b2g2(&cli),
        "b2g2-codec" => b2g2_with_codec(&cli),
        "b2g2-codec-lz4" => b2g2_with_codec_lz4(&cli),
        "g2b" => g2b(&cli),
        "g2b2" => g2b2(&cli),
        "g2b2-codec" => g2b2_with_codec(&cli),

        "b2b" => b2b(&cli),
        "g2g" => g2g(&cli),
        "gread" => {
            gread(&cli);
        }
        "gread2big-buf" => {
            gread2big_buf(&cli);
        }
        "gread-and-drop" => gread_and_drop(&cli),
        "g-read-write" => g_read_write(&cli),
        "b-read-write" => b_read_write(&cli),
        "b-read-write-uncompress" => b_read_write_uncompressed(&cli),
        mode => panic!("invalid mode. {}. only b2g/g2b are valid", mode),
    };
}
