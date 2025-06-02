use std::{
    io::{Read, Seek},
    path::{self, PathBuf},
};

use clap::Parser;
use gskits::pbar::{DEFAULT_INTERVAL, get_spin_pb};
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long = "in", help = "input rff file")]
    pub in_path: Option<String>,

    #[arg(long = "io-depth", default_value_t = 4)]
    pub io_depth: usize,

    #[arg(long = "chunk-size", help = "write chunk size")]
    pub chunk_size: Option<usize>,

    #[arg(long = "rep-times", help = "write total times")]
    pub rep_times: Option<usize>,
}

impl Cli {
    fn get_in_path(&self) -> PathBuf {
        path::Path::new(self.in_path.as_ref().unwrap()).into()
    }
}

fn read(cli: &Cli) {
    let mut file = std::fs::File::open(cli.get_in_path()).expect("Failed to open input file");
    file.seek(std::io::SeekFrom::Start(4 * 1024)).unwrap();
    let instant = std::time::Instant::now();

    let pb = get_spin_pb("reading".to_string(), DEFAULT_INTERVAL);
    let mut tot_size = 0;
    loop {
        let mut buf = [0u8; 8];
        let data_len = match file.read_exact(&mut buf) {
            Ok(_) => {
                let size = usize::from_le_bytes(buf);
                size
            }
            Err(e) => {
                eprintln!("Error reading file: {}", e);
                0
            }
        };

        if data_len == 0 {
            break;
        }
        let mut data = vec![0u8; data_len];
        if let Err(e) = file.read_exact(&mut data) {
            eprintln!("Error reading data: {}", e);
            break;
        }
        tot_size += data_len + 8; // 8 bytes for the length prefix
        pb.inc(1);
    }
    pb.finish();

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = tot_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec);
}

fn main() {
    let cli = Cli::parse();
    read(&cli);
}
