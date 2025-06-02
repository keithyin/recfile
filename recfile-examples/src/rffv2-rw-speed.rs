use std::{
    num::NonZero,
    path::{self, PathBuf},
};

use clap::Parser;
use recfile::io::v2;
use gskits::pbar::{DEFAULT_INTERVAL, get_bar_pb, get_spin_pb};
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long = "in", help = "input rff file")]
    pub in_path: Option<String>,

    #[arg(long = "out", help = "output rff file")]
    pub out_path: Option<String>,

    #[arg(long = "io-depth", default_value_t = 4)]
    pub io_depth: usize,

    #[arg(long = "chunk-size", help = "write chunk size")]
    pub chunk_size: Option<usize>,

    #[arg(long = "rep-times", help = "write total times")]
    pub rep_times: Option<usize>,
}

impl Cli {
    fn get_out_path(&self) -> PathBuf {
        path::Path::new(self.out_path.as_ref().unwrap()).into()
    }

    fn get_in_path(&self) -> PathBuf {
        path::Path::new(self.in_path.as_ref().unwrap()).into()
    }
}

fn write(cli: &Cli) {
    let mut writer =
        v2::RffWriter::new_writer(cli.get_out_path(), NonZero::new(cli.io_depth).unwrap());
    let data = vec!['A' as u8; cli.chunk_size.unwrap_or(1024 * 1024)];
    let tot_size = data.len() * cli.rep_times.expect("rep times must be set");
    let pb = get_bar_pb(
        "writing".to_string(),
        DEFAULT_INTERVAL,
        cli.rep_times.unwrap() as u64,
    );
    let instant = std::time::Instant::now();
    (0..cli.rep_times.unwrap()).into_iter().for_each(|_| {
        pb.inc(1);
        writer.write_serialized_data(&data).unwrap();
    });
    pb.finish();

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = tot_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec);
}

fn read(cli: &Cli) {
    let mut reader =
        v2::RffReader::new_reader(cli.get_in_path(), NonZero::new(cli.io_depth).unwrap());
    let pb = get_spin_pb("reading".to_string(), DEFAULT_INTERVAL);
    let mut tot_size = 0;
    let instant = std::time::Instant::now();
    while let Some(data) = reader.read_serialized_data() {
        pb.inc(1);
        tot_size += data.len();
    }
    pb.finish();

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = tot_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec);
}

/// ./target/release/rffv2-rw-speed --out rffv2-speed.rff --in rffv2-speed.rff --chunk-size 1048576 --rep-times 20480
/// read 2.3G, write 2.3G. chunk-size 1M
fn main() {
    let cli = Cli::parse();
    if cli.in_path.is_some() {
        write(&cli);
    }
    if cli.out_path.is_some() {
        read(&cli);
    }
}
