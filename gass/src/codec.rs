use crossbeam::channel::{Receiver, Sender};
use zstd;

pub fn zstd_block_compress(input: &[u8]) -> Vec<u8> {
    let compressed = zstd::bulk::compress(input, 6).unwrap();
    let mut final_res = Vec::with_capacity(compressed.len() + 4);
    final_res.extend_from_slice(&(input.len() as u32).to_le_bytes());
    final_res.extend(&compressed);
    final_res
}

pub fn zstd_block_decompress(input: &[u8]) -> Vec<u8> {
    let mut data_len = [0_u8; 4];
    data_len.copy_from_slice(&input[..4]);
    let data_len = u32::from_le_bytes(data_len);
    zstd::bulk::decompress(&input[4..], data_len as usize).unwrap()
}

pub fn zstd_block_compress_worker(recv: Receiver<Vec<u8>>, sender: Sender<Vec<u8>>) {
    for raw_data in recv {
        sender.send(zstd_block_compress(&raw_data)).unwrap();
    }
}

pub fn zstd_block_decompress_worker(recv: Receiver<Vec<u8>>, sender: Sender<Vec<u8>>) {
    for compressed in recv {
        sender.send(zstd_block_decompress(&compressed)).unwrap();
    }
}

pub fn lz4_block_compress(input: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(input)
}

pub fn lz4_block_decompress(input: &[u8]) -> Vec<u8> {
    lz4_flex::decompress_size_prepended(input).unwrap()
}

pub fn lz4_block_compress_worker(recv: Receiver<Vec<u8>>, sender: Sender<Vec<u8>>) {
    for raw_data in recv {
        let compressed = lz4_block_compress(&raw_data);
        sender.send(compressed).unwrap();
    }
}

pub fn lz4_block_decompress_worker(recv: Receiver<Vec<u8>>, sender: Sender<Vec<u8>>) {
    for compressed_data in recv {
        let decompressed = lz4_block_decompress(&compressed_data);
        sender.send(decompressed).unwrap();
    }
}
// pub fn zstd_block_compress(input: &[u8]) -> Vec<u8> {
//     compress(input, 6).unwrap()
// }

// pub fn zstd_block_decompress(input: &[u8]) -> Vec<u8> {
//     decompress(data, capacity)
// }
