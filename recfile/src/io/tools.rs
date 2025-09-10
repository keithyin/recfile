use crate::util::aligned_alloc;

pub fn encode_to_aligned_vec<Enc>(v: &Enc, page_size: usize) -> Vec<u8>
where
    Enc: bincode::Encode,
{
    let enc = bincode::encode_to_vec(v, bincode::config::standard()).unwrap();
    // let mut len = [0_u8; 4];
    assert!((enc.len() + 4) <= page_size);
    let mut aligned_vec = aligned_alloc(page_size, page_size);
    let len = (enc.len() as u32).to_le_bytes();
    aligned_vec[0..4].copy_from_slice(&len);

    aligned_vec[4..(enc.len() + 4)].copy_from_slice(&enc);
    aligned_vec
}
