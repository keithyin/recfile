use bincode::config::Configuration;

pub mod v1;
pub mod v2;
pub mod header;

pub fn get_bincode_cfg() -> Configuration {
    bincode::config::standard()
        .with_little_endian()
        .with_variable_int_encoding()
}


