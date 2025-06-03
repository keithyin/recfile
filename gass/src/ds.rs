use std::{collections::HashSet, ops::{Deref, DerefMut}};

use gskits::gsbam::bam_record_ext::BamRecordExt;
use rust_htslib::bam::{record::Aux, Record};


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