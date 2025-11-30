#![feature(portable_simd)]
#![feature(iter_array_chunks)]
#![feature(rustc_private)]

use std::{collections::BTreeMap, fs::File, io::Write, os::fd::AsRawFd};

use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::parse::{Weather, parse};

mod hash;
mod parse;

fn open_reader(file: &str) -> &[u8] {
    let file = File::open(file).unwrap();
    let len = file.metadata().unwrap().len();
    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len as libc::size_t,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        );
        if ptr == libc::MAP_FAILED {
            panic!("{:?}", std::io::Error::last_os_error());
        }
        if libc::madvise(ptr, len as libc::size_t, libc::MADV_SEQUENTIAL) != 0 {
            panic!("{:?}", std::io::Error::last_os_error());
        }
        std::slice::from_raw_parts(ptr as *const u8, len as usize)
    }
}

#[allow(dead_code)]
fn get_map(bytes: &[u8]) -> BTreeMap<&[u8], Weather> {
    parse(bytes).into_iter().collect::<BTreeMap<_, _>>()
}

#[allow(dead_code)]
fn get_map_par(bytes: &[u8]) -> BTreeMap<&[u8], Weather> {
    let num_threads = rayon::current_num_threads();
    let len = bytes.len();
    let block_size = len / num_threads;

    let mut blocks = vec![Default::default(); num_threads];
    let mut start = 0;
    for (i, block) in blocks.iter_mut().enumerate().take(num_threads - 1) {
        let approx_end = (i + 1) * block_size;
        let search_slice = &bytes[start..approx_end];
        let offset = unsafe {
            let n = libc::memrchr(
                search_slice.as_ptr() as *const libc::c_void,
                b'\n' as libc::c_int,
                search_slice.len(),
            ) as *const u8;
            if n.is_null() {
                panic!("No newline found!")
            } else {
                n.offset_from(search_slice.as_ptr()) as usize + 1
            }
        };

        let end = start + offset;
        *block = &bytes[start..end];
        start = end;
    }
    blocks[num_threads - 1] = &bytes[start..];

    blocks
        .into_par_iter()
        .map(|bytes| parse(bytes))
        .reduce_with(|mut acc, other| {
            for (station, measurement) in other {
                acc.entry(station)
                    .and_modify(|entry| {
                        entry.total += measurement.total;
                        entry.min = entry.min.min(measurement.min);
                        entry.max = entry.max.max(measurement.max);
                        entry.sum += measurement.sum;
                    })
                    .or_insert(measurement);
            }
            acc
        })
        .unwrap()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
}

fn main() {
    let bytes = open_reader("data/measurements.txt");
    let mut map = get_map_par(bytes).into_iter().peekable();

    let mut stdout = std::io::stdout().lock();
    stdout.write_all(b"{").unwrap();
    while let Some((id, stats)) = map.next() {
        stdout.write_all(id).unwrap();
        write!(
            stdout,
            "={:.1}/{:.1}/{:.1}",
            stats.min as f32 * 0.1,
            (stats.sum as f64 / stats.total as f64) * 0.1,
            stats.max as f32 * 0.1,
        )
        .unwrap();
        if map.peek().is_some() {
            stdout.write_all(b", ").unwrap();
        }
    }
    stdout.write_all(b"}\n").unwrap();
}
