#![feature(portable_simd)]
#![feature(iter_array_chunks)]
#![feature(rustc_private)]

use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    os::fd::AsRawFd,
    simd::{Mask, Simd, cmp::SimdPartialEq},
};

use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[derive(Default)]
struct Weather {
    total: usize,
    min: f32,
    max: f32,
    sum: f32,
}

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

const CHUNK_SIZE: usize = 64;

const SIMD_NEWLINE: Simd<u8, CHUNK_SIZE> = Simd::splat(b'\n');
const SIMD_DELIM: Simd<u8, CHUNK_SIZE> = Simd::splat(b';');

fn parse<'a>(data: &'a [u8]) -> fxhash::FxHashMap<&'a [u8], Weather> {
    let mut prev = 0;
    let mut pos = 0;
    let (chunks, remainder) = data.as_chunks();
    chunks
        .iter()
        .map(|chunk| Simd::from_array(*chunk))
        .chain([Simd::load_or_default(remainder)])
        .map(|chunk| chunk.simd_eq(SIMD_NEWLINE) | chunk.simd_eq(SIMD_DELIM))
        .map(Mask::to_bitmask)
        .flat_map(|mask| {
            let v = extract_fields(data, &mut prev, pos, mask);
            pos += CHUNK_SIZE;
            v
        })
        .array_chunks()
        .map(|[station, measurement]| {
            (
                station,
                unsafe { std::str::from_utf8_unchecked(measurement) }
                    .parse::<f32>()
                    .unwrap(),
            )
        })
        .fold(
            fxhash::FxHashMap::<&'a [u8], Weather>::default(),
            |mut acc, (station, measurement)| {
                acc.entry(station)
                    .and_modify(|entry| {
                        entry.total += 1;
                        entry.min = entry.min.min(measurement);
                        entry.max = entry.max.max(measurement);
                        entry.sum += measurement;
                    })
                    .or_insert(Weather {
                        total: 1,
                        min: measurement,
                        max: measurement,
                        sum: measurement,
                    });
                acc
            },
        )
}

#[inline(always)]
fn extract_fields<'a>(
    data: &'a [u8],
    prev: &mut usize,
    pos: usize,
    mut combined: u64,
) -> Vec<&'a [u8]> {
    let mut v = vec![];
    while combined != 0 {
        let i = combined.trailing_zeros() as usize;
        let current = pos + i;
        let field = unsafe { data.get_unchecked(*prev..current) };
        v.push(field);
        *prev = current + 1;
        combined &= combined - 1;
    }
    v
}

#[allow(dead_code)]
fn get_map(bytes: &[u8]) -> BTreeMap<&[u8], Weather> {
    parse(bytes).into_iter().collect::<BTreeMap<_, _>>()
}

#[allow(dead_code)]
fn get_map_rayon(bytes: &[u8]) -> BTreeMap<&[u8], Weather> {
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
                let entry = acc.entry(station).or_default();
                entry.total += measurement.total;
                entry.min = entry.min.min(measurement.min);
                entry.max = entry.max.max(measurement.max);
                entry.sum += measurement.sum;
            }
            acc
        })
        .unwrap()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
}

fn main() {
    let bytes = open_reader("data/measurements.txt");
    let mut map = get_map_rayon(bytes).into_iter().peekable();

    let mut stdout = std::io::stdout().lock();
    stdout.write_all(b"{").unwrap();
    while let Some((id, stats)) = map.next() {
        stdout.write_all(id).unwrap();
        write!(
            stdout,
            "={:.1}/{:.1}/{:.1}",
            stats.min,
            stats.sum / stats.total as f32,
            stats.max,
        )
        .unwrap();
        if map.peek().is_some() {
            stdout.write_all(b", ").unwrap();
        }
    }
    stdout.write_all(b"}\n").unwrap();
}
