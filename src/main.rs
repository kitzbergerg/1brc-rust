#![feature(portable_simd)]
#![feature(iter_array_chunks)]
#![feature(rustc_private)]

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::Write,
    os::fd::AsRawFd,
    simd::{Mask, Simd, cmp::SimdPartialEq},
};

#[derive(Default)]
struct Weather {
    total: usize,
    min: f64,
    max: f64,
    sum: f64,
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

fn parse<'a>(data: &'a [u8]) -> HashMap<&'a [u8], Weather> {
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
                std::str::from_utf8(measurement)
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            )
        })
        .fold(
            HashMap::<&'a [u8], Weather>::new(),
            |mut acc, (station, measurement)| {
                let entry = acc.entry(station).or_default();
                entry.total += 1;
                entry.min = entry.min.min(measurement);
                entry.max = entry.max.max(measurement);
                entry.sum += measurement;
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

fn main() {
    let mut map = parse(open_reader("data/measurements.txt"))
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .peekable();

    let mut stdout = std::io::stdout().lock();
    stdout.write_all(b"{").unwrap();
    while let Some((id, stats)) = map.next() {
        stdout.write_all(id).unwrap();
        write!(
            stdout,
            "={:.1}/{:.1}/{:.1}",
            stats.min,
            stats.sum / stats.total as f64,
            stats.max,
        )
        .unwrap();
        if map.peek().is_some() {
            stdout.write_all(b", ").unwrap();
        }
    }
    stdout.write_all(b"}\n").unwrap();
}
