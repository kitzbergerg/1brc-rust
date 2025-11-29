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

struct Weather {
    total: u32,
    min: i16,
    max: i16,
    sum: i64,
}

#[inline(always)]
fn parse_temp(t: &[u8]) -> i16 {
    let t_len = t.len();
    unsafe { std::hint::assert_unchecked(t_len >= 3) };
    let is_neg = std::hint::select_unpredictable(t[0] == b'-', true, false);
    let sign = i16::from(!is_neg) * 2 - 1;
    let skip = usize::from(is_neg);
    let has_dd = std::hint::select_unpredictable(t_len - skip == 4, true, false);
    let mul = i16::from(has_dd) * 90 + 10;
    let t1 = mul * i16::from(t[skip] - b'0');
    let t2 = i16::from(has_dd) * 10 * i16::from(t[t_len - 3] - b'0');
    let t3 = i16::from(t[t_len - 1] - b'0');
    sign * (t1 + t2 + t3)
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

    let mut iter = chunks
        .iter()
        .map(|chunk| Simd::from_array(*chunk))
        .chain([Simd::load_or_default(remainder)])
        .map(|chunk| chunk.simd_eq(SIMD_NEWLINE) | chunk.simd_eq(SIMD_DELIM))
        .map(Mask::to_bitmask);

    let mut map = fxhash::FxHashMap::<&'a [u8], Weather>::default();
    let mut buf = [&[][..]; 64];
    let mut count = 0;
    'outer: loop {
        // fill buffer
        'inner: while count < 32 {
            if let Some(mask) = iter.next() {
                extract_fields(data, &mut prev, pos, mask, &mut buf, &mut count);
                pos += CHUNK_SIZE;
            } else if count == 0 {
                break 'outer;
            } else {
                break 'inner;
            }
        }

        // empty buffer
        for i in 0..count / 2 {
            let station = unsafe { buf.get_unchecked(2 * i) };
            let measurement = parse_temp(unsafe { buf.get_unchecked(2 * i + 1) });
            map.entry(station)
                .and_modify(|entry| {
                    entry.total += 1;
                    entry.min = entry.min.min(measurement);
                    entry.max = entry.max.max(measurement);
                    entry.sum += measurement as i64;
                })
                .or_insert(Weather {
                    total: 1,
                    min: measurement,
                    max: measurement,
                    sum: measurement as i64,
                });
        }

        // handle possible remainder
        let is_odd = count & 1;
        buf[0] = buf[count - is_odd];
        count = is_odd;
    }
    map
}

#[inline(always)]
fn extract_fields<'a>(
    data: &'a [u8],
    prev: &mut usize,
    pos: usize,
    mut combined: u64,
    v: &mut [&'a [u8]; 64],
    count: &mut usize,
) {
    while combined != 0 {
        let i = combined.trailing_zeros() as usize;
        let current = pos + i;
        unsafe { *v.get_unchecked_mut(*count) = data.get_unchecked(*prev..current) };
        *prev = current + 1;
        combined &= combined - 1;
        *count += 1;
    }
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
    let mut map = get_map(bytes).into_iter().peekable();

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
