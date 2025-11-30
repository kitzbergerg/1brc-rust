use std::simd::{Mask, Simd, cmp::SimdPartialEq};

use crate::hash::MyHashMap;

pub struct Weather {
    pub total: u32,
    pub min: i16,
    pub max: i16,
    pub sum: i64,
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

const CHUNK_SIZE: usize = 64;

const SIMD_NEWLINE: Simd<u8, CHUNK_SIZE> = Simd::splat(b'\n');
const SIMD_DELIM: Simd<u8, CHUNK_SIZE> = Simd::splat(b';');

pub fn parse<'a>(data: &'a [u8]) -> MyHashMap<&'a [u8], Weather> {
    let mut prev = 0;
    let mut pos = 0;
    let (chunks, remainder) = data.as_chunks();

    let mut iter = chunks
        .iter()
        .map(|chunk| Simd::from_array(*chunk))
        .chain(std::iter::once(Simd::load_or_default(remainder)))
        .map(|chunk| chunk.simd_eq(SIMD_NEWLINE) | chunk.simd_eq(SIMD_DELIM))
        .map(Mask::to_bitmask);

    let mut map = MyHashMap::<&'a [u8], Weather>::default();
    let mut buf = [&[][..]; 128];
    let mut count = 0;
    'outer: loop {
        // fill buffer
        'inner: while count < 64 + 32 {
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

#[cfg(not(target_feature = "avx512vbmi2"))]
#[inline(always)]
fn extract_fields<'a>(
    data: &'a [u8],
    prev: &mut usize,
    pos: usize,
    mut combined: u64,
    buf: &mut [&'a [u8]; 128],
    count: &mut usize,
) {
    for _ in 0..combined.count_ones() {
        let i = combined.trailing_zeros() as usize;
        let current = pos + i;
        unsafe { *buf.get_unchecked_mut(*count) = data.get_unchecked(*prev..current) };
        *prev = current + 1;
        combined &= combined - 1;
        *count += 1;
    }
}

#[cfg(target_feature = "avx512vbmi2")]
#[inline(always)]
fn extract_fields<'a>(
    data: &'a [u8],
    prev: &mut usize,
    pos: usize,
    combined: u64,
    buf: &mut [&'a [u8]; 128],
    count: &mut usize,
) {
    // write a simd array like [0, 1, 2, 3, ...]
    const RANGE: Simd<u8, CHUNK_SIZE> = {
        let mut tmp = [0u8; CHUNK_SIZE];
        let mut i = 0u8;
        while i < CHUNK_SIZE as u8 {
            tmp[i as usize] = i;
            i += 1;
        }
        Simd::from_array(tmp)
    };

    // compress matches into the first slots: "a;b\n" -> [1, 3, 0, ...]
    let offsets: Simd<u8, CHUNK_SIZE> =
        unsafe { std::arch::x86_64::_mm512_maskz_compress_epi8(combined, RANGE.into()) }.into();
    for i in 0..combined.count_ones() {
        let offset = offsets[i as usize] as usize;
        let current = pos + offset;
        unsafe { *buf.get_unchecked_mut(*count) = data.get_unchecked(*prev..current) };
        *prev = current + 1;
        *count += 1;
    }
}
