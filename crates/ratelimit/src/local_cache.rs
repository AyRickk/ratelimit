//! Keys known to be over their limit, so requests for them skip the backend until the entry
//! expires (`LOCAL_CACHE_SIZE_IN_BYTES`).
//!
//! A port of the parts of `github.com/coocood/freecache` v1.2.4 that upstream uses — `Set` with
//! an empty value and `Get` — down to the byte layout of its ring buffers, so that which keys are
//! evicted when the cache is full matches upstream exactly. Times are Unix seconds from the
//! request's clock (freecache's timer).

use std::sync::Mutex;
use xxhash_rust::xxh64::xxh64;

/// freecache's smallest cache.
const MIN_BYTES: usize = 512 * 1024;
const SEGMENTS: usize = 256;
const ENTRY_HEADER_BYTES: usize = 24;
const MAX_KEY_BYTES: usize = 65535;

pub struct LocalCache {
    segments: Vec<Mutex<Segment>>,
}

impl LocalCache {
    pub fn new(capacity_bytes: usize) -> Self {
        let segment_bytes = capacity_bytes.max(MIN_BYTES) / SEGMENTS;
        Self {
            segments: (0..SEGMENTS)
                .map(|_| Mutex::new(Segment::new(segment_bytes)))
                .collect(),
        }
    }

    /// freecache's `Get`: whether `key` is cached and not expired at `now`. A hit refreshes
    /// the entry's access time; an expired entry is removed.
    pub fn contains(&self, key: &str, now: i64) -> bool {
        let hash = xxh64(key.as_bytes(), 0);
        self.segment(hash).locate(key.as_bytes(), hash, now as u32)
    }

    /// freecache's `Set` with an empty value, expiring `ttl_seconds` after `now`. Keys too long
    /// for freecache are not cached, as upstream (which ignores the error).
    pub fn insert(&self, key: &str, ttl_seconds: i64, now: i64) {
        let hash = xxh64(key.as_bytes(), 0);
        self.segment(hash)
            .set(key.as_bytes(), hash, ttl_seconds, now as u32);
    }

    fn segment(&self, hash: u64) -> std::sync::MutexGuard<'_, Segment> {
        self.segments[(hash & 255) as usize].lock().unwrap()
    }
}

/// freecache's `entryHdr`, 24 bytes, little-endian like the Go struct in memory.
#[derive(Debug, Clone, Copy, Default)]
struct EntryHeader {
    access_time: u32,
    expire_at: u32,
    key_len: u16,
    hash16: u16,
    val_len: u32,
    val_cap: u32,
    deleted: bool,
    slot_id: u8,
}

impl EntryHeader {
    fn encode(&self) -> [u8; ENTRY_HEADER_BYTES] {
        let mut b = [0u8; ENTRY_HEADER_BYTES];
        b[0..4].copy_from_slice(&self.access_time.to_le_bytes());
        b[4..8].copy_from_slice(&self.expire_at.to_le_bytes());
        b[8..10].copy_from_slice(&self.key_len.to_le_bytes());
        b[10..12].copy_from_slice(&self.hash16.to_le_bytes());
        b[12..16].copy_from_slice(&self.val_len.to_le_bytes());
        b[16..20].copy_from_slice(&self.val_cap.to_le_bytes());
        b[20] = self.deleted as u8;
        b[21] = self.slot_id;
        b
    }

    fn decode(b: &[u8; ENTRY_HEADER_BYTES]) -> Self {
        Self {
            access_time: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            expire_at: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            key_len: u16::from_le_bytes(b[8..10].try_into().unwrap()),
            hash16: u16::from_le_bytes(b[10..12].try_into().unwrap()),
            val_len: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            val_cap: u32::from_le_bytes(b[16..20].try_into().unwrap()),
            deleted: b[20] != 0,
            slot_id: b[21],
        }
    }

    fn entry_len(&self) -> i64 {
        ENTRY_HEADER_BYTES as i64 + i64::from(self.key_len) + i64::from(self.val_cap)
    }
}

fn is_expired(expire_at: u32, now: u32) -> bool {
    expire_at != 0 && expire_at <= now
}

/// freecache's `entryPtr`.
#[derive(Debug, Clone, Copy, Default)]
struct EntryPtr {
    offset: i64,
    hash16: u16,
    key_len: u16,
}

/// freecache's `segment`.
struct Segment {
    rb: RingBuf,
    total_count: i64,
    total_time: i64,
    vacuum_len: i64,
    slot_lens: [i32; 256],
    slot_cap: i32,
    slots_data: Vec<EntryPtr>,
}

impl Segment {
    fn new(size: usize) -> Self {
        Self {
            rb: RingBuf::new(size),
            total_count: 0,
            total_time: 0,
            vacuum_len: size as i64,
            slot_lens: [0; 256],
            slot_cap: 1,
            slots_data: vec![EntryPtr::default(); 256],
        }
    }

    fn slot_range(&self, slot_id: u8) -> std::ops::Range<usize> {
        let start = (i32::from(slot_id) * self.slot_cap) as usize;
        start..start + self.slot_lens[slot_id as usize] as usize
    }

    fn read_header(&self, offset: i64) -> EntryHeader {
        let mut buf = [0u8; ENTRY_HEADER_BYTES];
        self.rb.read_at(&mut buf, offset);
        EntryHeader::decode(&buf)
    }

    /// `seg.set` with an empty value.
    fn set(&mut self, key: &[u8], hash: u64, expire_seconds: i64, now: u32) {
        if key.len() > MAX_KEY_BYTES {
            return;
        }
        let max_key_val_len = self.rb.data.len() as i64 / 4 - ENTRY_HEADER_BYTES as i64;
        if key.len() as i64 > max_key_val_len {
            return;
        }
        let expire_at = if expire_seconds > 0 {
            now.wrapping_add(expire_seconds as u32)
        } else {
            0
        };
        let slot_id = (hash >> 8) as u8;
        let hash16 = (hash >> 16) as u16;
        let (mut idx, matched) = self.lookup(slot_id, hash16, key);

        let mut hdr;
        if matched {
            let ptr = self.slots_data[self.slot_range(slot_id).start + idx];
            hdr = self.read_header(ptr.offset);
            hdr.slot_id = slot_id;
            hdr.hash16 = hash16;
            hdr.key_len = key.len() as u16;
            let origin_access_time = hdr.access_time;
            hdr.access_time = now;
            hdr.expire_at = expire_at;
            hdr.val_len = 0;
            // An empty value always fits: overwrite in place.
            self.total_time += i64::from(hdr.access_time) - i64::from(origin_access_time);
            self.rb.write_at(&hdr.encode(), ptr.offset);
            return;
        }
        hdr = EntryHeader {
            access_time: now,
            expire_at,
            key_len: key.len() as u16,
            hash16,
            val_len: 0,
            // freecache avoids a zero capacity.
            val_cap: 1,
            deleted: false,
            slot_id,
        };

        let entry_len = hdr.entry_len();
        if self.evacuate(entry_len, slot_id, now) {
            idx = self.lookup(slot_id, hash16, key).0;
        }
        let new_off = self.rb.end;
        self.insert_entry_ptr(slot_id, hash16, new_off, idx, hdr.key_len);
        self.rb.write(&hdr.encode());
        self.rb.write(key);
        self.rb.skip(i64::from(hdr.val_cap - hdr.val_len));
        self.total_time += i64::from(now);
        self.total_count += 1;
        self.vacuum_len -= entry_len;
    }

    /// `seg.evacuate`: frees `entry_len` bytes at the head of the ring buffer, dropping deleted,
    /// expired and least recently used entries, and moving the others to the tail (a second
    /// chance; after five in a row the next one is dropped). Returns whether `slot_id` changed.
    fn evacuate(&mut self, entry_len: i64, slot_id: u8, now: u32) -> bool {
        let mut slot_modified = false;
        let mut consecutive_evacuate = 0;
        while self.vacuum_len < entry_len {
            let old_off = self.rb.end + self.vacuum_len - self.rb.size();
            let old_hdr = self.read_header(old_off);
            let old_entry_len = old_hdr.entry_len();
            if old_hdr.deleted {
                consecutive_evacuate = 0;
                self.total_time -= i64::from(old_hdr.access_time);
                self.total_count -= 1;
                self.vacuum_len += old_entry_len;
                continue;
            }
            let expired = is_expired(old_hdr.expire_at, now);
            let least_recent_used =
                i64::from(old_hdr.access_time) * self.total_count <= self.total_time;
            if expired || least_recent_used || consecutive_evacuate > 5 {
                self.del_entry_ptr_by_offset(old_hdr.slot_id, old_hdr.hash16, old_off);
                if old_hdr.slot_id == slot_id {
                    slot_modified = true;
                }
                consecutive_evacuate = 0;
                self.total_time -= i64::from(old_hdr.access_time);
                self.total_count -= 1;
                self.vacuum_len += old_entry_len;
            } else {
                let new_off = self.rb.evacuate(old_off, old_entry_len as usize);
                self.update_entry_ptr(old_hdr.slot_id, old_hdr.hash16, old_off, new_off);
                consecutive_evacuate += 1;
            }
        }
        slot_modified
    }

    /// `seg.locate` without peeking: found and not expired. Refreshes the access time, and
    /// removes an expired entry.
    fn locate(&mut self, key: &[u8], hash: u64, now: u32) -> bool {
        let slot_id = (hash >> 8) as u8;
        let hash16 = (hash >> 16) as u16;
        let (idx, matched) = self.lookup(slot_id, hash16, key);
        if !matched {
            return false;
        }
        let ptr = self.slots_data[self.slot_range(slot_id).start + idx];
        let mut hdr = self.read_header(ptr.offset);
        if is_expired(hdr.expire_at, now) {
            self.del_entry_ptr(slot_id, idx);
            return false;
        }
        self.total_time += i64::from(now.wrapping_sub(hdr.access_time));
        hdr.access_time = now;
        self.rb.write_at(&hdr.encode(), ptr.offset);
        true
    }

    fn expand(&mut self) {
        let old_cap = self.slot_cap as usize;
        let mut data = vec![EntryPtr::default(); old_cap * 2 * 256];
        for i in 0..256 {
            let off = i * old_cap;
            let len = self.slot_lens[i] as usize;
            data[off * 2..off * 2 + len].copy_from_slice(&self.slots_data[off..off + len]);
        }
        self.slot_cap *= 2;
        self.slots_data = data;
    }

    fn update_entry_ptr(&mut self, slot_id: u8, hash16: u16, old_off: i64, new_off: i64) {
        let (idx, matched) = self.lookup_by_offset(slot_id, hash16, old_off);
        if matched {
            let at = self.slot_range(slot_id).start + idx;
            self.slots_data[at].offset = new_off;
        }
    }

    fn insert_entry_ptr(
        &mut self,
        slot_id: u8,
        hash16: u16,
        offset: i64,
        idx: usize,
        key_len: u16,
    ) {
        if self.slot_lens[slot_id as usize] == self.slot_cap {
            self.expand();
        }
        self.slot_lens[slot_id as usize] += 1;
        let range = self.slot_range(slot_id);
        let slot = &mut self.slots_data[range];
        slot.copy_within(idx..slot.len() - 1, idx + 1);
        slot[idx] = EntryPtr {
            offset,
            hash16,
            key_len,
        };
    }

    fn del_entry_ptr_by_offset(&mut self, slot_id: u8, hash16: u16, offset: i64) {
        let (idx, matched) = self.lookup_by_offset(slot_id, hash16, offset);
        if matched {
            self.del_entry_ptr(slot_id, idx);
        }
    }

    fn del_entry_ptr(&mut self, slot_id: u8, idx: usize) {
        let range = self.slot_range(slot_id);
        let offset = self.slots_data[range.start + idx].offset;
        let mut hdr = self.read_header(offset);
        hdr.deleted = true;
        self.rb.write_at(&hdr.encode(), offset);
        let slot = &mut self.slots_data[range];
        slot.copy_within(idx + 1.., idx);
        self.slot_lens[slot_id as usize] -= 1;
    }

    /// `entryPtrIdx`: the first entry of the slot whose hash16 is not below `hash16`.
    fn ptr_idx(&self, slot_id: u8, hash16: u16) -> usize {
        let slot = &self.slots_data[self.slot_range(slot_id)];
        slot.partition_point(|ptr| ptr.hash16 < hash16)
    }

    fn lookup(&self, slot_id: u8, hash16: u16, key: &[u8]) -> (usize, bool) {
        let range = self.slot_range(slot_id);
        let mut idx = self.ptr_idx(slot_id, hash16);
        while idx < range.len() {
            let ptr = self.slots_data[range.start + idx];
            if ptr.hash16 != hash16 {
                break;
            }
            if usize::from(ptr.key_len) == key.len()
                && self
                    .rb
                    .equal_at(key, ptr.offset + ENTRY_HEADER_BYTES as i64)
            {
                return (idx, true);
            }
            idx += 1;
        }
        (idx, false)
    }

    fn lookup_by_offset(&self, slot_id: u8, hash16: u16, offset: i64) -> (usize, bool) {
        let range = self.slot_range(slot_id);
        let mut idx = self.ptr_idx(slot_id, hash16);
        while idx < range.len() {
            let ptr = self.slots_data[range.start + idx];
            if ptr.hash16 != hash16 {
                break;
            }
            if ptr.offset == offset {
                return (idx, true);
            }
            idx += 1;
        }
        (idx, false)
    }
}

/// freecache's `RingBuf`: a byte stream whose offsets keep growing, stored in a fixed buffer.
struct RingBuf {
    begin: i64,
    end: i64,
    data: Vec<u8>,
    index: usize,
}

impl RingBuf {
    fn new(size: usize) -> Self {
        Self {
            begin: 0,
            end: 0,
            data: vec![0; size],
            index: 0,
        }
    }

    fn size(&self) -> i64 {
        self.data.len() as i64
    }

    fn data_off(&self, off: i64) -> usize {
        let len = self.data.len();
        let mut data_off = if self.end - self.begin < len as i64 {
            (off - self.begin) as usize
        } else {
            self.index + (off - self.begin) as usize
        };
        if data_off >= len {
            data_off -= len;
        }
        data_off
    }

    fn read_at(&self, p: &mut [u8], off: i64) {
        if off > self.end || off < self.begin {
            return;
        }
        let len = self.data.len();
        let read_off = self.data_off(off);
        let read_end = read_off + (self.end - off) as usize;
        if read_end <= len {
            let n = p.len().min(read_end - read_off);
            p[..n].copy_from_slice(&self.data[read_off..read_off + n]);
        } else {
            let n = p.len().min(len - read_off);
            p[..n].copy_from_slice(&self.data[read_off..read_off + n]);
            if n < p.len() {
                let m = (p.len() - n).min(read_end - len);
                p[n..n + m].copy_from_slice(&self.data[..m]);
            }
        }
    }

    fn write(&mut self, p: &[u8]) {
        let len = self.data.len();
        if p.len() > len {
            return;
        }
        let mut n = 0;
        while n < p.len() {
            let written = (len - self.index).min(p.len() - n);
            self.data[self.index..self.index + written].copy_from_slice(&p[n..n + written]);
            self.end += written as i64;
            n += written;
            self.index += written;
            if self.index >= len {
                self.index -= len;
            }
        }
        if self.end - self.begin > len as i64 {
            self.begin = self.end - len as i64;
        }
    }

    fn write_at(&mut self, p: &[u8], off: i64) {
        if off + p.len() as i64 > self.end || off < self.begin {
            return;
        }
        let len = self.data.len();
        let write_off = self.data_off(off);
        let write_end = write_off + (self.end - off) as usize;
        if write_end <= len {
            let n = p.len().min(write_end - write_off);
            self.data[write_off..write_off + n].copy_from_slice(&p[..n]);
        } else {
            let n = p.len().min(len - write_off);
            self.data[write_off..write_off + n].copy_from_slice(&p[..n]);
            if n < p.len() {
                let m = (p.len() - n).min(write_end - len);
                self.data[..m].copy_from_slice(&p[n..n + m]);
            }
        }
    }

    fn equal_at(&self, p: &[u8], off: i64) -> bool {
        if off + p.len() as i64 > self.end || off < self.begin {
            return false;
        }
        let len = self.data.len();
        let read_off = self.data_off(off);
        let read_end = read_off + p.len();
        if read_end <= len {
            p == &self.data[read_off..read_end]
        } else {
            let first = len - read_off;
            p[..first] == self.data[read_off..] && p[first..] == self.data[..p.len() - first]
        }
    }

    /// Moves `length` bytes at `off` to the end of the stream; returns their new offset.
    fn evacuate(&mut self, off: i64, length: usize) -> i64 {
        if off + length as i64 > self.end || off < self.begin {
            return -1;
        }
        let len = self.data.len();
        let read_off = self.data_off(off);
        if read_off == self.index {
            self.index += length;
            if self.index >= len {
                self.index -= len;
            }
        } else if read_off < self.index {
            let n = (len - self.index).min(length);
            self.data.copy_within(read_off..read_off + n, self.index);
            self.index += n;
            if self.index == len {
                let rest = length - n;
                self.data.copy_within(read_off + n..read_off + n + rest, 0);
                self.index = rest;
            }
        } else {
            let read_end = read_off + length;
            if read_end <= len {
                let n = (len - self.index).min(length);
                self.data.copy_within(read_off..read_off + n, self.index);
                self.index += n;
            } else {
                let first = len - read_off;
                let n = (len - self.index).min(first);
                self.data.copy_within(read_off..read_off + n, self.index);
                self.index += n;
                let tail = length - n;
                let m = (len - self.index).min(tail);
                self.data.copy_within(0..m, self.index);
                self.index += m;
                if self.index == len {
                    self.data.copy_within(m..tail, 0);
                    self.index = tail - m;
                }
            }
        }
        let new_off = self.end;
        self.end += length as i64;
        if self.begin < self.end - len as i64 {
            self.begin = self.end - len as i64;
        }
        new_off
    }

    fn skip(&mut self, length: i64) {
        let len = self.data.len();
        self.end += length;
        self.index += length as usize;
        while self.index >= len {
            self.index -= len;
        }
        if self.end - self.begin > len as i64 {
            self.begin = self.end - len as i64;
        }
    }
}
