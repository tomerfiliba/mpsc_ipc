////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Algorithm:
//
// Setup: the ring buffer is made of `num_entries` (must be a power of 2), each entry being `entry_size` bytes
// (rounded up to 8 bytes). We also hold `num_entries` control words (AtomicU64). Everything is initialized
// to zero.
//
// We hold a header with immutable info (first cache line), followed by two atomic indices (ridx and widx)
// in a separate cache line. Both indices are in the same cache line because both the consumer and the producer
// access them. The indices are monotonically-increasing, being masked by `(num_entries-1)` when accessing
// the arrays.
//
// No-std: the code itself uses std, for things like `anyhow`, but these are all done in the setup phase,
// when opening and mmaping files. The algorithm itself does not require any runtime or OS support like futex.
//
// Producer: when pushing, the producer checks for room (widx<ridx+num_entries), and if there is room,
// it attempts to advance widx by 1, using a CAS. If it fails, it tries again. Upon success, it owns slot the slot,
// and writes the control word using a CAS. The contol word has a pid part that is unique to each producer,
// and the length of the data (which must be <= entry_size, and also fit in 32 bits). This CAS writing may fail --
// which means the consumer has given up on us, in which case some one else might be holding the slot. The
// pid ensures we'd be aware of it and bail out. If we succeed in writing the control word, we proceed to
// writing the entry's data (which may take time), followed by rewriting the control word, this time setting the
// FINISHED bit (only if it matches the expected value).
//
// Consumer: when popping, the consumer checks if there are entries to read (ridx<widx). If there are, it reads
// the control word and checks the finished bit. If it's set, it's safe to read the entry, clear the control word
// and advance ridx. If the finished bit is not set, it means either the producer died in the middle of writing
// the entry, or the consumer has ran into a producer which that's still busy writing the entry. In this case,
// the consumer should stall a little and retry (either using `sched_yield`/`nanosleep` or spinning), after which
// either the entry became finished, or the consumer gives up on this entry by clearing the control word and
// advancing ridx.
//
// The only open issue is a "sleepy producer" that started writing a large entry, hanged until the consumer gave
// up, and then woke up and continued the memcpy. In this case, it will corrupt and entry that's already taken by
// some other producer. To solve that, the stall function takes the producer's pid. It is allowed to wait for
// any duration or time (returning `Retry`), or to skip the entry while leaving it occupied (`Skip`), as well as
// clearing the entry (`Reclaim`). Note that `Reclaim` is only safe to use if the producer is dead (or if you
// can ensure it will never wake up)
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use anyhow::{ensure, Result};
use memmap::{MmapMut, MmapOptions};
use std::sync::atomic::{
    AtomicU64,
    Ordering::{Relaxed, Release, SeqCst},
};
use std::{fs::OpenOptions, path::Path, slice};

const MAGIC: u32 = 0x2d06_9f03;
const VERSION: u32 = 1;
const PID_MASK: u32 = 0x7fff_ffff;

fn align(n: u64, alignment: u64) -> u64 {
    ((n + alignment - 1) / alignment) * alignment
}

#[repr(C, align(128))]
struct RingBufParams {
    magic: u32,
    version: u32,
    entry_size: u64,
    num_entries: u64,
    control_offset: u64,
    entries_offset: u64,
}

#[repr(C, align(128))]
struct RingBufHeader {
    // first cache line
    params: RingBufParams,
    // second cache line
    read_idx: AtomicU64,
    write_idx: AtomicU64,
}

// +----------+-----------+-------------+
// | finished |    pid    |     len     |
// |    (1)   |    (31)   |     (32)    |
// +----------+-----------+-------------+
struct ControlWord(u64);

impl ControlWord {
    fn new(pid: u32, len: u32) -> Self {
        Self(((pid as u64) << 32) | (len as u64))
    }
    fn load(atomic: &AtomicU64) -> Self {
        Self(atomic.load(Relaxed))
    }
    fn claim(&self, atomic: &AtomicU64) -> bool {
        atomic.compare_exchange(0, self.0, SeqCst, Relaxed).is_ok()
    }

    fn len(&self) -> usize {
        (self.0 as u32) as usize
    }
    fn pid(&self) -> u32 {
        ((self.0 >> 32) as u32) & PID_MASK
    }
    fn is_finished(&self) -> bool {
        self.0 >> 63 != 0
    }
    fn mark_finished(&self, atomic: &AtomicU64) -> bool {
        atomic
            .compare_exchange(self.0, (1 << 63) | self.0, SeqCst, Relaxed)
            .is_ok()
    }
}

struct RingBuf {
    ptr: *const u8,
    control_ptr: *const AtomicU64,
    entries_ptr: *const u8,
    num_entries: u64,
    entry_size: u64,
    _mmap: Option<MmapMut>,
}

impl RingBuf {
    #[inline]
    fn header(&self) -> &RingBufHeader {
        unsafe { &*(self.ptr as *const RingBufHeader) }
    }

    #[inline]
    fn control_word(&self, idx: u64) -> &AtomicU64 {
        unsafe {
            &*self
                .control_ptr
                .add((idx & (self.num_entries - 1)) as usize)
        }
    }

    #[inline]
    fn entry(&self, idx: u64) -> &[u8] {
        let mask = self.num_entries - 1;
        unsafe {
            slice::from_raw_parts(
                self.entries_ptr
                    .byte_add(((idx & mask) * self.entry_size) as usize),
                self.entry_size as usize,
            )
        }
    }

    #[inline]
    fn entry_mut(&self, idx: u64) -> &mut [u8] {
        let mask = self.num_entries - 1;
        unsafe {
            slice::from_raw_parts_mut(
                self.entries_ptr
                    .byte_add(((idx & mask) * self.entry_size) as usize) as *mut _,
                self.entry_size as usize,
            )
        }
    }
}

pub struct SingleConsumer {
    ring: RingBuf,
}

pub enum StallResult {
    Retry,                // stall (re-check the status of the entry)
    SkipAndKeepTombstone, // skip this entry, it will be "tombstoned"
    SkipAndReclaim,       // reclaim (clear) this entry (allow it to be reused) --
                          // ONLY DO THIS IF THE PRODUCER IS SURELY DEAD
}

impl SingleConsumer {
    fn _open_or_create(
        path: impl AsRef<Path>,
        entry_size: u64,
        num_entries: u64,
        truncate: bool,
    ) -> Result<Self> {
        ensure!(
            num_entries.is_power_of_two(),
            "num_entries must be a power of 2, got {num_entries}"
        );
        ensure!(
            entry_size <= u32::MAX as u64,
            "entry_size must fit in 32 bits, got {entry_size}"
        );

        let pgsz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 };
        ensure!(pgsz >= 1, "_SC_PAGESIZE failed");

        let entry_size = align(entry_size, size_of::<u64>() as u64);
        let control_offset = size_of::<RingBufHeader>() as u64;
        let entries_offset = align(
            control_offset + (size_of::<ControlWord>() as u64) * num_entries,
            pgsz,
        );
        let sz = align(entries_offset + entry_size * num_entries, pgsz);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(truncate)
            .open(path)?;
        file.set_len(sz as u64)?;

        let mut mmap = unsafe { MmapOptions::new().map_mut(&file) }?;
        let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut RingBufHeader) };
        if header.params.magic == 0 {
            header.params.magic = MAGIC;
            header.params.version = VERSION;
            header.params.entry_size = entry_size;
            header.params.num_entries = num_entries;
            header.params.control_offset = control_offset;
            header.params.entries_offset = entries_offset;
            header.read_idx = AtomicU64::new(0);
            header.write_idx = AtomicU64::new(0);
        }
        ensure!(
            header.params.magic == MAGIC
                && header.params.version == VERSION
                && header.params.entry_size == entry_size
        );

        Ok(Self {
            ring: RingBuf {
                ptr: mmap.as_ptr(),
                num_entries,
                entry_size,
                control_ptr: unsafe {
                    mmap.as_ptr().byte_add(control_offset as usize) as *const AtomicU64
                },
                entries_ptr: unsafe { mmap.as_ptr().byte_add(entries_offset as usize) },
                _mmap: Some(mmap),
            },
        })
    }

    pub fn create(path: impl AsRef<Path>, entry_size: u64, num_entries: u64) -> Result<Self> {
        Self::_open_or_create(path, entry_size, num_entries, true)
    }

    pub fn open_or_create(
        path: impl AsRef<Path>,
        entry_size: u64,
        num_entries: u64,
    ) -> Result<Self> {
        Self::_open_or_create(path, entry_size, num_entries, false)
    }

    pub fn from_buffer(
        buf: &mut [u8],
        entry_size: u64,
        num_entries: u64,
        clear: bool,
    ) -> Result<Self> {
        ensure!(
            num_entries.is_power_of_two(),
            "num_entries must be a power of 2, got {num_entries}"
        );
        ensure!(
            entry_size <= u32::MAX as u64,
            "entry_size must fit in 32 bits, got {entry_size}"
        );

        let pgsz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 };
        ensure!(pgsz >= 1, "_SC_PAGESIZE failed");

        let entry_size = align(entry_size, size_of::<u64>() as u64);
        let control_offset = size_of::<RingBufHeader>() as u64;
        let entries_offset = align(
            control_offset + (size_of::<ControlWord>() as u64) * num_entries,
            pgsz,
        );

        if clear {
            buf.fill(0);
        }

        let header = unsafe { &mut *(buf.as_mut_ptr() as *mut RingBufHeader) };
        if header.params.magic == 0 {
            header.params.magic = MAGIC;
            header.params.version = VERSION;
            header.params.entry_size = entry_size;
            header.params.num_entries = num_entries;
            header.params.control_offset = control_offset;
            header.params.entries_offset = entries_offset;
            header.read_idx = AtomicU64::new(0);
            header.write_idx = AtomicU64::new(0);
        }
        ensure!(
            header.params.magic == MAGIC
                && header.params.version == VERSION
                && header.params.entry_size == entry_size
        );

        Ok(Self {
            ring: RingBuf {
                ptr: buf.as_ptr(),
                num_entries,
                entry_size,
                control_ptr: unsafe {
                    buf.as_ptr().byte_add(control_offset as usize) as *const AtomicU64
                },
                entries_ptr: unsafe { buf.as_ptr().byte_add(entries_offset as usize) },
                _mmap: None,
            },
        })
    }

    pub fn pop(&self, buf: &mut [u8], mut stall: impl FnMut(u32, usize) -> StallResult) -> bool {
        debug_assert!(buf.len() >= self.ring.entry_size as usize);
        let header = self.ring.header();
        let mut attempt = 0;
        loop {
            let ridx = header.read_idx.load(Relaxed);
            let widx = header.write_idx.load(Relaxed);
            debug_assert!(ridx <= widx, "ridx={ridx} widx={widx}");
            if ridx == widx {
                return false;
            }
            let ctrl = self.ring.control_word(ridx);
            let ctrl_word = ControlWord::load(ctrl);

            if !ctrl_word.is_finished() {
                match stall(ctrl_word.pid(), attempt) {
                    StallResult::Retry => { // keep waiting
                    }
                    StallResult::SkipAndKeepTombstone => {
                        // leave this entry occupied and move to the next one
                        header.read_idx.fetch_add(1, Release);
                    }
                    StallResult::SkipAndReclaim => {
                        // forcefully clear the entry and move to the next one -- should only be done if the caller
                        // is sure the producer is dead, otherwise the producer might wake up in the future and
                        // corrupt the entry's buffer (the memcpy part is not atomic)
                        ctrl.store(0, SeqCst);
                        header.read_idx.fetch_add(1, Release);
                    }
                }
                attempt += 1;
                continue;
            }

            let entry = self.ring.entry(ridx);
            let len = ctrl_word.len();
            debug_assert!(len <= entry.len(), "len={len} entry_size={}", entry.len());
            buf[..len].copy_from_slice(&entry[..len]);

            ctrl.store(0, SeqCst);
            header.read_idx.fetch_add(1, Relaxed);
            return true;
        }
    }
}

pub struct MultiProducer {
    ring: RingBuf,
    tid: u32,
}

impl MultiProducer {
    fn gettid() -> Result<u32> {
        let tid = unsafe { libc::gettid() };
        ensure!(tid > 0, "gettid failed");
        let tid = tid as u32;
        ensure!(
            tid & PID_MASK == tid,
            "PIDs are expected to have only 24 meaningful bits"
        );
        Ok(tid & PID_MASK)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file) }?;
        let header = unsafe { &*(mmap.as_ptr() as *const RingBufHeader) };
        ensure!(header.params.magic == MAGIC && header.params.version == VERSION);

        Ok(Self {
            ring: RingBuf {
                ptr: mmap.as_ptr(),
                control_ptr: unsafe {
                    mmap.as_ptr()
                        .byte_add(header.params.control_offset as usize)
                        as *const AtomicU64
                },
                entries_ptr: unsafe {
                    mmap.as_ptr()
                        .byte_add(header.params.entries_offset as usize)
                },
                num_entries: header.params.num_entries,
                entry_size: header.params.entry_size,
                _mmap: Some(mmap),
            },
            tid: Self::gettid()?,
        })
    }

    pub fn from_buffer(buf: &mut [u8]) -> Result<Self> {
        let header = unsafe { &*(buf.as_ptr() as *const RingBufHeader) };
        ensure!(header.params.magic == MAGIC && header.params.version == VERSION);

        Ok(Self {
            ring: RingBuf {
                ptr: buf.as_ptr(),
                control_ptr: unsafe {
                    buf.as_ptr().byte_add(header.params.control_offset as usize) as *const AtomicU64
                },
                entries_ptr: unsafe {
                    buf.as_ptr().byte_add(header.params.entries_offset as usize)
                },
                num_entries: header.params.num_entries,
                entry_size: header.params.entry_size,
                _mmap: None,
            },
            tid: Self::gettid()?,
        })
    }

    pub fn push(&self, data: &[u8]) -> bool {
        debug_assert!(!data.is_empty() && data.len() <= self.ring.entry_size as usize);
        let header = self.ring.header();
        loop {
            let ridx = header.read_idx.load(Relaxed);
            let widx = header.write_idx.load(Relaxed);
            if widx >= ridx + self.ring.num_entries {
                // no room
                return false;
            }
            if header
                .write_idx
                .compare_exchange(widx, widx + 1, SeqCst, Relaxed)
                .is_err()
            {
                continue;
            }

            let ctrl = self.ring.control_word(widx);
            let ctrl_word = ControlWord::new(self.tid, data.len() as u32);
            if !ctrl_word.claim(ctrl) {
                // this entry is taken (due to another process still holding it), skip for now
                continue;
            }
            self.ring.entry_mut(widx)[..data.len()].copy_from_slice(data);

            if !ctrl_word.mark_finished(ctrl) {
                // we may have corrupted an entry now belonging to another producer during the memcpy above
                // all we can do is signal this case by overwriting the control word to `FINISHED|0` so the
                // consumer will not read anything from it
                return false;
            }

            return true;
        }
    }
}

#[test]
fn test_ring() -> Result<()> {
    let sc = SingleConsumer::create("/tmp/myring", 8, 128)?;

    let pushes = std::sync::Arc::new(AtomicU64::new(0));
    let attempts = std::sync::Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for i in 0..16usize {
        let pushes = pushes.clone();
        let attempts = attempts.clone();
        handles.push(std::thread::spawn(move || {
            let mp = MultiProducer::open("/tmp/myring").unwrap();
            for j in i * 1000..i * 1000 + 100 {
                while !mp.push(&(j.to_ne_bytes()[..])) {
                    attempts.fetch_add(1, SeqCst);
                    std::thread::yield_now();
                }
                pushes.fetch_add(1, SeqCst);
            }
        }));
        std::thread::yield_now();
    }

    let mut res = vec![];
    loop {
        let mut buf = [0u8; 8];
        while sc.pop(&mut buf, |_, _| {
            std::thread::yield_now();
            StallResult::Retry
        }) {
            res.push(unsafe { *(buf.as_ptr() as *const usize) });
        }
        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
    }

    println!("{:?}", res);
    assert_eq!(res.len(), pushes.load(SeqCst) as _);
    println!("{}", attempts.load(SeqCst));

    Ok(())
}
