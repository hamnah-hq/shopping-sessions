// Shopping Sessions pipeline, native implementation.
use memchr::memchr_iter;
use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

type S = &'static [u8];

const DAY: i64 = 86_400_000;
const NB: usize = 256; // partitions

static mut T0: Option<Instant> = None;
fn log(msg: &str) {
    #[allow(static_mut_refs)]
    let t = unsafe { T0.unwrap().elapsed().as_secs_f64() };
    eprintln!("[{:7.3}s] {}", t, msg);
}

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(b);
    let x = h.finish();
    // mix
    let x = (x ^ (x >> 31)).wrapping_mul(0x9E3779B97F4A7C15);
    x ^ (x >> 29)
}

// ---------------------------------------------------------------- CSV
/// Split a line into exactly N fields following the dialect. Returns None when malformed.
/// Fields are unquoted; empty field => empty slice (null).
#[inline]
fn split_line<const N: usize>(line: S) -> Option<[S; N]> {
    let mut out: [S; N] = [&[]; N];
    let mut n = 0usize;
    let mut start = 0usize;
    let len = line.len();
    let mut i = 0usize;
    loop {
        // find end of field
        let mut end = len;
        while i < len {
            if line[i] == b',' {
                end = i;
                break;
            }
            i += 1;
        }
        if n >= N {
            return None;
        }
        let f = &line[start..end];
        // validate quotes
        let v: S = if !f.is_empty() && f[0] == b'"' {
            if f.len() < 2 || f[f.len() - 1] != b'"' {
                return None;
            }
            let inner = &f[1..f.len() - 1];
            if memchr::memchr(b'"', inner).is_some() {
                return None;
            }
            inner
        } else {
            if memchr::memchr(b'"', f).is_some() {
                return None;
            }
            f
        };
        out[n] = v;
        n += 1;
        if end == len {
            break;
        }
        i = end + 1;
        start = i;
    }
    if n != N {
        return None;
    }
    Some(out)
}

struct Chunk {
    data: S,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> S {
    let f = std::fs::File::open(p).expect("open");
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::Mmap::map(&f).expect("mmap") };
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
    }
    let m = Box::leak(Box::new(m));
    &m[..]
}

/// Data region of each file (after header), cut into chunks at line boundaries.
fn chunks_for(paths: &[PathBuf], target: usize) -> Vec<Chunk> {
    let maps: Vec<S> = paths.par_iter().map(|p| map_file(p)).collect();
    let mut out = vec![];
    for d in maps {
        let body = match memchr::memchr(b'\n', d) {
            Some(i) => &d[i + 1..],
            None => continue,
        };
        let mut s = 0usize;
        while s < body.len() {
            let mut e = (s + target).min(body.len());
            if e < body.len() {
                match memchr::memchr(b'\n', &body[e..]) {
                    Some(j) => e = e + j + 1,
                    None => e = body.len(),
                }
            }
            out.push(Chunk { data: &body[s..e] });
            s = e;
        }
    }
    out
}

/// Iterate lines of a chunk: every '\n' ends a line; a final line without '\n' counts too.
#[inline]
fn for_lines<F: FnMut(S)>(d: S, mut f: F) {
    let mut s = 0usize;
    for i in memchr_iter(b'\n', d) {
        f(&d[s..i]);
        s = i + 1;
    }
    if s < d.len() {
        f(&d[s..]);
    }
}

// ---------------------------------------------------------------- timestamps
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    Some(dig(s[i])? * 10 + dig(s[i + 1])?)
}

const BAD: i64 = i64::MIN;

#[inline]
fn parse_ts(s: &[u8]) -> i64 {
    if s.is_empty() {
        return BAD;
    }
    if s[0].is_ascii_digit() && s.iter().all(|c| c.is_ascii_digit()) {
        let mut k = 0;
        while k < s.len() && s[k] == b'0' {
            k += 1;
        }
        let t = &s[k..];
        if t.len() > 18 {
            return BAD; // far outside any window
        }
        let mut v: i64 = 0;
        for &c in t {
            v = v * 10 + (c - b'0') as i64;
        }
        return v;
    }
    parse_iso(s).unwrap_or(BAD)
}

#[inline]
fn parse_iso(s: &[u8]) -> Option<i64> {
    if s.len() != 20 && s.len() != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    let off = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sg = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sg * (oh * 3600 + om * 60)
    };
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => 31,
    };
    if d < 1 || d > dim {
        return None;
    }
    let secs = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se - off;
    Some(secs * 1000)
}

#[inline]
fn parse_price(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut k = 0;
    while k < s.len() && s[k] == b'0' {
        k += 1;
    }
    let t = &s[k..];
    if t.len() > 8 {
        return None;
    }
    let mut v = 0i64;
    for &c in t {
        v = v * 10 + (c - b'0') as i64;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------- union find
fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let px = p[x as usize];
        p[x as usize] = p[px as usize];
        x = px;
    }
    x
}

// ---------------------------------------------------------------- records
#[derive(Clone, Copy)]
struct RawEv {
    id: S,
    user: S,
    product: S,
    price: S,
    ts: S,
    ing: i64,
    et: u8, // 0 view 1 cart 2 purchase 255 bad
}

#[derive(Clone, Copy)]
struct Ev {
    person: S,
    ts: i64,
    price: i64, // purchase price, 0 for non-purchase
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Clk {
    person: S,
    ts: i64,
    id: S,
    camp: u16,
}

fn main() {
    unsafe {
        T0 = Some(Instant::now());
    }
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();

    // manifest
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let get = |k: &str| -> i64 {
        let i = man.find(k).expect("key") + k.len();
        let rest = &man[i..];
        let a = rest.find('"').unwrap() + 1; // after the colon there is a quote
        let rest2 = &rest[a..];
        let b = rest2.find('"').unwrap();
        parse_iso(rest2[..b].as_bytes()).expect("manifest ts")
    };
    let ws = get("\"window_start\"");
    let we = get("\"window_end\"");
    let cws = ws - 7 * DAY;
    let ndays = ((we - ws + DAY - 1) / DAY) as usize;
    let day0 = ws.div_euclid(DAY);

    // products & campaigns
    let pdata = map_file(&data.join("products.csv"));
    let mut cat_names: Vec<S> = vec![];
    let mut cat_idx: FxHashMap<S, u16> = FxHashMap::default();
    let mut prod: FxHashMap<S, u16> = FxHashMap::default();
    if let Some(i) = memchr::memchr(b'\n', pdata) {
        for_lines(&pdata[i + 1..], |l| {
            if let Some([p, c]) = split_line::<2>(l) {
                let ci = *cat_idx.entry(c).or_insert_with(|| {
                    cat_names.push(c);
                    (cat_names.len() - 1) as u16
                });
                prod.insert(p, ci);
            }
        });
    }
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown");
    // rank of category by byte order for tie-breaks
    let mut order: Vec<u16> = (0..cat_names.len() as u16).collect();
    order.sort_by(|a, b| cat_names[*a as usize].cmp(cat_names[*b as usize]));
    let mut cat_rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u16;
    }
    let ncat = cat_names.len();

    let cdata = map_file(&data.join("campaigns.csv"));
    let mut camp_names: Vec<S> = vec![];
    let mut camp_ch: Vec<S> = vec![];
    let mut camp: FxHashMap<S, u16> = FxHashMap::default();
    if let Some(i) = memchr::memchr(b'\n', cdata) {
        for_lines(&cdata[i + 1..], |l| {
            if let Some([c, ch]) = split_line::<2>(l) {
                if !camp.contains_key(c) {
                    camp.insert(c, camp_names.len() as u16);
                    camp_names.push(c);
                    camp_ch.push(ch);
                }
            }
        });
    }
    let ncamp = camp_names.len();
    let organic = ncamp; // index
    log("small tables");

    // ------------------------------------------------ links
    let lchunks = chunks_for(&list_csv(&data.join("links")), 4 << 20);
    let pairs: Vec<Vec<(S, S)>> = lchunks
        .par_iter()
        .map(|c| {
            let mut v = vec![];
            for_lines(c.data, |l| {
                if let Some([a, b]) = split_line::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((a, b));
                    }
                }
            });
            v
        })
        .collect();
    let mut lid: FxHashMap<S, u32> = FxHashMap::default();
    let mut names: Vec<S> = vec![];
    let mut par: Vec<u32> = vec![];
    for v in &pairs {
        for &(a, b) in v {
            let ia = *lid.entry(a).or_insert_with(|| {
                names.push(a);
                par.push(par.len() as u32);
                (par.len() - 1) as u32
            });
            let ib = *lid.entry(b).or_insert_with(|| {
                names.push(b);
                par.push(par.len() as u32);
                (par.len() - 1) as u32
            });
            let ra = find(&mut par, ia);
            let rb = find(&mut par, ib);
            if ra != rb {
                par[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    drop(pairs);
    let n = names.len();
    let mut best: Vec<S> = vec![&[]; n];
    let mut root = vec![0u32; n];
    for i in 0..n {
        let r = find(&mut par, i as u32);
        root[i] = r;
        let b = &mut best[r as usize];
        if b.is_empty() || names[i] < *b {
            *b = names[i];
        }
    }
    // id -> person name
    let pname: FxHashMap<S, S> = names.iter().enumerate().map(|(i, &nm)| (nm, best[root[i] as usize])).collect();
    drop(lid);
    let pname = Arc::new(pname);
    log(&format!("links ids={}", n));
    let person_of = |u: S| -> S {
        match pname.get(u) {
            Some(&p) => p,
            None => u,
        }
    };

    // ------------------------------------------------ events: parse, shuffle by event id
    let echunks = chunks_for(&list_csv(&data.join("events")), 8 << 20);
    log(&format!("event chunks={}", echunks.len()));
    let parts: Vec<Vec<Vec<RawEv>>> = echunks
        .par_iter()
        .map(|c| {
            let mut b: Vec<Vec<RawEv>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(c.data, |l| {
                if let Some([id, user, ts, ing, et, product, price]) = split_line::<7>(l) {
                    if id.is_empty() {
                        return;
                    }
                    let ingv = match parse_iso(ing) {
                        Some(v) => v,
                        None => i64::MAX,
                    };
                    let etc = match et {
                        b"view" => 0,
                        b"cart" => 1,
                        b"purchase" => 2,
                        _ => 255,
                    };
                    let h = hash_bytes(id) as usize % NB;
                    b[h].push(RawEv { id, user, product, price, ts, ing: ingv, et: etc });
                }
            });
            b
        })
        .collect();
    log("events parsed");
    // dedup per bucket, validate, map to person, shuffle by person
    let evparts: Vec<Vec<Vec<Ev>>> = (0..NB)
        .into_par_iter()
        .map(|bi| {
            let cnt: usize = parts.iter().map(|p| p[bi].len()).sum();
            let mut m: FxHashMap<S, RawEv> = FxHashMap::with_capacity_and_hasher(cnt, Default::default());
            for p in &parts {
                for r in &p[bi] {
                    match m.get_mut(r.id) {
                        Some(e) => {
                            if r.ing < e.ing {
                                *e = *r;
                            }
                        }
                        None => {
                            m.insert(r.id, *r);
                        }
                    }
                }
            }
            let mut ob: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::new()).collect();
            for r in m.values() {
                if r.user.is_empty() || r.et == 255 || r.product.is_empty() {
                    continue;
                }
                let ts = parse_ts(r.ts);
                if ts == BAD || ts < ws || ts >= we {
                    continue;
                }
                let price = if r.et == 2 {
                    match parse_price(r.price) {
                        Some(v) => v,
                        None => continue,
                    }
                } else {
                    0
                };
                if r.ing.saturating_sub(ts) > DAY {
                    continue;
                }
                let person = person_of(r.user);
                let cat = *prod.get(r.product).unwrap_or(&unknown);
                let h = hash_bytes(person) as usize % NB;
                ob[h].push(Ev { person, ts, price, cat, purchase: r.et == 2 });
            }
            ob
        })
        .collect();
    drop(parts);
    log("events deduped");

    // ------------------------------------------------ clicks
    let cchunks = chunks_for(&list_csv(&data.join("clicks")), 8 << 20);
    let cparts: Vec<Vec<Vec<Clk>>> = cchunks
        .par_iter()
        .map(|c| {
            let mut b: Vec<Vec<Clk>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(c.data, |l| {
                if let Some([id, user, cp, ts]) = split_line::<4>(l) {
                    if id.is_empty() || user.is_empty() {
                        return;
                    }
                    let ci = match camp.get(cp) {
                        Some(&x) => x,
                        None => return,
                    };
                    let t = parse_ts(ts);
                    if t == BAD || t < cws || t >= we {
                        return;
                    }
                    let person = person_of(user);
                    let h = hash_bytes(person) as usize % NB;
                    b[h].push(Clk { person, ts: t, id, camp: ci });
                }
            });
            b
        })
        .collect();
    log("clicks parsed");

    // ------------------------------------------------ per person bucket
    let ncs = ncamp + 1;
    struct BucketOut {
        users: Vec<(S, i32, i64, i64, i64, Option<u16>)>,
        camp: Vec<[i64; 4]>, // (camp*ndays+day) -> sessions, purchasing, revenue, assisted
    }
    let outs: Vec<BucketOut> = (0..NB)
        .into_par_iter()
        .map(|bi| {
            // person ids
            let mut pid: FxHashMap<S, u32> = FxHashMap::default();
            let mut pnames: Vec<S> = vec![];
            let mut evs: Vec<(u64, u32)> = vec![]; // key (pid<<32 | ts-ws), index
            let mut evflat: Vec<Ev> = vec![];
            for p in &evparts {
                for e in &p[bi] {
                    let id = *pid.entry(e.person).or_insert_with(|| {
                        pnames.push(e.person);
                        (pnames.len() - 1) as u32
                    });
                    evs.push((((id as u64) << 32) | ((e.ts - ws) as u64), evflat.len() as u32));
                    evflat.push(*e);
                }
            }
            let mut clks: Vec<(u32, i64, S, u16)> = vec![];
            for p in &cparts {
                for c in &p[bi] {
                    if let Some(&id) = pid.get(c.person) {
                        clks.push((id, c.ts, c.id, c.camp));
                    }
                }
            }
            evs.sort_unstable_by_key(|x| x.0);
            // clicks: by person, ts asc, click id desc (so the last of equal ts is the smallest id)
            clks.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(b.2.cmp(a.2)));

            let mut users = vec![];
            let mut camp_acc = vec![[0i64; 4]; ncs * ndays];
            let mut ccount = vec![0u32; ncs];
            let mut since = vec![0usize; ncs];
            let mut cat_sum = vec![0i64; ncat];

            let mut ei = 0usize;
            let mut ci = 0usize;
            // session buffers
            let mut s_start: Vec<i64> = vec![];
            let mut s_day: Vec<usize> = vec![];
            while ei < evs.len() {
                let p = (evs[ei].0 >> 32) as u32;
                let mut ej = ei;
                while ej < evs.len() && (evs[ej].0 >> 32) as u32 == p {
                    ej += 1;
                }
                while ci < clks.len() && clks[ci].0 < p {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < clks.len() && clks[cj].0 == p {
                    cj += 1;
                }
                let cl = &clks[ci..cj];
                // sessionize
                s_start.clear();
                s_day.clear();
                let mut cur_day: i64 = -1;
                let mut d_sess = 0i64;
                let mut d_ev = 0i64;
                let mut d_rev = 0i64;
                let mut d_has = false;
                let mut k = ei;
                let mut hi = 0usize; // clicks added: cl[..hi]
                let mut lo = 0usize; // clicks removed: cl[..lo]
                let flush = |users: &mut Vec<(S, i32, i64, i64, i64, Option<u16>)>,
                             cat_sum: &mut Vec<i64>,
                             day: i64,
                             ns: i64,
                             ne: i64,
                             rv: i64,
                             has: bool| {
                    let top = if has {
                        let mut bc = usize::MAX;
                        for c in 0..ncat {
                            if cat_sum[c] > 0 {
                                if bc == usize::MAX
                                    || cat_sum[c] > cat_sum[bc]
                                    || (cat_sum[c] == cat_sum[bc] && cat_rank[c] < cat_rank[bc])
                                {
                                    bc = c;
                                }
                            }
                        }
                        for c in cat_sum.iter_mut() {
                            *c = 0;
                        }
                        if bc == usize::MAX {
                            None
                        } else {
                            Some(bc as u16)
                        }
                    } else {
                        None
                    };
                    users.push((pnames[p as usize], (day) as i32, ns, ne, rv, top));
                };
                let mut sidx = 0usize;
                while k < ej {
                    let start = evflat[evs[k].1 as usize].ts;
                    let mut last = start;
                    let mut n_ev = 0i64;
                    let mut rev = 0i64;
                    let mut has_p = false;
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        if cur_day >= 0 {
                            flush(&mut users, &mut cat_sum, cur_day, d_sess, d_ev, d_rev, d_has);
                        }
                        cur_day = day;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                        d_has = false;
                    }
                    while k < ej {
                        let e = &evflat[evs[k].1 as usize];
                        if e.ts - last > 1_800_000 {
                            break;
                        }
                        last = e.ts;
                        n_ev += 1;
                        if e.purchase {
                            rev += e.price;
                            has_p = true;
                            cat_sum[e.cat as usize] += e.price;
                        }
                        k += 1;
                    }
                    d_sess += 1;
                    d_ev += n_ev;
                    d_rev += rev;
                    d_has |= has_p;
                    let dix = (day - day0) as usize;
                    // clicks window for this session: [start - 7d, start]
                    while hi < cl.len() && cl[hi].1 <= start {
                        let c = cl[hi].3 as usize;
                        if ccount[c] == 0 {
                            since[c] = sidx;
                        }
                        ccount[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && cl[lo].1 < start - 7 * DAY {
                        let c = cl[lo].3 as usize;
                        ccount[c] -= 1;
                        if ccount[c] == 0 {
                            // active for sessions [since, sidx)
                            add_assist(&mut camp_acc, c, since[c], sidx, &s_day, ndays);
                        }
                        lo += 1;
                    }
                    let credited = if hi > lo { cl[hi - 1].3 as usize } else { organic };
                    let a = &mut camp_acc[credited * ndays + dix];
                    a[0] += 1;
                    a[1] += has_p as i64;
                    a[2] += rev;
                    s_start.push(start);
                    s_day.push(dix);
                    sidx += 1;
                }
                if cur_day >= 0 {
                    flush(&mut users, &mut cat_sum, cur_day, d_sess, d_ev, d_rev, d_has);
                }
                // close remaining active campaigns
                for j in lo..hi {
                    let c = cl[j].3 as usize;
                    if ccount[c] > 0 {
                        add_assist(&mut camp_acc, c, since[c], sidx, &s_day, ndays);
                        ccount[c] = 0;
                    }
                }
                ei = ej;
                ci = cj;
            }
            BucketOut { users, camp: camp_acc }
        })
        .collect();
    log("sessions + attribution");

    // ------------------------------------------------ outputs
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .build(),
    );
    let uschema = Arc::new(
        parse_message_type(
            "message s { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions;
             REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BYTE_ARRAY top_category (UTF8); }",
        )
        .unwrap(),
    );
    const NF: usize = 16;
    (0..NF).into_par_iter().for_each(|fi| {
        let rows: Vec<&(S, i32, i64, i64, i64, Option<u16>)> =
            outs.iter().enumerate().filter(|(i, _)| i % NF == fi).flat_map(|(_, o)| o.users.iter()).collect();
        let f = std::fs::File::create(ud.join(format!("part-{:03}.parquet", fi))).unwrap();
        let mut w = SerializedFileWriter::new(f, uschema.clone(), props.clone()).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 => {
                    let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.0.to_vec())).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<i32> = rows.iter().map(|r| r.1).collect();
                    c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                2 | 3 | 4 => {
                    let v: Vec<i64> = rows.iter().map(|r| if col == 2 { r.2 } else if col == 3 { r.3 } else { r.4 }).collect();
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let defs: Vec<i16> = rows.iter().map(|r| r.5.is_some() as i16).collect();
                    let v: Vec<ByteArray> =
                        rows.iter().filter_map(|r| r.5).map(|c| ByteArray::from(cat_names[c as usize].to_vec())).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
        w.close().unwrap();
    });

    // campaign table
    let mut tot = vec![[0i64; 4]; ncs * ndays];
    for o in &outs {
        for (t, a) in tot.iter_mut().zip(o.camp.iter()) {
            for j in 0..4 {
                t[j] += a[j];
            }
        }
    }
    let mut cid: Vec<ByteArray> = vec![];
    let mut cch: Vec<ByteArray> = vec![];
    let mut cday: Vec<i32> = vec![];
    let mut cv: [Vec<i64>; 4] = [vec![], vec![], vec![], vec![]];
    for c in 0..ncs {
        for d in 0..ndays {
            let a = tot[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == organic {
                cid.push(ByteArray::from(&b"organic"[..]));
                cch.push(ByteArray::from(&b"organic"[..]));
            } else {
                cid.push(ByteArray::from(camp_names[c].to_vec()));
                cch.push(ByteArray::from(camp_ch[c].to_vec()));
            }
            cday.push((day0 + d as i64) as i32);
            for j in 0..4 {
                cv[j].push(if c == organic && j == 3 { 0 } else { a[j] });
            }
        }
    }
    let cschema = Arc::new(
        parse_message_type(
            "message s { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); REQUIRED INT32 day (DATE);
             REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let f = std::fs::File::create(cd.join("part-000.parquet")).unwrap();
    let mut w = SerializedFileWriter::new(f, cschema, props.clone()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut col = 0;
    while let Some(mut c) = rg.next_column().unwrap() {
        match col {
            0 => {
                c.typed::<ByteArrayType>().write_batch(&cid, None, None).unwrap();
            }
            1 => {
                c.typed::<ByteArrayType>().write_batch(&cch, None, None).unwrap();
            }
            2 => {
                c.typed::<Int32Type>().write_batch(&cday, None, None).unwrap();
            }
            k => {
                c.typed::<Int64Type>().write_batch(&cv[k - 3], None, None).unwrap();
            }
        }
        c.close().unwrap();
        col += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
    log("done");
    std::process::exit(0);
}

/// Campaign c assisted sessions [a, b) of the current person; add per day.
#[inline]
fn add_assist(acc: &mut [[i64; 4]], c: usize, a: usize, b: usize, s_day: &[usize], ndays: usize) {
    let mut i = a;
    while i < b {
        let d = s_day[i];
        let mut j = i + 1;
        while j < b && s_day[j] == d {
            j += 1;
        }
        acc[c * ndays + d][3] += (j - i) as i64;
        i = j;
    }
}
