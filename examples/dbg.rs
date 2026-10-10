use bitdaemon_common::{Bitfield, DownloadPriority, PieceBlock, PieceIndex};
use bitdaemon_session::piece_picker::*;
struct S([u32;16]); impl PickerStats for S { fn inc(&mut self, c: PickerCounter){ self.0[c as usize]+=1; } }
fn main() {
    let mut p = PiecePicker::new(i64::from(4*4*0x4000), 4*0x4000);
    let have = Bitfield::with_value(4, true);
    p.inc_refcount_bitfield(&have);
    let mut picked = Vec::new();
    let mut s = S([0;16]);
    let flags = p.pick_pieces(&have, &mut picked, 4, 0, None, PickerOptions::RAREST_FIRST, &[], 10, &mut s);
    println!("picked={} flags={:?}", picked.len(), flags);
}
