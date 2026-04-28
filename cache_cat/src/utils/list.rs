use std::collections::VecDeque;

pub fn lrange<T: Clone>(deque: &VecDeque<T>, start: i64, end: i64) -> Vec<T> {
    let len = deque.len() as i64;

    if len == 0 {
        return vec![];
    }

    // 处理负索引
    let mut start = if start < 0 { len + start } else { start };
    let mut end = if end < 0 { len + end } else { end };

    // 边界裁剪
    start = start.max(0);
    end = end.min(len - 1);

    if start > end {
        return vec![];
    }

    let count = (end - start + 1) as usize;

    deque
        .iter()
        .skip(start as usize)
        .take(count)
        .cloned()
        .collect()
}
