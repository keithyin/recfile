#[derive(Debug, Clone)]
pub struct FixedSizeStack {
    stack: Vec<usize>,
    cur_top: usize,
}

impl FixedSizeStack {
    pub fn new(size: usize) -> Self {
        Self {
            stack: Vec::with_capacity(size),
            cur_top: 0,
        }
    }

    pub fn fill_stack(mut self) -> Self {
        let cap = self.stack.capacity();
        (0..cap).into_iter().for_each(|i| {
            self.stack.push(i);
        });
        self.cur_top = cap;
        self
    }

    pub fn push(&mut self, item: usize) {
        if self.cur_top < self.stack.capacity() {
            self.stack[self.cur_top] = item;
            self.cur_top += 1;
        } else {
            panic!("Stack overflow");
        }
    }

    pub fn pop(&mut self) -> Option<usize> {
        if self.cur_top > 0 {
            self.cur_top -= 1;
            Some(self.stack[self.cur_top])
        } else {
            None
        }
    }
}
