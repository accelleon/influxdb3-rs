use crate::{InfluxDBError, TagMap, TimestampPrecision, ToPoint};

const MAX_LINES: usize = 10_000;
const MAX_BYTES: usize = 10 * 1024 * 1024; // 10 MB

pub(crate) struct Batcher<'a> {
    buffers: Vec<Vec<u8>>,
    current_buffer: Vec<u8>,
    current_lines: usize,
    precision: TimestampPrecision,
    default_tags: &'a TagMap,
}

impl<'a> Batcher<'a> {
    pub fn new(precision: TimestampPrecision, default_tags: &'a TagMap) -> Self {
        Batcher {
            buffers: Vec::new(),
            current_buffer: Vec::new(),
            current_lines: 0,
            precision,
            default_tags,
        }
    }

    pub fn add_point<T>(&mut self, point: T) -> Result<(), InfluxDBError>
    where
        T: ToPoint,
    {
        point.to_point().serialize(&mut self.current_buffer, self.precision, self.default_tags);
        self.current_lines += 1;

        if self.current_lines >= MAX_LINES || self.current_buffer.len() >= MAX_BYTES {
            let mut new_buffer = Vec::new();
            std::mem::swap(&mut new_buffer, &mut self.current_buffer);
            self.buffers.push(new_buffer);
            self.current_lines = 0;
        }

        Ok(())
    }

    pub fn add_points<T, I>(&mut self, points: I) -> Result<(), InfluxDBError>
    where
        T: ToPoint,
        I: IntoIterator<Item = T>,
    {
        for point in points {
            self.add_point(point)?;
        }
        Ok(())
    }

    pub fn finalize(mut self) -> impl Iterator<Item = Vec<u8>> {
        if !self.current_buffer.is_empty() {
            let mut new_buffer = Vec::new();
            std::mem::swap(&mut new_buffer, &mut self.current_buffer);
            self.buffers.push(new_buffer);
        }
        self.buffers.into_iter()
    }
}