// Copyright (C) 2010 Canonical Ltd
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

//! Record counting support for showing progress of revision fetch.
//!
//! [`RecordCounter`] keeps an estimate of how much work a fetch (push, pull,
//! branch, checkout) will involve so a progress bar can show a
//! fetched-vs-estimate ratio.

/// Users update the progress bar every `STEP` records. An odd number keeps the
/// last digit of the fetched-vs-estimate ratio changing periodically.
pub const STEP: i64 = 7;

/// Maintains estimates of the work required for a fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordCounter {
    pub initialized: bool,
    pub current: i64,
    pub key_count: i64,
    pub max: i64,
    pub step: i64,
}

impl Default for RecordCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordCounter {
    pub fn new() -> Self {
        Self {
            initialized: false,
            current: 0,
            key_count: 0,
            max: 0,
            step: STEP,
        }
    }

    /// Whether [`setup`](Self::setup) has been called.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Estimate the maximum amount of "inserting stream" work.
    ///
    /// The 10.3 multiplier comes from empirical data across three projects; it
    /// is chosen to under-promise/over-deliver and to render a realistic
    /// progress ratio. See the original Python for the derivation.
    pub fn estimate_max(&self, key_count: i64) -> i64 {
        (key_count as f64 * 10.3) as i64
    }

    /// Set up `max`/`current` to reflect the amount of work pending.
    pub fn setup(&mut self, key_count: i64, current: i64) {
        self.current = current;
        self.key_count = key_count;
        self.max = self.estimate_max(key_count);
        self.initialized = true;
    }

    /// Increment `current` by `count`, growing `max` so it stays ahead.
    pub fn increment(&mut self, count: i64) {
        self.current += count;
        if self.current > self.max {
            self.max += self.key_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_uninitialized() {
        let rc = RecordCounter::new();
        assert!(!rc.is_initialized());
        assert_eq!(rc.current, 0);
        assert_eq!(rc.key_count, 0);
        assert_eq!(rc.max, 0);
        assert_eq!(rc.step, 7);
    }

    #[test]
    fn setup_estimates_max() {
        let mut rc = RecordCounter::new();
        rc.setup(1000, 0);
        assert!(rc.is_initialized());
        assert_eq!(rc.key_count, 1000);
        assert_eq!(rc.current, 0);
        assert_eq!(rc.max, 10300);
    }

    #[test]
    fn setup_honours_current() {
        let mut rc = RecordCounter::new();
        rc.setup(1000, 200);
        assert_eq!(rc.current, 200);
        assert_eq!(rc.max, 10300);
    }

    #[test]
    fn increment_advances_current() {
        let mut rc = RecordCounter::new();
        rc.setup(10, 0);
        rc.increment(5);
        assert_eq!(rc.current, 5);
        assert_eq!(rc.max, 103);
    }

    #[test]
    fn increment_grows_max_when_overrun() {
        let mut rc = RecordCounter::new();
        rc.setup(10, 0);
        rc.max = 3;
        rc.increment(5);
        assert_eq!(rc.current, 5);
        assert_eq!(rc.max, 13);
    }
}
