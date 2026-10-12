use std::fmt;

/// Row id of `selectors.id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectorId(i64);

/// Row id of `fill_jobs.id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(i64);

macro_rules! id_impl {
    ($name:ident) => {
        impl $name {
            pub(crate) fn from_raw(id: i64) -> Self {
                Self(id)
            }

            pub fn get(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_impl!(SelectorId);
id_impl!(JobId);
