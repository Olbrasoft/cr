//! Strongly-typed ID wrappers to prevent accidentally swapping IDs of different entity types.

/// Macro to generate a newtype ID wrapper around i32.
macro_rules! define_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(i32);

        impl $name {
            /// Get the inner i32 value.
            pub fn value(self) -> i32 {
                self.0
            }
        }

        impl From<i32> for $name {
            fn from(id: i32) -> Self {
                Self(id)
            }
        }

        impl From<$name> for i32 {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

define_id!(RegionId);
define_id!(DistrictId);
define_id!(OrpId);
define_id!(MunicipalityId);
define_id!(LandmarkId);
define_id!(PoolId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_conversion() {
        let id = RegionId::from(42);
        assert_eq!(id.value(), 42);
        let raw: i32 = id.into();
        assert_eq!(raw, 42);
    }

    #[test]
    fn id_display() {
        let id = OrpId::from(123);
        assert_eq!(format!("{id}"), "123");
    }

    #[test]
    fn id_equality() {
        assert_eq!(RegionId::from(1), RegionId::from(1));
        assert_ne!(RegionId::from(1), RegionId::from(2));
    }
}
