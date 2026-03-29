//! Strongly-typed ID wrappers to prevent accidentally swapping IDs of different entity types.

/// Macro to generate a newtype ID wrapper around i32.
macro_rules! define_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(pub i32);

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
    fn different_id_types_are_incompatible() {
        let region_id = RegionId(1);
        let district_id = DistrictId(1);
        // These are different types — can't be compared or swapped
        assert_eq!(region_id.0, district_id.0); // same inner value
        // But region_id != district_id would not compile (different types)
    }

    #[test]
    fn id_conversion() {
        let id = RegionId::from(42);
        assert_eq!(id.0, 42);
        let raw: i32 = id.into();
        assert_eq!(raw, 42);
    }

    #[test]
    fn id_display() {
        let id = OrpId(123);
        assert_eq!(format!("{id}"), "123");
    }
}
