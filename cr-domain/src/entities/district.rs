use crate::error::DomainError;
use crate::slug::slug_from_name;
use crate::{Coordinates, DistrictId, RegionId};

/// A Czech Republic district (okres).
#[derive(Debug, Clone, PartialEq)]
pub struct District {
    id: DistrictId,
    name: String,
    slug: String,
    district_code: String,
    region_id: RegionId,
    coordinates: Option<Coordinates>,
}

impl District {
    pub fn new(
        id: DistrictId,
        name: impl Into<String>,
        district_code: impl Into<String>,
        region_id: RegionId,
    ) -> Result<Self, DomainError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(DomainError::EmptyName);
        }
        let slug = slug_from_name(&name);
        Ok(Self {
            id,
            name,
            slug,
            district_code: district_code.into(),
            region_id,
            coordinates: None,
        })
    }

    pub fn with_coordinates(mut self, coords: Coordinates) -> Self {
        self.coordinates = Some(coords);
        self
    }

    pub fn id(&self) -> DistrictId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn slug(&self) -> &str {
        &self.slug
    }
    pub fn district_code(&self) -> &str {
        &self.district_code
    }
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }
    pub fn coordinates(&self) -> Option<&Coordinates> {
        self.coordinates.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_district() {
        let d = District::new(
            DistrictId::from(1),
            "Praha-západ",
            "CZ020A",
            RegionId::from(1),
        )
        .unwrap();
        assert_eq!(d.name(), "Praha-západ");
        assert_eq!(d.slug(), "praha-zapad");
        assert_eq!(d.id().value(), 1);
    }

    #[test]
    fn empty_name_rejected() {
        let d = District::new(DistrictId::from(1), "", "CZ020A", RegionId::from(1));
        assert_eq!(d, Err(DomainError::EmptyName));
    }

    #[test]
    fn whitespace_only_name_rejected() {
        let d = District::new(DistrictId::from(1), "   ", "CZ020A", RegionId::from(1));
        assert_eq!(d, Err(DomainError::EmptyName));
    }
}
