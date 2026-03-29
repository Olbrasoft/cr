use crate::error::DomainError;
use crate::slug::slug_from_name;
use crate::{Coordinates, RegionId};

/// A Czech Republic region (kraj).
#[derive(Debug, Clone, PartialEq)]
pub struct Region {
    id: RegionId,
    name: String,
    slug: String,
    region_code: String,
    nuts_code: String,
    coordinates: Option<Coordinates>,
}

impl Region {
    pub fn new(
        id: RegionId,
        name: impl Into<String>,
        region_code: impl Into<String>,
        nuts_code: impl Into<String>,
    ) -> Result<Self, DomainError> {
        let name = name.into();
        if name.is_empty() {
            return Err(DomainError::EmptyName);
        }
        let slug = slug_from_name(&name);
        Ok(Self {
            id,
            name,
            slug,
            region_code: region_code.into(),
            nuts_code: nuts_code.into(),
            coordinates: None,
        })
    }

    pub fn with_coordinates(mut self, coords: Coordinates) -> Self {
        self.coordinates = Some(coords);
        self
    }

    pub fn id(&self) -> RegionId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn slug(&self) -> &str {
        &self.slug
    }
    pub fn region_code(&self) -> &str {
        &self.region_code
    }
    pub fn nuts_code(&self) -> &str {
        &self.nuts_code
    }
    pub fn coordinates(&self) -> Option<&Coordinates> {
        self.coordinates.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_region() {
        let r = Region::new(RegionId::from(1), "Středočeský kraj", "CZ020", "CZ02").unwrap();
        assert_eq!(r.name(), "Středočeský kraj");
        assert_eq!(r.slug(), "stredocesky-kraj");
        assert_eq!(r.id().value(), 1);
    }

    #[test]
    fn empty_name_rejected() {
        let r = Region::new(RegionId::from(1), "", "CZ020", "CZ02");
        assert_eq!(r, Err(DomainError::EmptyName));
    }
}
