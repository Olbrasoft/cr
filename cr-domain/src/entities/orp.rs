use crate::error::DomainError;
use crate::slug::slug_from_name;
use crate::{Coordinates, DistrictId, OrpId};

/// An administrative district of a municipality with extended competence (ORP).
#[derive(Debug, Clone, PartialEq)]
pub struct Orp {
    id: OrpId,
    name: String,
    slug: String,
    orp_code: String,
    district_id: DistrictId,
    coordinates: Option<Coordinates>,
}

impl Orp {
    pub fn new(
        id: OrpId,
        name: impl Into<String>,
        orp_code: impl Into<String>,
        district_id: DistrictId,
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
            orp_code: orp_code.into(),
            district_id,
            coordinates: None,
        })
    }

    pub fn with_coordinates(mut self, coords: Coordinates) -> Self {
        self.coordinates = Some(coords);
        self
    }

    pub fn id(&self) -> OrpId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn slug(&self) -> &str {
        &self.slug
    }
    pub fn orp_code(&self) -> &str {
        &self.orp_code
    }
    pub fn district_id(&self) -> DistrictId {
        self.district_id
    }
    pub fn coordinates(&self) -> Option<&Coordinates> {
        self.coordinates.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_orp() {
        let o = Orp::new(
            OrpId::from(1),
            "Brandýs nad Labem-Stará Boleslav",
            "2101",
            DistrictId::from(1),
        )
        .unwrap();
        assert_eq!(o.name(), "Brandýs nad Labem-Stará Boleslav");
        assert_eq!(o.slug(), "brandys-nad-labem-stara-boleslav");
        assert_eq!(o.id().value(), 1);
    }

    #[test]
    fn empty_name_rejected() {
        let o = Orp::new(OrpId::from(1), "", "2101", DistrictId::from(1));
        assert_eq!(o, Err(DomainError::EmptyName));
    }

    #[test]
    fn whitespace_only_name_rejected() {
        let o = Orp::new(OrpId::from(1), "   ", "2101", DistrictId::from(1));
        assert_eq!(o, Err(DomainError::EmptyName));
    }
}
