use crate::error::DomainError;
use crate::slug::slug_from_name;
use crate::{Coordinates, MunicipalityId, OrpId};

/// A Czech municipality (obec).
#[derive(Debug, Clone, PartialEq)]
pub struct Municipality {
    id: MunicipalityId,
    name: String,
    slug: String,
    municipality_code: String,
    pou_code: String,
    orp_id: OrpId,
    coordinates: Option<Coordinates>,
    population: Option<i32>,
}

impl Municipality {
    pub fn new(
        id: MunicipalityId,
        name: impl Into<String>,
        municipality_code: impl Into<String>,
        pou_code: impl Into<String>,
        orp_id: OrpId,
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
            municipality_code: municipality_code.into(),
            pou_code: pou_code.into(),
            orp_id,
            coordinates: None,
            population: None,
        })
    }

    pub fn with_coordinates(mut self, coords: Coordinates) -> Self {
        self.coordinates = Some(coords);
        self
    }

    pub fn with_population(mut self, pop: i32) -> Self {
        self.population = Some(pop);
        self
    }

    pub fn id(&self) -> MunicipalityId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn slug(&self) -> &str {
        &self.slug
    }
    pub fn municipality_code(&self) -> &str {
        &self.municipality_code
    }
    pub fn pou_code(&self) -> &str {
        &self.pou_code
    }
    pub fn orp_id(&self) -> OrpId {
        self.orp_id
    }
    pub fn coordinates(&self) -> Option<&Coordinates> {
        self.coordinates.as_ref()
    }
    pub fn population(&self) -> Option<i32> {
        self.population
    }
}
