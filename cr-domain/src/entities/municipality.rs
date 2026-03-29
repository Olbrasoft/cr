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
    wikipedia_url: Option<String>,
    official_website: Option<String>,
    coat_of_arms_ext: Option<String>,
    flag_ext: Option<String>,
    elevation: Option<f64>,
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
        if name.trim().is_empty() {
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
            wikipedia_url: None,
            official_website: None,
            coat_of_arms_ext: None,
            flag_ext: None,
            elevation: None,
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

    pub fn with_wikipedia_url(mut self, url: impl Into<String>) -> Self {
        self.wikipedia_url = Some(url.into());
        self
    }

    pub fn with_official_website(mut self, url: impl Into<String>) -> Self {
        self.official_website = Some(url.into());
        self
    }

    pub fn with_coat_of_arms_ext(mut self, ext: impl Into<String>) -> Self {
        self.coat_of_arms_ext = Some(ext.into());
        self
    }

    pub fn with_flag_ext(mut self, ext: impl Into<String>) -> Self {
        self.flag_ext = Some(ext.into());
        self
    }

    pub fn with_elevation(mut self, elevation: f64) -> Self {
        self.elevation = Some(elevation);
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
    pub fn wikipedia_url(&self) -> Option<&str> {
        self.wikipedia_url.as_deref()
    }
    pub fn official_website(&self) -> Option<&str> {
        self.official_website.as_deref()
    }
    pub fn coat_of_arms_ext(&self) -> Option<&str> {
        self.coat_of_arms_ext.as_deref()
    }
    pub fn flag_ext(&self) -> Option<&str> {
        self.flag_ext.as_deref()
    }
    pub fn elevation(&self) -> Option<f64> {
        self.elevation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_municipality() {
        let m = Municipality::new(
            MunicipalityId::from(1),
            "Benešov",
            "529303",
            "2101",
            OrpId::from(1),
        )
        .unwrap();
        assert_eq!(m.name(), "Benešov");
        assert_eq!(m.slug(), "benesov");
        assert_eq!(m.id().value(), 1);
    }

    #[test]
    fn empty_name_rejected() {
        let m = Municipality::new(
            MunicipalityId::from(1),
            "",
            "529303",
            "2101",
            OrpId::from(1),
        );
        assert_eq!(m, Err(DomainError::EmptyName));
    }

    #[test]
    fn whitespace_only_name_rejected() {
        let m = Municipality::new(
            MunicipalityId::from(1),
            "   ",
            "529303",
            "2101",
            OrpId::from(1),
        );
        assert_eq!(m, Err(DomainError::EmptyName));
    }
}
